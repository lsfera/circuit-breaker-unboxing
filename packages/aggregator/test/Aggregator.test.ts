import { test } from "node:test";
import assert from "node:assert/strict";
import { Duration, Effect, Layer, Ref, Option as O} from "effect";
import { TestClock } from "effect/testing";
import { Aggregator } from "../src/Aggregator.ts";
import { InMemoryCoordinationLayer } from "../src/Coordination.ts";
import { EventBus, EventSink } from "../src/Events.ts";
import { FleetSource, SimFleetLayer, parseStats } from "../src/FleetSource.ts";
import { emptyIntegrity, record } from "../src/Http.ts";
import * as Breaker from "@egress/domain/Breaker.ts";
import { Config, defaultConfig, State } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * The whole pipeline under TestClock: no sleeps, no flakiness, and a simulated
 * minute costs microseconds.
 */

const SPECS = [
  { apiId: "payments", endpoints: 6, rps: 900, failureRate: 0 },
  { apiId: "tax-calc", endpoints: 3, rps: 120, failureRate: 0 },
];

const CFG = { ...defaultConfig, dwellMs: 500, minStateMs: 500, openMs: 1000 };

/**
 * TestClock starts at the epoch, which would make a genuine "we published
 * 1970" bug indistinguishable from normal test time. Anchoring it to a
 * realistic instant makes epoch leakage unambiguous.
 */
const T0 = Date.parse("2026-09-02T12:00:00.000Z");

/** A sink that records everything it was handed, so we can assert on delivery. */
const RecordingSink = (into: Ref.Ref<ReadonlyArray<CircuitEvent>>) =>
  Layer.succeed(EventSink, {
    name: "recording",
    deliver: (event) => Ref.update(into, (xs) => [...xs, event]),
    deadLetters: Effect.succeed([]),
    drainOutbox: Effect.succeed(0),
  });

const harness = (delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>) =>
  Aggregator.layer.pipe(
    // provideMerge, not provide: the tests drive FleetSource and read EventBus
    // directly, so those services stay in the output context.
    Layer.provideMerge(
      Layer.mergeAll(
        SimFleetLayer(SPECS, 5),
        EventBus.layer,
        RecordingSink(delivered),
        // Solo HA: this instance always wins its own lease, same as a single
        // real process would. Dedicated failover/fencing tests below build
        // their own two-instance harness instead of this one.
        InMemoryCoordinationLayer,
      ),
    ),
    Layer.provideMerge(TestClock.layer()),
  );

const run = <A>(
  body: (delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>) => Effect.Effect<
    A,
    never,
    Aggregator | FleetSource | EventBus | TestClock.TestClock
  >,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
      return yield* TestClock.setTime(T0).pipe(
        Effect.andThen(body(delivered)),
      ).pipe(
        Effect.provide(harness(delivered)),
        Effect.provideService(Config, CFG),
      );
    }),
  );

/** Advance simulated time by running the tick loop N times. */
const ticks = (n: number) =>
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    for (let i = 0; i < n; i++) {
      yield* agg.tick;
      yield* TestClock.adjust(Duration.millis(CFG.tickMs));
    }
  });

test("healthy fleet publishes no transitions", async () => {
  const state = await run(() =>
    Effect.gen(function* () {
      yield* ticks(40);
      const agg = yield* Aggregator;
      return yield* agg.stateOf("payments");
    }),
  );
  assert.deepEqual(state, O.some(State.CLOSED));
});

test("total outage opens, restoration closes, delivered in order", async () => {
  const result = await run((delivered) =>
    Effect.gen(function* () {
      const fleet = yield* FleetSource;
      const agg = yield* Aggregator;

      yield* ticks(8);
      yield* fleet.setFailureRate("payments", 1);
      yield* ticks(60);
      const opened = yield* agg.stateOf("payments");

      yield* fleet.setFailureRate("payments", 0);
      yield* ticks(200);
      const closed = yield* agg.stateOf("payments");

      const events = yield* Ref.get(delivered);
      const other = yield* agg.stateOf("tax-calc");
      return { opened, closed, events, other };
    }),
  );

  assert.deepEqual(result.opened, O.some(State.OPEN));
  assert.deepEqual(result.closed, O.some(State.CLOSED));
  assert.deepEqual(result.other, O.some(State.CLOSED), "other APIs are unaffected");

  const transitions = result.events.filter(
    (e) => e.type === "egress.circuit.state_changed" && e.data.apiId === "payments",
  );
  assert.ok(transitions.length >= 2);

  // The delivery contract: per-API sequences are gapless and strictly rising.
  const seqs = transitions.map((e) => e.data.sequence);
  assert.deepEqual(
    seqs,
    seqs.map((_, i) => i + 1),
    "sequence numbers must be gapless",
  );
  assert.equal(transitions[0]?.data.state, State.OPEN);
  assert.equal(transitions.at(-1)?.data.state, State.CLOSED);
});

test("every published event is a valid CloudEvent carrying full state", async () => {
  const events = await run((delivered) =>
    Effect.gen(function* () {
      const fleet = yield* FleetSource;
      yield* fleet.setFailureRate("payments", 1);
      yield* ticks(40);
      return yield* Ref.get(delivered);
    }),
  );

  assert.ok(events.length > 0);
  for (const e of events) {
    assert.equal(e.specversion, "1.0");
    assert.equal(e.subject, `api://${e.data.apiId}`);
    assert.equal(e.datacontenttype, "application/json");
    // The tick's own instant, not the wall clock. Under TestClock every
    // published `time` is at or after T0 and never ahead of where the clock
    // has been advanced to — which is only assertable because the envelope
    // and the payload now read the same clock.
    const t = Date.parse(e.time);
    assert.ok(t >= T0, `${e.time} is before the clock was anchored`);
    assert.ok(t <= T0 + 40 * CFG.tickMs, `${e.time} is ahead of the test clock`);
    // Full state, not a delta — a subscriber can sync from any single event.
    assert.equal(typeof e.data.healthyEndpoints, "number");
    assert.equal(typeof e.data.totalEndpoints, "number");
    assert.equal(typeof e.data.reportingReplicas, "number");
    // Must be a real instant, never the epoch: a breaker that has not yet
    // changed still has to report when its current condition began.
    assert.ok(Number.isFinite(Date.parse(e.data.observedSince)));
    assert.notEqual(e.data.observedSince, "1970-01-01T00:00:00.000Z");
  }
});

test("snapshots republish current state for late subscribers", async () => {
  const events = await run((delivered) =>
    Effect.gen(function* () {
      // snapshotMs is 15s; 400 ticks at 250ms is 100s of simulated time.
      yield* ticks(400);
      return yield* Ref.get(delivered);
    }),
  );
  const snaps = events.filter((e) => e.type === "egress.circuit.snapshot");
  assert.ok(snaps.length >= 4, `expected periodic snapshots, got ${snaps.length}`);
  // Snapshots deliberately repeat the current sequence — only transitions
  // carry the gapless guarantee.
  assert.ok(snaps.every((s) => s.data.sequence === 0));
});

test("the bus fans events out to subscribers", async () => {
  const seen = await run(() =>
    Effect.gen(function* () {
      const bus = yield* EventBus;
      const fleet = yield* FleetSource;
      yield* fleet.setFailureRate("payments", 1);
      yield* ticks(40);
      return yield* bus.recent;
    }),
  );
  assert.ok(seen.some((e) => e.data.state === State.OPEN));
});

// ---------------------------------------------------------------------------
// Envoy stats parsing — the one production path the simulator never exercises.
// ---------------------------------------------------------------------------

const STATS = {
  stats: [
    { name: "cluster.payments-provider.membership_healthy", value: 2 },
    { name: "cluster.payments-provider.membership_total", value: 6 },
    { name: "cluster.payments-provider.outlier_detection.ejections_active", value: 4 },
    { name: "cluster.payments-provider.upstream_rq_pending_overflow", value: 17 },
    { name: "cluster.payments-provider.upstream_cx_overflow", value: 3 },
    { name: "cluster.payments-provider.upstream_rq_retry_overflow", value: 1 },
    { name: "cluster.tax-calc.membership_healthy", value: 3 },
    { name: "cluster.tax-calc.membership_total", value: 3 },
    // Noise Envoy really emits, which must not be mistaken for a cluster stat.
    { name: "cluster.payments-provider.upstream_rq_2xx", value: 91234 },
    { name: "cluster_manager.active_clusters", value: 3 },
    { name: "http.egress.downstream_rq_total", value: 5000 },
    { name: "listener.0.0.0.0_10000.downstream_cx_total", value: 12 },
  ],
};

test("parses envoy admin stats, summing the three overflow counters", () => {
  const { reports } = parseStats("envoy-00", STATS, 1000, () => true);
  assert.equal(reports.length, 2, "one report per cluster, noise excluded");

  const pay = reports.find((r) => r.apiId === "payments-provider");
  assert.ok(pay);
  assert.equal(pay.healthy, 2);
  assert.equal(pay.total, 6);
  assert.equal(pay.ejectionsActive, 4);
  assert.equal(pay.overflowTotal, 21);
  assert.equal(pay.observedAt, 1000);
});

test("cluster filter drops clusters that are not tracked APIs", () => {
  const { reports } = parseStats("envoy-00", STATS, 1000, (c) => c === "tax-calc");
  assert.deepEqual(
    reports.map((r) => r.apiId),
    ["tax-calc"],
  );
});

/**
 * Zero is not a neutral default here: `{ healthy: 0, total: 6 }` is how Envoy
 * says every host in this cluster is gone.
 */
test("a cluster missing either membership gauge is not reported at all", () => {
  const { reports, incomplete } = parseStats(
    "envoy-00",
    { stats: [{ name: "cluster.x.membership_total", value: 4 }] },
    1000,
    () => true,
  );
  assert.deepEqual(reports, [], "no vote can be computed, so no vote is cast");
  assert.deepEqual(incomplete, ["x"], "and the caller is told, rather than left short a replica");

  // The mirror case: healthy without total would have voted OK, which hides a
  // problem rather than inventing one — still a reading nobody reported.
  assert.deepEqual(
    parseStats("envoy-00", { stats: [{ name: "cluster.x.membership_healthy", value: 4 }] }, 1000, () => true)
      .reports,
    [],
  );

  // A matched stat carrying no value at all is absent, not zero.
  assert.deepEqual(
    parseStats(
      "envoy-00",
      { stats: [{ name: "cluster.x.membership_total", value: 4 }, { name: "cluster.x.membership_healthy" }] },
      1000,
      () => true,
    ).reports,
    [],
  );
});

/** The consequence, which is not visible until the number reaches the breaker. */
test("an incomplete stat set can no longer publish a total outage", () => {
  const partial = { stats: [{ name: "cluster.payments-provider.membership_total", value: 6 }] };
  let breaker = Breaker.initial("payments-provider", defaultConfig, 1000);
  for (const id of ["envoy-00", "envoy-01", "envoy-02"]) {
    for (const report of parseStats(id, partial, 1000, () => true).reports) {
      breaker = Breaker.ingest(breaker, report);
    }
  }
  const [stepped, change] = Breaker.step(breaker, 1000, defaultConfig);
  assert.equal(stepped.lastVotes.DOWN, 0, "nobody said the cluster was down, so nobody votes DOWN");
  assert.ok(O.isNone(change), "and nothing is published");
  // Before the fix this run produced three DOWN votes and published
  // CLOSED -> OPEN with reason ALL_ENDPOINTS_EJECTED.
});

test("the three overflow counters still default to zero, which is their identity", () => {
  const { reports } = parseStats(
    "envoy-00",
    {
      stats: [
        { name: "cluster.x.membership_healthy", value: 3 },
        { name: "cluster.x.membership_total", value: 3 },
      ],
    },
    1000,
    () => true,
  );
  assert.equal(reports[0]?.overflowTotal, 0);
  assert.equal(reports[0]?.ejectionsActive, 0);
});

// ---------------------------------------------------------------------------
// Delivery integrity, as a pure function.
//
// This is what /api/subscriber reports and what the README points at when it
// claims the stream is gapless and non-repeating, and it had no test at all.
// ---------------------------------------------------------------------------

const changed = (apiId: string, sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type: "egress.circuit.state_changed",
  source: "test",
  subject: `api://${apiId}`,
  id: `id-${apiId}-${sequence}`,
  time: new Date(0).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId,
    sequence,
    previousState: "CLOSED",
    state: "OPEN",
    reason: "OUTLIER_EJECTION",
    healthyEndpoints: 0,
    totalEndpoints: 6,
    observedSince: new Date(0).toISOString(),
    reportingReplicas: 3,
  },
});

const snapshotOf = (apiId: string, sequence: number): CircuitEvent => ({
  ...changed(apiId, sequence),
  type: "egress.circuit.snapshot",
});

const fold = (events: ReadonlyArray<CircuitEvent>) =>
  events.reduce(record, emptyIntegrity);

test("a consecutive run is neither gapped nor duplicated", () => {
  const i = fold([1, 2, 3, 4].map((n) => changed("payments", n)));
  assert.equal(i.received, 4);
  assert.equal(i.duplicates, 0);
  assert.deepEqual(i.gaps, []);
});

test("a skipped sequence is a gap", () => {
  const i = fold([1, 2, 5].map((n) => changed("payments", n)));
  assert.equal(i.gaps.length, 1);
  assert.match(i.gaps[0]!, /payments: jumped 2 -> 5/);
});

test("a repeated sequence is a duplicate", () => {
  const i = fold([1, 2, 2].map((n) => changed("payments", n)));
  assert.equal(i.duplicates, 1);
  assert.deepEqual(i.gaps, []);
});

test("a sequence that goes backwards is a duplicate too", () => {
  // The shape a leadership bug produces: an instance resumes from stale
  // in-memory state and republishes numbers a later leader already used.
  // Counting only exact repeats left this invisible.
  const i = fold([10, 11, 12, 8, 9].map((n) => changed("payments", n)));
  assert.equal(i.duplicates, 2, "8 and 9 both reuse a sequence already published");
  assert.deepEqual(i.gaps, [], "going backwards is not a gap");
});

test("snapshots repeat the current sequence and are exempt", () => {
  const i = fold([
    changed("payments", 1),
    snapshotOf("payments", 1),
    snapshotOf("payments", 1),
    changed("payments", 2),
  ]);
  assert.equal(i.snapshots, 2);
  assert.equal(i.duplicates, 0, "a snapshot repeating the sequence is the contract, not a breach");
  assert.deepEqual(i.gaps, []);
});

test("sequences are tracked per API, not globally", () => {
  const i = fold([
    changed("payments", 1),
    changed("shipping", 1),
    changed("payments", 2),
    changed("shipping", 2),
  ]);
  assert.equal(i.duplicates, 0);
  assert.deepEqual(i.gaps, []);
});
