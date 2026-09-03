import { test } from "node:test";
import assert from "node:assert/strict";
import { Duration, Effect, Layer, Option, Ref } from "effect";
import { TestClock } from "effect/testing";
import { Aggregator, AggregatorLayer } from "../src/Aggregator.ts";
import {
  CheckpointStore,
  HaSettings,
  LeaderElection,
  makeInMemoryCoordination,
} from "../src/Coordination.ts";
import { EventBus, EventBusLayer, EventSink } from "../src/Events.ts";
import { FleetSource, SimFleetLayer } from "../src/FleetSource.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Two concerns: does the coordination primitive itself resist the exact race
 * it exists to close (Coordination tests, no Aggregator involved), and does
 * a second Aggregator instance actually pick up where a fenced-out first one
 * left off (the failover test, driving two full Aggregator layers that share
 * one in-memory coordinator — two "processes" in one test process).
 */

const CFG = { ...defaultConfig, dwellMs: 500, minStateMs: 500, openMs: 1000 };
const T0 = Date.parse("2026-09-02T12:00:00.000Z");
const SPECS = [{ apiId: "payments", endpoints: 6, rps: 900, failureRate: 0 }];

// ---------------------------------------------------------------------------
// The primitive: fencing must be against the shared lease token, not a
// per-API "last write wins" value — otherwise a stale leader can still win
// on any API the new leader has not published for yet.
// ---------------------------------------------------------------------------

test("renewal keeps the same token; a genuine handoff strictly increases it", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const { leaderElection } = yield* makeInMemoryCoordination;
        yield* TestClock.setTime(T0);

        const first = yield* leaderElection.tryAcquireOrRenew("A", 1000);
        const renewed = yield* leaderElection.tryAcquireOrRenew("A", 1000);
        assert.deepEqual(renewed, first, "renewal by the same holder keeps the token");

        yield* TestClock.adjust(Duration.millis(2000)); // outlive the lease
        const handoff = yield* leaderElection.tryAcquireOrRenew("B", 1000);
        assert.ok(Option.isSome(first) && Option.isSome(handoff));
        assert.ok(
          (handoff as Option.Some<number>).value > (first as Option.Some<number>).value,
          "a real handoff must produce a strictly higher token",
        );
      }),
      TestClock.layer(),
    ),
  );
});

test("a live holder blocks a competing acquire", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const { leaderElection } = yield* makeInMemoryCoordination;
        yield* leaderElection.tryAcquireOrRenew("A", 10_000);
        const blocked = yield* leaderElection.tryAcquireOrRenew("B", 10_000);
        assert.ok(Option.isNone(blocked), "B must not acquire while A's lease is live");
      }),
      TestClock.layer(),
    ),
  );
});

test("a stale token is rejected even for an API no one has checkpointed yet", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const { leaderElection, checkpointStore } = yield* makeInMemoryCoordination;

        const aToken = yield* leaderElection.tryAcquireOrRenew("A", 1000);
        yield* TestClock.adjust(Duration.millis(2000));
        const bToken = yield* leaderElection.tryAcquireOrRenew("B", 1000);
        assert.ok(Option.isSome(aToken) && Option.isSome(bToken));

        // "brand-new-api" has never been checkpointed by anyone. A per-key
        // "last write wins" fence would wrongly let A's stale token win here
        // simply because nothing higher has touched this specific key yet —
        // that was the bug. Fencing against the shared lease token closes it
        // for every key at once, the moment leadership actually changed.
        const staleWrite = yield* checkpointStore
          .save("brand-new-api", (aToken as Option.Some<number>).value, {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 1,
            changedAt: 0,
            openBackoffMs: 4000,
          })
          .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
        assert.equal(staleWrite, "fenced", "A's stale token must be rejected");

        const freshWrite = yield* checkpointStore
          .save("brand-new-api", (bToken as Option.Some<number>).value, {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 1,
            changedAt: 0,
            openBackoffMs: 4000,
          })
          .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
        assert.equal(freshWrite, "ok", "B's current token must be accepted");
      }),
      TestClock.layer(),
    ),
  );
});

// ---------------------------------------------------------------------------
// Through the whole Aggregator: a standby never publishes, and a takeover
// resumes the sequence rather than restarting it.
// ---------------------------------------------------------------------------

/** A sink that records everything it was handed, so we can assert on delivery. */
const RecordingSink = (into: Ref.Ref<ReadonlyArray<CircuitEvent>>) =>
  Layer.succeed(EventSink, {
    name: "recording",
    deliver: (event: CircuitEvent) => Ref.update(into, (xs) => [...xs, event]),
    deadLetters: Effect.succeed([]),
  });

const instanceLayer = (
  instanceId: string,
  coordination: {
    readonly leaderElection: typeof LeaderElection.Service;
    readonly checkpointStore: typeof CheckpointStore.Service;
  },
  delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>,
) =>
  AggregatorLayer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        SimFleetLayer(SPECS, 5),
        EventBusLayer,
        RecordingSink(delivered),
        Layer.succeed(LeaderElection, coordination.leaderElection),
        Layer.succeed(CheckpointStore, coordination.checkpointStore),
        Layer.succeed(HaSettings, { instanceId, leaseTtlMs: 1000 }),
      ),
    ),
    // TestClock is deliberately not provided here — shared from the test's
    // own outer scope so both instances advance on the same simulated clock.
  );

const ticks = (n: number) =>
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    for (let i = 0; i < n; i++) {
      yield* agg.tick;
      yield* TestClock.adjust(Duration.millis(CFG.tickMs));
    }
  });

test("a non-leader instance never publishes, even under total failure", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        // Someone else holds a long lease before this instance ever ticks.
        yield* coordination.leaderElection.tryAcquireOrRenew("someone-else", 1_000_000);
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);

        yield* Effect.gen(function* () {
          const fleet = yield* FleetSource;
          yield* fleet.setFailureRate("payments", 1);
          const agg = yield* Aggregator;
          for (let i = 0; i < 40; i++) {
            yield* agg.tick;
            assert.equal(yield* agg.isLeader, false);
            yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          }
        }).pipe(
          Effect.provide(instanceLayer("standby", coordination, delivered)),
          Effect.provideService(Config, CFG),
        );

        assert.deepEqual(yield* Ref.get(delivered), [], "a standby must never publish");
      }),
      TestClock.layer(),
    ),
  );
});

test("failover resumes sequence and previousState from the checkpoint, not from zero", async () => {
  const { lastFromA, firstFromB } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const deliveredA = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        const deliveredB = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);

        yield* Effect.gen(function* () {
          const fleet = yield* FleetSource;
          yield* ticks(8);
          yield* fleet.setFailureRate("payments", 1);
          yield* ticks(60); // enough for CLOSED -> OPEN, per the existing Aggregator tests
        }).pipe(
          Effect.provide(instanceLayer("A", coordination, deliveredA)),
          Effect.provideService(Config, CFG),
        );

        const aEvents = yield* Ref.get(deliveredA);
        const lastFromA = aEvents
          .filter((e) => e.type === "egress.circuit.state_changed")
          .at(-1);
        assert.ok(lastFromA, "instance A must have published at least one transition");

        // A's lease (1000ms TTL) outlives its last renewal by more than that
        // before B ever calls tryAcquireOrRenew — a genuine handoff, not a
        // renewal.
        yield* TestClock.adjust(Duration.millis(2000));

        yield* Effect.gen(function* () {
          yield* ticks(40); // B's own fleet starts healthy; give it room to settle
        }).pipe(
          Effect.provide(instanceLayer("B", coordination, deliveredB)),
          Effect.provideService(Config, CFG),
        );

        const bEvents = yield* Ref.get(deliveredB);
        const firstFromB = bEvents.find((e) => e.type === "egress.circuit.state_changed");
        return { lastFromA, firstFromB };
      }),
      TestClock.layer(),
    ),
  );

  assert.ok(firstFromB, "instance B must publish at least one transition after taking over");
  assert.equal(
    firstFromB!.data.sequence,
    lastFromA!.data.sequence + 1,
    "sequence must continue across a failover, not restart at zero",
  );
  assert.equal(
    firstFromB!.data.previousState,
    lastFromA!.data.state,
    "previousState must reflect the checkpointed state, not a cold start",
  );
});
