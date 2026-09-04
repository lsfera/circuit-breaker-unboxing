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
import { EventBusLayer, EventSink } from "../src/Events.ts";
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

/**
 * The case the test above does not reach: an instance that led, *lost* the
 * lease, and is later promoted again — with its own breakers still warm in
 * memory from the first stint.
 *
 * Rehydration only fires for APIs the instance has no breaker for, so a warm
 * registry sails straight past it and resumes from whatever sequence this
 * instance last used itself — while whoever led in between has already
 * published past that number. Two different payloads under one sequence,
 * which is precisely the break `/api/subscriber` exists to detect. Losing
 * the lease therefore has to drop the registry, so that a re-promotion is
 * indistinguishable from the cold start the test above covers.
 *
 * The instance that leads in between is modelled by its effect on the shared
 * coordinator rather than as a second Aggregator: it takes the lease (which
 * is what demotes A) and moves the API on by five transitions. Whether it is
 * a whole instance is beside the point here — the failover test above
 * already covers that — and what matters is what A does when it comes back.
 */
test("a re-promoted instance resumes from the checkpoint, not its own stale sequence", async () => {
  const { firstStint, secondStint, handoffSequence } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        const marker = yield* Ref.make(0);
        const handoff = yield* Ref.make(0);

        yield* Effect.gen(function* () {
          const fleet = yield* FleetSource;
          yield* ticks(8);
          yield* fleet.setFailureRate("payments", 1);
          yield* ticks(60); // A drives CLOSED -> ... -> OPEN and publishes it

          const soFar = yield* Ref.get(delivered);
          const last = soFar.filter((e) => e.type === "egress.circuit.state_changed").at(-1);
          assert.ok(last, "A must publish during its first stint");
          yield* Ref.set(marker, soFar.length);

          // Someone else leads. A's lease lapses first, so this is a genuine
          // handoff with a strictly higher token.
          yield* TestClock.adjust(Duration.millis(2000));
          const token = yield* coordination.leaderElection.tryAcquireOrRenew("B", 1000);
          assert.ok(Option.isSome(token), "the other instance must be able to acquire");
          const advanced = last.data.sequence + 5;
          yield* Ref.set(handoff, advanced);
          yield* coordination.checkpointStore.save("payments", token.value, {
            state: "CLOSED",
            reason: "PROBE_SUCCEEDED",
            sequence: advanced,
            changedAt: yield* Effect.clockWith((c) => c.currentTimeMillis),
            openBackoffMs: CFG.openMs,
          });

          // A ticks again. Its first ticks find B's lease still live — that
          // is where it learns it was demoted — and once that lease lapses
          // it takes over. Its own fleet is still failing, so it has a real
          // transition to publish as soon as it does.
          yield* ticks(80);
        }).pipe(
          Effect.provide(instanceLayer("A", coordination, delivered)),
          Effect.provideService(Config, CFG),
        );

        const all = yield* Ref.get(delivered);
        const split = yield* Ref.get(marker);
        return {
          firstStint: all.slice(0, split),
          secondStint: all.slice(split),
          handoffSequence: yield* Ref.get(handoff),
        };
      }),
      TestClock.layer(),
    ),
  );

  const changes = (xs: ReadonlyArray<CircuitEvent>) =>
    xs.filter((e) => e.type === "egress.circuit.state_changed");

  const before = changes(firstStint);
  const after = changes(secondStint);
  assert.ok(before.length > 0, "A must publish during its first stint");
  assert.ok(after.length > 0, "A must publish again once it is re-promoted");

  assert.equal(
    after[0]!.data.sequence,
    handoffSequence + 1,
    `a re-promoted instance must continue from the checkpoint (${handoffSequence}), ` +
      `not from its own stale in-memory sequence (${before.at(-1)!.data.sequence}); ` +
      `got ${after.map((e) => e.data.sequence).join(",")}`,
  );
  assert.equal(
    after[0]!.data.previousState,
    "CLOSED",
    "previousState must come from the checkpoint the other leader left behind",
  );
  assert.ok(
    after.every((e) => e.data.sequence > handoffSequence),
    "no event after re-promotion may reuse a sequence the other leader already published",
  );
});
