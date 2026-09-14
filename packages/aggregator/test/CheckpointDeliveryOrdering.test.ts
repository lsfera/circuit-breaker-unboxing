import { test } from "node:test";
import assert from "node:assert/strict";
import { Duration, Effect, Layer, Option as O, Ref } from "effect";
import { TestClock } from "effect/testing";
import { Aggregator } from "../src/Aggregator.ts";
import {
  HaSettings,
  LeaderElection,
  CheckpointStore,
  makeInMemoryCoordination,
} from "../src/Coordination.ts";
import { EventBus, EventSink } from "../src/Events.ts";
import { FleetSource, SimFleetLayer } from "../src/FleetSource.ts";
import { Config, defaultConfig, DeliveryFailed, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * The bug this guards against: a leader used to checkpoint a sequenced
 * transition *before* its publish reached the broker, because delivery was
 * fire-and-forget. A leader cut off from RabbitMQ (Redis still reachable)
 * could checkpoint sequences whose publish never landed, step down moments
 * later, and hand a successor a checkpoint ahead of what any daemon actually
 * saw — a permanent gap, since the successor resumes *after* the unconfirmed
 * sequence rather than re-deriving it.
 *
 * The fix: `Aggregator.ts`'s `attemptTick` now awaits `sink.deliver` for a
 * sequenced event and only checkpoints on confirmation; on failure it stops
 * processing this tick's remaining sequenced events and steps down, so the
 * next leader (this instance re-acquiring, or another) resumes from the last
 * *confirmed* checkpoint and re-publishes the sequence the failed leader
 * could not deliver.
 */

const CFG = { ...defaultConfig, dwellMs: 500, minStateMs: 500, openMs: 1000 };
const T0 = Date.parse("2026-09-02T12:00:00.000Z");
const SPECS = [{ apiId: "payments", endpoints: 6, rps: 900, failureRate: 0 }];

/**
 * Succeeds (and records) the first `succeedFirst` SEQUENCED deliveries, then
 * fails every one after — the shape of the chaos run this reproduces:
 * confirmed publishes, then a broker that stops confirming mid-run. Snapshot
 * events always go through unconditionally and are not counted: they are
 * fire-and-forget by design (see Aggregator.ts), so gating them here would
 * make which attempt trips the budget depend on fork scheduling rather than
 * on the sequenced events this test is about.
 */
const FailableSink = (
  succeedFirst: number,
  delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>,
  attempts: Ref.Ref<number>,
) =>
  Layer.succeed(EventSink, {
    name: "failable",
    deliver: (event: CircuitEvent) =>
      event.type !== SEQUENCED_EVENT
        ? Effect.void
        : Ref.modify(attempts, (n) => [n, n + 1] as const).pipe(
            Effect.flatMap((n) =>
              n < succeedFirst
                ? Ref.update(delivered, (xs) => [...xs, event]).pipe(Effect.asVoid)
                : Effect.fail(
                    new DeliveryFailed({
                      sink: "failable",
                      apiId: event.data.apiId,
                      cause: "simulated: broker stopped confirming",
                    }),
                  ),
            ),
          ),
    deadLetters: Effect.succeed([]),
    drainOutbox: Effect.succeed(0),
    ready: Effect.succeed(true),
    resetConnection: Effect.void,
  });

/** A sink that records everything it was handed and never fails — instance B's. */
const RecordingSink = (into: Ref.Ref<ReadonlyArray<CircuitEvent>>) =>
  Layer.succeed(EventSink, {
    name: "recording",
    deliver: (event: CircuitEvent) => Ref.update(into, (xs) => [...xs, event]),
    deadLetters: Effect.succeed([]),
    drainOutbox: Effect.succeed(0),
    ready: Effect.succeed(true),
    resetConnection: Effect.void,
  });

const instanceLayer = (
  instanceId: string,
  coordination: {
    readonly leaderElection: typeof LeaderElection.Service;
    readonly checkpointStore: typeof CheckpointStore.Service;
  },
  sink: Layer.Layer<EventSink>,
  leaseTtlMs = 1000,
) =>
  Aggregator.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        SimFleetLayer(SPECS, 5),
        EventBus.layer,
        sink,
        Layer.succeed(LeaderElection, coordination.leaderElection),
        Layer.succeed(CheckpointStore, coordination.checkpointStore),
        Layer.succeed(HaSettings, { instanceId, leaseTtlMs }),
      ),
    ),
    // TestClock shared from the outer scope, same as Coordination.test.ts's
    // two-instance harness, so both "processes" advance on one clock.
  );

const ticks = (n: number) =>
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    for (let i = 0; i < n; i++) {
      yield* agg.tick;
      yield* TestClock.adjust(Duration.millis(CFG.tickMs));
    }
  });

test("a failed sequenced delivery does not advance the checkpoint, and steps the leader down", async () => {
  const { checkpoint, delivered, stillLeader } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        const attempts = yield* Ref.make(0);

        const stillLeader = yield* Effect.gen(function* () {
          const fleet = yield* FleetSource;
          yield* fleet.setFailureRate("payments", 1);
          // CLOSED -> OPEN: the one delivery FailableSink lets through, so
          // this checkpoints normally, same as any other test.
          yield* ticks(60);

          const afterFirstCheckpoint = yield* coordination.checkpointStore.load("payments");
          assert.ok(O.isSome(afterFirstCheckpoint), "the first transition must checkpoint");
          assert.equal(afterFirstCheckpoint.value.state, "OPEN");

          // OPEN -> HALF_OPEN fires on elapsed backoff alone (see Breaker.ts's
          // waitOutBackoff), regardless of the fleet's own reports — so this
          // is a second sequenced event whose delivery FailableSink now fails.
          yield* ticks(40);

          return yield* Aggregator.pipe(Effect.flatMap((agg) => agg.isLeader));
        }).pipe(
          Effect.provide(instanceLayer("A", coordination, FailableSink(1, delivered, attempts))),
          Effect.provideService(Config, CFG),
        );

        const checkpoint = yield* coordination.checkpointStore.load("payments");
        return { checkpoint, delivered: yield* Ref.get(delivered), stillLeader };
      }),
      TestClock.layer(),
    ),
  );

  assert.equal(stillLeader, false, "a leader whose publish was never confirmed must step down");
  assert.equal(delivered.length, 1, "only the confirmed delivery may have gone out");
  assert.ok(O.isSome(checkpoint));
  assert.equal(
    checkpoint.value.sequence,
    delivered[0]!.data.sequence,
    "the checkpoint must sit at the last CONFIRMED sequence, not one delivery failed to confirm",
  );
});

test("a second instance resumes at last-confirmed + 1 — the sequence the failed leader could not deliver, not a gap past it", async () => {
  const { handoffSequence, firstFromB } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const deliveredA = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        const attempts = yield* Ref.make(0);

        yield* Effect.gen(function* () {
          const fleet = yield* FleetSource;
          yield* fleet.setFailureRate("payments", 1);
          yield* ticks(60); // CLOSED -> OPEN, confirmed and checkpointed
          yield* ticks(40); // OPEN -> HALF_OPEN attempted, delivery fails, steps down
        }).pipe(
          Effect.provide(instanceLayer("A", coordination, FailableSink(1, deliveredA, attempts))),
          Effect.provideService(Config, CFG),
        );

        const handoff = yield* coordination.checkpointStore.load("payments");
        assert.ok(O.isSome(handoff), "instance A must have left a confirmed checkpoint behind");
        const handoffSequence = handoff.value.sequence;

        // A released the lease on stepping down (releaseAndHoldOff) and A's
        // own hold-off is per-instance, in-memory state — it does not bind B.
        const deliveredB = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        yield* Effect.gen(function* () {
          yield* ticks(10);
        }).pipe(
          Effect.provide(instanceLayer("B", coordination, RecordingSink(deliveredB))),
          Effect.provideService(Config, CFG),
        );

        const bEvents = yield* Ref.get(deliveredB);
        const firstFromB = bEvents.find((e) => e.type === "egress.circuit.state_changed");
        return { handoffSequence, firstFromB };
      }),
      TestClock.layer(),
    ),
  );

  assert.ok(firstFromB, "instance B must publish the sequence A could not");
  assert.equal(
    firstFromB!.data.sequence,
    handoffSequence + 1,
    `B must publish exactly last-confirmed + 1 (${handoffSequence + 1}) — the sequence A ` +
      `attempted but never confirmed — not skip past it and not repeat ${handoffSequence}`,
  );
});
