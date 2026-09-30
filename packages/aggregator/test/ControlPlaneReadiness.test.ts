import { test } from "node:test";
import assert from "node:assert/strict";
import { Duration, Effect, Layer, Option as O, Ref } from "effect";
import { TestClock } from "effect/testing";
import { Aggregator } from "../src/Aggregator.ts";
import {
  HaSettings,
  LeaderElection,
  CheckpointStore,
} from "../src/Coordination.ts";
import { EventBus, EventSink, makeWebhookSink } from "../src/Events.ts";
import { FleetSource, SimFleetLayer } from "../src/FleetSource.ts";
import {
  InMemoryCoordinationLayer,
  InMemoryOutboxLayer,
  makeInMemoryCoordination,
} from "./support/InMemory.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * A sink whose readiness the test flips on demand, in the style of
 * Coordination.test.ts's RecordingSink — same recording, plus a controllable
 * `ready`. `deadLetters`/`drainOutbox` are unused by these tests.
 */
const ControllableSink = (
  readyRef: Ref.Ref<boolean>,
  delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>,
  resets?: Ref.Ref<number>,
) =>
  Layer.succeed(EventSink, {
    name: "controllable",
    deliver: (event: CircuitEvent) => Ref.update(delivered, (xs) => [...xs, event]),
    deadLetters: Effect.succeed([]),
    drainOutbox: Effect.succeed(0),
    ready: Ref.get(readyRef),
    resetConnection: resets === undefined ? Effect.void : Ref.update(resets, (n) => n + 1),
  });

const CFG = { ...defaultConfig, dwellMs: 500, minStateMs: 500, openMs: 1000 };
const T0 = Date.parse("2026-09-02T12:00:00.000Z");
const SPECS = [{ apiId: "payments", endpoints: 6, rps: 900, failureRate: 0 }];

const instanceLayer = (
  coordination: {
    readonly leaderElection: typeof LeaderElection.Service;
    readonly checkpointStore: typeof CheckpointStore.Service;
  },
  readyRef: Ref.Ref<boolean>,
  delivered: Ref.Ref<ReadonlyArray<CircuitEvent>>,
  leaseTtlMs = 1000,
  instanceId = "instance",
  resets?: Ref.Ref<number>,
) =>
  Aggregator.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        SimFleetLayer(SPECS, 5),
        EventBus.layer,
        ControllableSink(readyRef, delivered, resets),
        Layer.succeed(LeaderElection, coordination.leaderElection),
        Layer.succeed(CheckpointStore, coordination.checkpointStore),
        Layer.succeed(HaSettings, { instanceId, leaseTtlMs }),
      ),
    ),
  );

test("a leader whose sink becomes not-ready releases the lease within one tick, and publishes nothing afterwards", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const ready = yield* Ref.make(true);
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);

        yield* Effect.gen(function* () {
          const agg = yield* Aggregator;
          const fleet = yield* FleetSource;
          yield* fleet.setFailureRate("payments", 1);

          yield* agg.tick;
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          assert.equal(yield* agg.isLeader, true, "must hold the lease while ready");

          yield* Ref.set(ready, false);
          yield* agg.tick;
          assert.equal(yield* agg.isLeader, false, "must step down on the very tick readiness drops");

          // Not just an in-process flag: a competing instance must be able to
          // acquire immediately, the same property the shutdown release has.
          const taken = yield* coordination.leaderElection.tryAcquireOrRenew("someone-else", 1000);
          assert.ok(O.isSome(taken), "the lease must actually be released, not merely abandoned");
          yield* coordination.leaderElection.release("someone-else");

          const publishedBeforeSilence = yield* Ref.get(delivered);
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          for (let i = 0; i < 20; i++) {
            yield* agg.tick;
            yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          }
          const publishedAfter = yield* Ref.get(delivered);
          assert.deepEqual(
            publishedAfter,
            publishedBeforeSilence,
            "nothing may be published while the sink is not ready",
          );
        }).pipe(
          Effect.provide(instanceLayer(coordination, ready, delivered)),
          Effect.provideService(Config, CFG),
        );
      }),
      TestClock.layer(),
    ),
  );
});

test("an instance whose sink is not ready never acquires, even when the lease is free", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const ready = yield* Ref.make(false);
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);

        yield* Effect.gen(function* () {
          const agg = yield* Aggregator;
          for (let i = 0; i < 40; i++) {
            yield* agg.tick;
            assert.equal(yield* agg.isLeader, false);
            yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          }
        }).pipe(
          Effect.provide(instanceLayer(coordination, ready, delivered)),
          Effect.provideService(Config, CFG),
        );

        assert.deepEqual(yield* Ref.get(delivered), [], "a never-ready instance must never publish");
        const stillFree = yield* coordination.leaderElection.tryAcquireOrRenew("probe", 1000);
        assert.ok(O.isSome(stillFree), "the lease must still be free — this instance never took it");
      }),
      TestClock.layer(),
    ),
  );
});

test("after readiness returns, this instance does not acquire until the hold-off (one lease TTL) passes", async () => {
  const leaseTtlMs = 1000;
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const ready = yield* Ref.make(true);
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);

        yield* Effect.gen(function* () {
          const agg = yield* Aggregator;

          yield* agg.tick;
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          assert.equal(yield* agg.isLeader, true, "acquires while ready");

          yield* Ref.set(ready, false);
          yield* agg.tick;
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          assert.equal(yield* agg.isLeader, false, "steps down once not ready");

          yield* Ref.set(ready, true);
          yield* agg.tick;
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          assert.equal(
            yield* agg.isLeader,
            false,
            "must not reacquire on the very next tick after readiness returns",
          );

          // Run out the rest of the one-lease-TTL hold-off.
          yield* TestClock.adjust(Duration.millis(leaseTtlMs));
          yield* agg.tick;
          assert.equal(
            yield* agg.isLeader,
            true,
            "must acquire again once the hold-off has fully elapsed",
          );
        }).pipe(
          Effect.provide(instanceLayer(coordination, ready, delivered, leaseTtlMs)),
          Effect.provideService(Config, CFG),
        );
      }),
      TestClock.layer(),
    ),
  );
});

test("the webhook-only configuration is unaffected: an always-ready sink never triggers a step-down", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);

        yield* Effect.gen(function* () {
          const agg = yield* Aggregator;
          for (let i = 0; i < 30; i++) {
            yield* agg.tick;
            assert.equal(
              yield* agg.isLeader,
              true,
              `must hold the lease on tick ${i} — an unreachable webhook subscriber is not a leadership question`,
            );
            yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          }
        }).pipe(
          Effect.provide(
            Aggregator.layer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  SimFleetLayer(SPECS, 5),
                  EventBus.layer,
                  // Port 1 is privileged and never bound in this sandbox, so
                  // every delivery fails fast — proving readiness really is
                  // independent of the sink's own delivery outcome.
                  Layer.effect(EventSink, makeWebhookSink("http://127.0.0.1:1/subscriber/webhook")).pipe(
                    Layer.provide(InMemoryOutboxLayer),
                  ),
                  InMemoryCoordinationLayer,
                ),
              ),
            ),
          ),
          Effect.provideService(Config, CFG),
        );
      }),
      TestClock.layer(),
    ),
  );
});

test("stepping down fences the sink exactly once, not on every standby tick", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        yield* TestClock.setTime(T0);
        const coordination = yield* makeInMemoryCoordination;
        const ready = yield* Ref.make(true);
        const delivered = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
        const resets = yield* Ref.make(0);

        yield* Effect.gen(function* () {
          const agg = yield* Aggregator;
          yield* agg.tick;
          yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          assert.equal(yield* agg.isLeader, true);
          assert.equal(yield* Ref.get(resets), 0, "a leader that stays ready is never fenced");

          yield* Ref.set(ready, false);
          for (let i = 0; i < 10; i++) {
            yield* agg.tick;
            yield* TestClock.adjust(Duration.millis(CFG.tickMs));
          }
          assert.equal(yield* Ref.get(resets), 1, "fenced once, on the transition");
        }).pipe(
          Effect.provide(instanceLayer(coordination, ready, delivered, 1000, "instance", resets)),
          Effect.provideService(Config, CFG),
        );
      }),
      TestClock.layer(),
    ),
  );
});
