import { test } from "node:test";
import assert from "node:assert/strict";
import { Duration, Effect, Fiber, Ref } from "effect";
import { TestClock } from "effect/testing";
import { Rmq, RmqError } from "@egress/rmq/Client.ts";
import type { RmqService } from "@egress/rmq/Client.ts";
import { makeAmqpControlPlaneSink } from "../src/AmqpControlPlaneSink.ts";
import type { SinkImpl } from "../src/Events.ts";
import { SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * A stand-in for the whole amqplib-backed client: `isConnected` and `send`
 * are the only two members `makeAmqpControlPlaneSink` actually calls after
 * startup, both driven by Refs the test controls; everything else is a
 * stub that must type-check but is never invoked.
 */
const fakeRmq = (
  connected: Ref.Ref<boolean>,
  sendResult: Ref.Ref<Effect.Effect<void, RmqError>>,
): RmqService => {
  const notUsed = Effect.die("not used in this test");
  return {
    declareQueue: () => notUsed,
    declareTopicExchange: () => Effect.succeed<unknown>(undefined),
    bind: () => notUsed,
    consume: () => notUsed,
    publisherToExchange: (exchange, routingKey) => Effect.succeed({ exchange, routingKey }),
    publisherToQueue: () => notUsed,
    send: () => Effect.flatMap(Ref.get(sendResult), (e) => e),
    cancelConsumer: () => Effect.void,
    closeConsumer: () => Effect.void,
    lost: Effect.never,
    isConnected: Ref.get(connected),
    resetConnection: Effect.void,
  };
};

const event = (sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type: SEQUENCED_EVENT,
  source: "test",
  subject: "api://payments",
  id: `id-${sequence}`,
  time: new Date(0).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId: "payments",
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

const failure = () => Effect.fail(new RmqError({ operation: "send", cause: "simulated" }));

/**
 * `sink.deliver` blocks on its own forked fiber (see AmqpControlPlaneSink.ts),
 * so calling it directly under TestClock deadlocks: nothing advances the
 * virtual clock the fiber is waiting on until `deliver` itself returns. Per
 * TestClock's own doc ("fork the effect being tested, then adjust the clock
 * time"), fork first so the retry chain can register its sleeps, then adjust.
 */
const deliverAndSettle = (
  sink: Pick<SinkImpl, "deliver">,
  event: CircuitEvent,
  duration: Duration.Input,
) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(sink.deliver(event));
    yield* TestClock.adjust(duration);
    yield* Fiber.await(fiber);
  });

/**
 * Comfortably more than DELIVERY_RETRY's worst case (exponential(100ms) x3 ≈
 * 700ms of virtual delay between attempts) — the fake `send` never does real
 * I/O, so TestClock.adjust runs the whole retry chain to completion.
 */
const SETTLE = Duration.seconds(3);

test("two consecutive failed attempts flip readiness off; a success resets the counter", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const connected = yield* Ref.make(true);
        const sendResult = yield* Ref.make<Effect.Effect<void, RmqError>>(failure());
        const sink = yield* makeAmqpControlPlaneSink.pipe(
          Effect.provideService(Rmq, fakeRmq(connected, sendResult)),
        );

        assert.equal(yield* sink.ready, true, "ready before anything has been attempted");

        // One fully-retried delivery already contains 4 failed attempts
        // (DELIVERY_RETRY), well past CONSECUTIVE_FAILURE_THRESHOLD's 2 — no
        // second delivery is needed to see readiness flip.
        yield* deliverAndSettle(sink, event(0), SETTLE);
        assert.equal(
          yield* sink.ready,
          false,
          "two consecutive failed attempts must flip readiness off",
        );
        const failed = yield* sink.deadLetters;
        assert.equal(failed.length, 1, "the exhausted delivery is dead-lettered");

        // Post-fix, a real delivery's own attempts short-circuit once the
        // shared streak is known bad (see AmqpControlPlaneSink.ts's
        // failureStreakActive) — recovery is proven by the heartbeat, which
        // main.ts always runs regardless of delivery traffic, not by a bare
        // real-event success.
        yield* Ref.set(sendResult, Effect.void);
        const probeFiber = yield* Effect.forkChild(sink.probe);
        yield* TestClock.adjust(Duration.seconds(1));
        assert.equal(
          yield* sink.ready,
          true,
          "the heartbeat's own next success resets the consecutive-failure counter",
        );
        yield* Fiber.interrupt(probeFiber);

        yield* deliverAndSettle(sink, event(1), SETTLE);
        assert.equal(
          yield* sink.ready,
          true,
          "a delivery succeeds normally once the connection is known good again",
        );
      }),
      TestClock.layer(),
    ),
  );
});

test("ready is false while disconnected, even with no delivery attempts", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const connected = yield* Ref.make(false);
        const sendResult = yield* Ref.make<Effect.Effect<void, RmqError>>(Effect.void);
        const sink = yield* makeAmqpControlPlaneSink.pipe(
          Effect.provideService(Rmq, fakeRmq(connected, sendResult)),
        );

        assert.equal(yield* sink.ready, false, "not connected means not ready, before any delivery");

        yield* Ref.set(connected, true);
        assert.equal(yield* sink.ready, true, "reconnecting alone is enough with no failures counted");
      }),
      TestClock.layer(),
    ),
  );
});

test("a failure streak expires on its own, so an instance that stepped down can lead again", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const connected = yield* Ref.make(true);
        const sendResult = yield* Ref.make<Effect.Effect<void, RmqError>>(failure());
        const sink = yield* makeAmqpControlPlaneSink.pipe(
          Effect.provideService(Rmq, fakeRmq(connected, sendResult)),
        );

        for (let i = 0; i < 3; i++) {
          yield* deliverAndSettle(sink, event(i), SETTLE);
        }
        assert.equal(yield* sink.ready, false);

        // A stepped-down instance delivers nothing, so no success can reset the streak.
        yield* TestClock.adjust(Duration.seconds(10));
        assert.equal(yield* sink.ready, true, "with no attempts for the window, readiness returns");
      }),
      TestClock.layer(),
    ),
  );
});

test("the heartbeat alone discovers a dead connection, with nothing ever delivered", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const connected = yield* Ref.make(true);
        // A never-resolving send, same as the "broker never confirms" test
        // below: the fault this exists for is a silent packet drop, not an
        // immediate refusal, so only PUBLISH_CONFIRM_TIMEOUT ever ends an
        // attempt.
        const sendResult = yield* Ref.make<Effect.Effect<void, RmqError>>(Effect.never);
        const sink = yield* makeAmqpControlPlaneSink.pipe(
          Effect.provideService(Rmq, fakeRmq(connected, sendResult)),
        );

        assert.equal(yield* sink.ready, true, "ready before the probe has run at all");

        // `makeAmqpControlPlaneSink` never forks its own probe — see
        // `AmqpSinkImpl.probe`'s comment — so nothing here runs until this
        // test starts it itself, same as main.ts does.
        const probeFiber = yield* Effect.forkChild(sink.probe);
        // PROBE_INTERVAL's comment: worst case is 2×PROBE_INTERVAL +
        // PUBLISH_CONFIRM_TIMEOUT ≈ 4s.
        yield* TestClock.adjust(Duration.seconds(5));

        assert.equal(
          yield* sink.ready,
          false,
          "two consecutive failed heartbeats must flip readiness off on their own, with no event ever delivered",
        );
        assert.equal((yield* sink.deadLetters).length, 0, "a heartbeat is never dead-lettered");

        yield* Fiber.interrupt(probeFiber);
      }),
      TestClock.layer(),
    ),
  );
});

test("a publish the broker never confirms counts as a failure", async () => {
  await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const connected = yield* Ref.make(true);
        const sendResult = yield* Ref.make<Effect.Effect<void, RmqError>>(Effect.never);
        const sink = yield* makeAmqpControlPlaneSink.pipe(
          Effect.provideService(Rmq, fakeRmq(connected, sendResult)),
        );

        for (let i = 0; i < 3; i++) {
          yield* deliverAndSettle(sink, event(i), Duration.seconds(10));
        }
        assert.equal(yield* sink.ready, false, "dropped packets look connected; the missing confirms must flip readiness");
        assert.equal((yield* sink.deadLetters).length, 3);
      }),
      TestClock.layer(),
    ),
  );
});
