import { Clock, Duration, Effect, Exit, Fiber, Ref, Scope } from "effect";
import { Rmq, RmqError } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  CONTROL_EXCHANGE_OPTIONS,
  encodeCircuitEvent,
  routingKeyFor,
} from "@egress/rmq/ControlPlane.ts";
import { DeliveryFailed } from "@egress/domain/Model.ts";
import { DEAD_LETTER_BUFFER, DELIVERY_RETRY } from "./Events.ts";
import type { SinkImpl } from "./Events.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Consecutive failed *attempts* (not deliveries) before this sink calls itself
 * not ready. Two land at ≈4.1s of confirm timeouts, inside the 5s lease TTL; three
 * (≈6.3s) let the standby win on TTL while this leader still reported itself
 * leader. One would let a single transient NACK flip leadership.
 */
const CONSECUTIVE_FAILURE_THRESHOLD = 2;

/**
 * How long a failure streak counts against readiness. It has to expire on its own:
 * an instance that stepped down publishes nothing, so no success would ever reset
 * it, and after a blip on both instances nobody would lead again.
 */
const FAILURE_STREAK_WINDOW_MS = 10_000;

const PUBLISH_CONFIRM_TIMEOUT = Duration.seconds(2);

/**
 * Publishes each CircuitEvent to `circuit.control`: bounded retry, dead-letter on
 * exhaustion, delivery forked off the tick loop. A dropped link is detected by the
 * AMQP heartbeat (1s, Client.ts) through `rmq.isConnected`, checked on every
 * attempt — see ADR 017.
 */
export const makeAmqpControlPlaneSink: Effect.Effect<SinkImpl, RmqError, Rmq> = Effect.gen(
  function* () {
    const rmq = yield* Rmq;
    yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, CONTROL_EXCHANGE_OPTIONS);

    const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
    /** Consecutive attempts that failed, and when the last one did; reset on the next successful attempt. */
    const consecutiveFailures = yield* Ref.make({ count: 0, lastAt: 0 });

    /** Deliveries fork here, not in the sink's scope, so a demotion can interrupt them without tearing the sink down. */
    const deliveryScope = yield* Ref.make(yield* Scope.make());

    /**
     * The confirm-failure streak. Only a success resets it early, so `publish`
     * consults it on retries only: on a first attempt a stale streak would block
     * the one delivery that could prove the connection recovered.
     */
    const failureStreakActive = Effect.all([Ref.get(consecutiveFailures), Clock.currentTimeMillis]).pipe(
      Effect.map(
        ([{ count, lastAt }, now]) => count >= CONSECUTIVE_FAILURE_THRESHOLD && now - lastAt < FAILURE_STREAK_WINDOW_MS,
      ),
    );

    /**
     * `isConnected` flips back the moment a connection recovers, so it is safe on
     * every attempt; checking it per attempt is what stops a retry chain already in
     * flight (~8.7s) from outliving the lease.
     */
    const knownBad = Effect.all([failureStreakActive, rmq.isConnected]).pipe(
      Effect.map(([streak, connected]) => streak || !connected),
    );

    const publish = (event: CircuitEvent, isRetry: boolean) =>
      Effect.flatMap(Clock.currentTimeMillis, (startedAt) => (isRetry ? knownBad : Effect.map(rmq.isConnected, (connected) => !connected)).pipe(
        Effect.flatMap((bad) =>
          bad
            ? Effect.fail(
                new DeliveryFailed({
                  sink: "amqp",
                  apiId: event.data.apiId,
                  cause: "connection already known bad",
                }),
              )
            : rmq.publisherToExchange(CONTROL_EXCHANGE, routingKeyFor(event.data.apiId)).pipe(
                Effect.flatMap((pub) => rmq.send(pub, encodeCircuitEvent(event))),
                Effect.mapError(
                  (e: RmqError) =>
                    new DeliveryFailed({ sink: "amqp", apiId: event.data.apiId, cause: String(e.cause) }),
                ),
                // A broker whose packets are being dropped never confirms and never errors: without
                // this the publish waits forever, nothing counts as failed, and the leader never steps down.
                Effect.timeoutOrElse({
                  duration: PUBLISH_CONFIRM_TIMEOUT,
                  orElse: () =>
                    Effect.fail(new DeliveryFailed({ sink: "amqp", apiId: event.data.apiId, cause: "no publish confirm within 2s" })),
                }),
                // Counted per attempt, before DELIVERY_RETRY decides, so readiness can flip mid-delivery.
                // The streak is attempts in sequence: one already under way when another failed
                // (the aggregator delivers a tick's transitions together) fails of the same cause
                // and is not counted again, or one blip would read as two and skip every retry.
                Effect.tapError(() =>
                  Clock.currentTimeMillis.pipe(
                    Effect.flatMap((now) =>
                      Ref.update(consecutiveFailures, (streak) =>
                        startedAt >= streak.lastAt ? { count: streak.count + 1, lastAt: now } : streak,
                      ),
                    ),
                  ),
                ),
              ),
        ),
      ));

    /** One publish and its confirm, retried per DELIVERY_RETRY; only attempts after the first count as retries. */
    const attempt = Effect.fnUntraced(function* (event: CircuitEvent) {
      const isRetry = yield* Ref.make(false);
      return yield* Effect.suspend(() =>
        Ref.getAndSet(isRetry, true).pipe(Effect.flatMap((retry) => publish(event, retry))),
      ).pipe(
        Effect.retry(DELIVERY_RETRY),
        Effect.tap(() => Ref.set(consecutiveFailures, { count: 0, lastAt: 0 })),
      );
    });

    /**
     * `Fiber.await`, not `join`: the caller is the tick loop, which must survive a
     * demotion interrupting the delivery. A failure is returned so the checkpoint
     * does not advance past an event the broker never took.
     */
    const deliver = Effect.fnUntraced(function* (event: CircuitEvent) {
      const scope = yield* Ref.get(deliveryScope);
      const fiber = yield* Effect.forkIn(attempt(event), scope);
      const exit = yield* Fiber.await(fiber);
      if (Exit.isSuccess(exit)) return;
      const failure = new DeliveryFailed({
        sink: "amqp",
        apiId: event.data.apiId,
        cause: String(exit.cause),
      });
      // Bounded: an unreachable broker would otherwise grow this for the process's life.
      yield* Ref.update(dead, (xs) => [...xs, failure].slice(-DEAD_LETTER_BUFFER));
      return yield* Effect.fail(failure);
    });

    // No outbox here: the daemons re-learn the state from the next snapshot.
    const ready = Effect.map(knownBad, (bad) => !bad);

    /**
     * On demotion: interrupt in-flight deliveries (not merely abandon them, or a
     * retry could land a sequence the new leader has moved past), then drop the
     * sockets. The close is forked because a fiber parked on the dead socket only
     * unblocks once `rmq.resetConnection` destroys it.
     */
    const resetConnection = Effect.gen(function* () {
      const fresh = yield* Scope.make();
      const previous = yield* Ref.getAndSet(deliveryScope, fresh);
      yield* Effect.forkChild(Scope.close(previous, Exit.void));
      yield* rmq.resetConnection;
    });

    return {
      name: "amqp",
      deliver,
      deadLetters: Ref.get(dead),
      drainOutbox: Effect.succeed(0),
      ready,
      resetConnection,
    };
  },
);
