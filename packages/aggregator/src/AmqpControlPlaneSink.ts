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
 * Consecutive *attempts* — each a send plus its own confirm wait, not a
 * fully-retried delivery — that may fail back to back before this sink calls
 * itself not ready. `rmq.isConnected` alone is not enough — the bug this
 * exists for is a firewall dropping every packet on :5672 while the socket
 * still looks open, which the client's own `connected` flag does not notice
 * until a heartbeat times out.
 *
 * Counted per attempt rather than per delivery so readiness reacts inside
 * the first stuck delivery instead of waiting for DELIVERY_RETRY to exhaust
 * it (four attempts, ≈8.7s) and then two more deliveries on top of that.
 * Two attempts land at ≈4.1s of consecutive confirm timeouts (2s + 100ms
 * backoff + 2s) — inside `Coordination.ts`'s 5000ms lease TTL a standby
 * would otherwise wait out, with margin, rather than racing it: this was
 * 3 attempts (≈6.3s) until a live run of `net-control-partition+outage`
 * showed the standby winning on TTL expiry before this sink's own watchdog
 * ever finished counting, leaving the demoted leader reporting itself
 * leader for several seconds after it no longer was. Two, not one, so a
 * single unlucky event (a transient NACK, a channel mid-rebuild) still
 * cannot flip leadership on its own — just not three, which left this
 * step-down slower than the failover it exists to beat.
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
 * Readiness only reacted to a *real* publish attempt, and a real publish only
 * happens on a transition or `snapshotMs`'s 15s cadence — so a leader whose
 * broker connection died between events kept renewing its lease and calling
 * itself ready for however long it took the breaker to independently notice
 * the same outage and give this sink something to send. Measured live on
 * `net-control-partition+outage`: Envoy's own outlier-detection timing took
 * ≈9.5s to eject every endpoint and trip the breaker, on top of which
 * CONSECUTIVE_FAILURE_THRESHOLD's own ≈4.1s still had to run — comfortably
 * past the 5000ms lease TTL the standby was racing on the whole time.
 *
 * An earlier version of this sink fixed that with its own forked,
 * message-level heartbeat — publish-and-confirm on a fixed clock,
 * independent of whether the breaker had anything to send. It worked, but it
 * duplicated something already sitting misconfigured one layer down:
 * `Client.ts`'s `amqp.connect` never set a `heartbeat`, so it negotiated
 * RabbitMQ's own 60s default, and amqplib already tracks broker activity and
 * closes the connection on its own once that times out — the same
 * `isConnected` this sink already reads, with zero application code. Tuning
 * that down (`heartbeat: 1`, Client.ts) and consulting `rmq.isConnected` on
 * every attempt below, not just at tick start, measured *faster* than the
 * bespoke heartbeat had: 2.76s to detect a live `net-control-partition+outage`
 * drop, 3.46s to step down, against the message-level heartbeat's own
 * ≈3.8s/≈4s. See docs/decisions/017's second amendment for the removed
 * heartbeat and the run that replaced it.
 */

/**
 * A peer to WebhookSink, publishing the same CircuitEvent to
 * `circuit.control` instead of POSTing a webhook — the transport
 * `docs/rmq-control-plane.md`'s daemon fleet actually subscribes to. Same
 * `EventSink` contract: bounded retry, dead-letter on exhaustion, delivery
 * forked off the hot path so a slow or unreachable broker never stalls the
 * tick loop.
 *
 * One publisher per apiId, built per delivery: a `Publisher` in this client is
 * the (exchange, routingKey) pair a send is addressed with, not a resource, so
 * there is nothing to keep.
 *
 * Concurrency safety is the client's job, not this sink's: `@egress/rmq`
 * serializes every operation on the connection it owns, because creating
 * links concurrently silently misroutes in this library (see Client.ts's
 * module doc for both reproductions). This sink is free to fork a delivery
 * per event without thinking about it.
 */
export const makeAmqpControlPlaneSink: Effect.Effect<SinkImpl, RmqError, Rmq> = Effect.gen(
  function* () {
    const rmq = yield* Rmq;
    yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, CONTROL_EXCHANGE_OPTIONS);

    const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
    /** Consecutive attempts that failed, and when the last one did; reset on the next successful attempt. */
    const consecutiveFailures = yield* Ref.make({ count: 0, lastAt: 0 });

    /**
     * Deliveries are forked into this scope rather than the sink's own, so a
     * demotion can cut them loose without tearing the sink down: `resetConnection`
     * swaps in a fresh scope and closes this one, which interrupts whatever is
     * still retrying. See that function's comment for why the close is forked
     * rather than awaited.
     */
    const deliveryScope = yield* Ref.make(yield* Scope.make());

    /**
     * Whether the confirm-failure streak alone — not `isConnected` — is
     * currently active: CONSECUTIVE_FAILURE_THRESHOLD crossed within
     * FAILURE_STREAK_WINDOW_MS. Unlike `isConnected`, this is a decaying
     * signal that only a *success* can reset early, which is exactly why
     * `publish` below consults it only on a retry, never on a delivery's
     * first attempt: consulting it unconditionally would mean that once two
     * confirms failed, nothing could ever prove the connection had recovered
     * — every subsequent delivery's own first attempt would be short-circuited
     * by the same stale streak, for up to the full window, with no attempt
     * ever reaching the broker to reset it.
     */
    const failureStreakActive = Effect.all([Ref.get(consecutiveFailures), Clock.currentTimeMillis]).pipe(
      Effect.map(
        ([{ count, lastAt }, now]) => count >= CONSECUTIVE_FAILURE_THRESHOLD && now - lastAt < FAILURE_STREAK_WINDOW_MS,
      ),
    );

    /**
     * True once either signal already says this connection cannot be
     * trusted: `rmq.isConnected` (Client.ts's own tracking — driven by the
     * tuned AMQP protocol heartbeat among every other disconnect reason), or
     * `failureStreakActive`. `ready` reports this as-is. `publish` below
     * only consults the combined signal on a retry (see
     * `failureStreakActive`'s comment for why not on a first attempt) — but
     * consults `isConnected` alone on every attempt including the first,
     * since it carries no such risk: it flips back the instant a connection
     * actually recovers, not on a timer, so it can never strand a delivery
     * the way the decaying streak can. A retry already in flight cuts short
     * the moment either signal catches up to it, rather than running its
     * whole independent DELIVERY_RETRY chain regardless (~8.7s worst case)
     * — measured live, see docs/decisions/017's second amendment.
     */
    const knownBad = Effect.all([failureStreakActive, rmq.isConnected]).pipe(
      Effect.map(([streak, connected]) => streak || !connected),
    );

    const publish = (event: CircuitEvent, isRetry: boolean) =>
      (isRetry ? knownBad : Effect.map(rmq.isConnected, (connected) => !connected)).pipe(
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
                // Every attempt counts on its own, not only the delivery as a whole —
                // see CONSECUTIVE_FAILURE_THRESHOLD. This runs before DELIVERY_RETRY
                // decides whether to try again, so readiness can flip mid-delivery.
                Effect.tapError(() =>
                  Clock.currentTimeMillis.pipe(
                    Effect.flatMap((now) => Ref.update(consecutiveFailures, ({ count }) => ({ count: count + 1, lastAt: now }))),
                  ),
                ),
              ),
        ),
      );

    /**
     * One publish plus its confirm, retried per DELIVERY_RETRY — see
     * CONSECUTIVE_FAILURE_THRESHOLD's comment for the worst-case ~8.7s this
     * can take. `isRetry` starts false and flips after the first evaluation
     * (`Effect.suspend` re-reads it on every retry, same closure): `publish`
     * is only told "this is a retry" from the second attempt on — see its
     * own comment for why the first attempt of every delivery always gets a
     * real, unconditional try.
     */
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
     * Forked into the current leadership epoch's scope, same as before this
     * sink awaited its own callers, so a demotion's `resetConnection` can
     * still interrupt an in-flight publish (see `deliveryScope` above) — but
     * read back with `Fiber.await` rather than `Fiber.join`. `join` would
     * re-raise the fiber's own interruption into the caller, and the caller
     * is the tick loop: it must survive a demotion mid-delivery, not be
     * interrupted along with it. `await` always resolves with the `Exit`
     * instead — a confirm, an exhausted retry, or the scope-close interrupt —
     * so this effect always completes and the tick never hangs on it.
     *
     * A dead-lettered event now also fails the returned effect, not just this
     * sink's own bookkeeping: the caller (Aggregator's checkpoint loop) must
     * find out a publish never reached the broker, since that is exactly what
     * must stop its checkpoint from advancing.
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
      // Bounded for the same reason WebhookSink's is: a broker that stays
      // unreachable would otherwise grow this list for the life of the
      // process. The exact total lives in the metric.
      yield* Ref.update(dead, (xs) => [...xs, failure].slice(-DEAD_LETTER_BUFFER));
      return yield* Effect.fail(failure);
    });

    // Nothing durable behind this one: an event the control-plane exchange
    // could not take is dead-lettered and counted, not replayed. The daemons
    // re-learn the real state from the next snapshot, which is what
    // `snapshotMs` is for — a queue of stale transitions helps nobody.
    const ready = Effect.map(knownBad, (bad) => !bad);

    /**
     * Fence off this instance's outstanding publishes on step-down or
     * demotion (see `Aggregator.ts`'s `demoteAndFence`, the only caller).
     *
     * The scope swap stops any in-flight delivery from retrying its way onto
     * a freshly reconnected socket after this instance is no longer
     * authoritative — interrupting the fiber, not merely abandoning it, so a
     * retry already in flight cannot complete moments later and publish a
     * sequence a new leader has since moved past. Closing is forked rather
     * than awaited: a fiber parked on a promise the dead socket will never
     * settle only unblocks once `rmq.resetConnection`'s destroy actually
     * fires below, and a demotion must not wait on that round trip.
     *
     * `rmq.resetConnection` is what fences the broker itself — see its own
     * comment for the mechanism.
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
