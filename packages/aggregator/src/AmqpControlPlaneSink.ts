import { Clock, Duration, Effect, Exit, Fiber, Ref, Schedule, Scope } from "effect";
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
 * The heartbeat below tests the same connection on its own clock, so
 * readiness stops being gated on the breaker having something to say. Each
 * one is forked, not awaited, so the schedule keeps firing on its own fixed
 * cadence while an earlier heartbeat is still counting down its own 2s —
 * without that, a single stuck attempt would push every later one back by
 * its own timeout too, and this would be no faster than the transition path
 * it replaces. Overlapping instead, two consecutive failures land one
 * `PROBE_INTERVAL` apart once the first one fails, not one
 * `PUBLISH_CONFIRM_TIMEOUT` apart.
 *
 * Worst case is a fault landing just after a heartbeat has already gone out:
 * up to one full `PROBE_INTERVAL` before the next one fires, `+2s` for it to
 * time out, `+1s` more for the one after it to do the same and cross
 * CONSECUTIVE_FAILURE_THRESHOLD — `2×PROBE_INTERVAL + PUBLISH_CONFIRM_TIMEOUT`,
 * ≈4s. Comfortably inside the 5000ms lease TTL with margin, independent of
 * whatever the breaker itself is doing.
 */
const PROBE_INTERVAL = Duration.seconds(1);

/**
 * `routingKeyFor` always returns `circuit.<apiId>`, and every daemon binds
 * only its own exact key — so a key with no `circuit.` prefix at all can
 * never collide with a real API's binding, present or future. An unroutable
 * message on a topic exchange is simply dropped once confirmed; nothing
 * downstream ever sees this.
 */
export const HEARTBEAT_ROUTING_KEY = "__heartbeat__";

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
export type AmqpSinkImpl = SinkImpl & {
  /**
   * The heartbeat loop — see PROBE_INTERVAL's comment. Never completes on its
   * own, so the caller forks it (main.ts, alongside `rmq.lost`'s own fork)
   * into whatever scope should own its lifetime; this module does not fork
   * it itself so that constructing a sink for a test is inert by default —
   * only a test that wants the heartbeat's own behavior forks `.probe`.
   */
  readonly probe: Effect.Effect<unknown>;
};

export const makeAmqpControlPlaneSink: Effect.Effect<AmqpSinkImpl, RmqError, Rmq> = Effect.gen(
  function* () {
    const rmq = yield* Rmq;
    yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, CONTROL_EXCHANGE_OPTIONS);

    const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
    /** Consecutive attempts that failed, and when the last one did; reset on the next successful attempt. */
    const consecutiveFailures = yield* Ref.make({ count: 0, lastAt: 0 });

    /**
     * A publish-and-confirm with nothing behind it, counted into the same
     * `consecutiveFailures` a real delivery would be — see PROBE_INTERVAL's
     * comment for why. `Effect.ignore` at the end: a heartbeat's outcome
     * lives entirely in the Ref side effect, and nothing calling this needs
     * its `Exit`.
     */
    const heartbeat = rmq.publisherToExchange(CONTROL_EXCHANGE, HEARTBEAT_ROUTING_KEY).pipe(
      // Its own channel — see `send`'s own doc comment for why: sharing the
      // default one with real event delivery is what produced a genuine
      // duplicate on `circuit.control` under load, confirmed live.
      Effect.flatMap((pub) => rmq.send(pub, "", undefined, HEARTBEAT_ROUTING_KEY)),
      Effect.mapError(
        (e: RmqError) =>
          new DeliveryFailed({ sink: "amqp", apiId: HEARTBEAT_ROUTING_KEY, cause: String(e.cause) }),
      ),
      Effect.timeoutOrElse({
        duration: PUBLISH_CONFIRM_TIMEOUT,
        orElse: () =>
          Effect.fail(
            new DeliveryFailed({
              sink: "amqp",
              apiId: HEARTBEAT_ROUTING_KEY,
              cause: "no publish confirm within 2s",
            }),
          ),
      }),
      Effect.tapError(() =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => Ref.update(consecutiveFailures, ({ count }) => ({ count: count + 1, lastAt: now }))),
        ),
      ),
      Effect.tap(() => Ref.set(consecutiveFailures, { count: 0, lastAt: 0 })),
      Effect.ignore,
    );

    // Forked per attempt rather than awaited by the loop that fires them, so
    // a heartbeat still counting down its own PUBLISH_CONFIRM_TIMEOUT never
    // delays the next one — see PROBE_INTERVAL's comment for the arithmetic
    // this overlap buys. This loop itself is returned as `probe`, not forked
    // here: standbys gate even trying to acquire the lease on `sinkReady`
    // too (`attemptTick`), so it has to keep testing the connection across
    // every promotion and demotion, not just one leadership epoch — the
    // caller forks it once, outside any epoch-scoped lifetime.
    const probe = Effect.repeat(Effect.forkChild(heartbeat), Schedule.spaced(PROBE_INTERVAL));

    /**
     * Deliveries are forked into this scope rather than the sink's own, so a
     * demotion can cut them loose without tearing the sink down: `resetConnection`
     * swaps in a fresh scope and closes this one, which interrupts whatever is
     * still retrying. See that function's comment for why the close is forked
     * rather than awaited.
     */
    const deliveryScope = yield* Ref.make(yield* Scope.make());

    const publish = (event: CircuitEvent) =>
      rmq.publisherToExchange(CONTROL_EXCHANGE, routingKeyFor(event.data.apiId)).pipe(
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
      );

    /** One publish plus its confirm, retried per DELIVERY_RETRY — see CONSECUTIVE_FAILURE_THRESHOLD's comment for the worst-case ~8.7s this can take. */
    const attempt = (event: CircuitEvent) =>
      publish(event).pipe(
        Effect.retry(DELIVERY_RETRY),
        Effect.tap(() => Ref.set(consecutiveFailures, { count: 0, lastAt: 0 })),
      );

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
    const deliver = (event: CircuitEvent) =>
      Effect.gen(function* () {
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
    const ready = Effect.all([rmq.isConnected, Ref.get(consecutiveFailures), Clock.currentTimeMillis]).pipe(
      Effect.map(
        ([connected, { count, lastAt }, now]) =>
          connected && (count < CONSECUTIVE_FAILURE_THRESHOLD || now - lastAt >= FAILURE_STREAK_WINDOW_MS),
      ),
    );

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
      probe,
    };
  },
);
