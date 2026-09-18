import { Data, Effect, Match, Metric, Option as O } from "effect";
import { setTimeout as sleep } from "node:timers/promises";
import type { HttpClient } from "effect/unstable/http";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  decodeWorkMessage,
  readsWorkFormat,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import * as Breaker from "./Breaker.ts";
import * as Telemetry from "./Telemetry.ts";
import * as Upstream from "./Upstream.ts";
import type { CircuitState } from "cockatiel";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * One competing-consumer daemon, now with an in-process circuit breaker
 * (see Breaker.ts) — and still nothing else. It knows nothing about the
 * other daemons in its own fleet: its breaker's state is private to this
 * process, formed from only the calls this process itself has made. Five
 * replicas means five independent breakers that will open and close at
 * different times, for the same incident. That absence — no shared verdict
 * — is what the next branch in this series adds.
 */

export type ConsumerConfig = {
  readonly apiId: string;
  /** The one address a real client would be given — no replica names, no LB it can see through. */
  readonly egressAddr: string;
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
  readonly breaker: Breaker.BreakerConfig;
};

/** A failed call is thrown, not returned — cockatiel's `handleAll` policy classifies by thrown errors. */
class UpstreamCallFailed extends Data.TaggedError("UpstreamCallFailed")<{}> {}

/**
 * Whether a call outcome should be accepted or handed back to the broker.
 * Pulled out as a total function of the one thing that matters — pure,
 * exhaustively testable, no broker, breaker, or fetch involved.
 *
 * `"open"` and `"failed"` both requeue: the difference between them is
 * whether a call was actually attempted, which is a telemetry fact, not a
 * settlement fact. What differs operationally is upstream of this function
 * — see `OPEN_REQUEUE_DELAY_*` in `attempt` below.
 */
export type CallOutcome = Upstream.CallOutcome | "open";
export const decide = (outcome: CallOutcome): Settlement =>
  outcome === "ok" ? "accept" : "requeue";

/**
 * Held before releasing a breaker-open rejection back to the broker. Without
 * this, a message rejected instantly by an open local breaker (no call made,
 * no wait) goes straight back onto the queue and straight back to this same
 * consumer, which can spin against its own in-memory breaker at whatever
 * rate the broker will redeliver — hammering the *broker* even though the
 * third party is no longer being hammered. Same constants, same reasoning as
 * the shed-`429` hold the article series' predecessor daemon used before it
 * was removed: jittered so a fleet whose breakers open together doesn't
 * requeue in lockstep either.
 */
const OPEN_REQUEUE_DELAY_MIN_MS = 100;
const OPEN_REQUEUE_DELAY_SPREAD_MS = 300;

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handler below (amqplib's own callback, not an
  // Effect fiber) can still update metrics through this process's services —
  // see rmq-consumer/src/daemon.ts's identical comment on why the bare
  // `Effect.run*` entry points are wrong here.
  const services = yield* Effect.context<HttpClient.HttpClient>();
  const runInContext = Effect.runPromiseWith(services);

  // One breaker for the process's whole life, shared across every message —
  // see Breaker.ts for why that sharing is load-bearing, not incidental.
  const breaker = Breaker.make(cfg.breaker);

  breaker.onStateChange((state: CircuitState) =>
    runInContext(Metric.update(Telemetry.breakerState, Breaker.STATE_CODE[state])),
  );
  breaker.onBreak(() => {
    runInContext(Metric.update(Telemetry.breakerTrips, 1));
    runInContext(Effect.log(`${cfg.apiId}/consumer: breaker opened`));
  });
  breaker.onReset(() => runInContext(Effect.log(`${cfg.apiId}/consumer: breaker closed`)));

  let inFlight = 0;
  const setInFlight = (delta: 1 | -1) =>
    Effect.suspend(() => Metric.update(Telemetry.inFlight, (inFlight += delta)));

  const callUpstream = (key: string): Promise<void> =>
    runInContext(
      setInFlight(1).pipe(
        Effect.andThen(Upstream.call(`${cfg.egressAddr}${cfg.apiPath}`, key)),
        Effect.ensuring(setInFlight(-1)),
        Effect.filterOrFail(
          (outcome) => outcome === "ok",
          () => new UpstreamCallFailed(),
        ),
        Effect.asVoid,
      ),
    );

  const attempt = async (key: string): Promise<Settlement> => {
    const outcome: CallOutcome = await breaker.execute(() => callUpstream(key)).then(
      (): CallOutcome => "ok",
      // Breaker.isBrokenCircuitError: rejected locally, no call attempted —
      // this replica's own breaker is open. Anything else is a real call
      // that failed (timeout, connection refused, a non-2xx).
      (err): CallOutcome => (Breaker.isBrokenCircuitError(err) ? "open" : "failed"),
    );

    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome }), 1));
    await Match.value(outcome).pipe(
      Match.when("open", () => sleep(OPEN_REQUEUE_DELAY_MIN_MS + Math.random() * OPEN_REQUEUE_DELAY_SPREAD_MS)),
      Match.orElse(() => Promise.resolve()),
    );
    return decide(outcome);
  };

  // A body that declares a content type, encoding or message type this daemon
  // cannot read, does not decode, or carries no `message_id` to use as its
  // idempotency key, was never published by this fleet: discard
  // it rather than spend the delivery budget on something no retry can fix.
  //
  // Said out loud, because RabbitMQ's own guidance for a consumer handed a
  // delivery it cannot handle is to log it, and a publisher that starts sending
  // `gzip` by mistake would otherwise empty the queue into the dead-letter queue
  // without a trace. The counter carries the volume; the log carries what was
  // declared, at most once a second so a flood does not become the incident.
  let lastLoggedAt = 0;
  const discard = (reason: "format" | "malformed" | "keyless", delivery: DeliveryInfo): Promise<Settlement> => {
    runInContext(Metric.update(Metric.withAttributes(Telemetry.discarded, { reason }), 1));
    O.map(
      O.liftPredicate(Date.now(), (now) => now - lastLoggedAt >= 1000),
      (now) => {
        lastLoggedAt = now;
        const declared = (o: O.Option<string>) => O.getOrElse(o, () => "none");
        return runInContext(
          Effect.logWarning(
            `${cfg.apiId}/consumer: discarding a ${reason} delivery — message_id ${declared(delivery.messageId)}, ` +
              `type ${declared(delivery.type)}, content-type ${declared(delivery.contentType)}, ` +
              `content-encoding ${declared(delivery.contentEncoding)}`,
          ),
        );
      },
    );
    return Promise.resolve<Settlement>("discard");
  };

  const call = (body: string, delivery: DeliveryInfo): Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("malformed", delivery),
          // The key is the message's own `message_id`, assigned once by the
          // producer: no id means no safe retry, so no call.
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("keyless", delivery),
              onSome: attempt,
            }),
        })
      : discard("format", delivery);

  yield* rmq.consume(workQueue, (body, delivery) => call(body, delivery), {
    prefetch: cfg.maxInFlight,
  });

  // Set at startup so the series exists before the first state change.
  yield* Metric.update(Telemetry.breakerState, Breaker.INITIAL_STATE_CODE);

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} ` +
      `breaker=${cfg.breaker.consecutiveFailures}consecutive/${cfg.breaker.initialDelayMs}-${cfg.breaker.maxDelayMs}ms`,
  );
});
