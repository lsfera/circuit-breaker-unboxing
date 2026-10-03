import { Effect, Match, Metric, Option as O } from "effect";
import { setTimeout as sleep } from "node:timers/promises";
import type { HttpClient } from "effect/http";
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
 * One competing-consumer daemon with its own in-process circuit breaker (Breaker.ts). Nothing coordinates the
 * daemons: each breaker's state is private to this process and formed only from its own calls.
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

/**
 * Whether a call outcome is accepted, handed back to the broker or dead-lettered: a total function of the one
 * thing that matters, so it is testable without a broker, breaker or fetch. `"open"` and `"failed"` both
 * requeue; they differ in whether a call was attempted, which is telemetry, not settlement. `"client_error"`
 * skips the delivery budget and goes straight to the dead-letter queue: a retry gets the same answer.
 */
export type CallOutcome = Breaker.CallOutcome | "open";
export const decide = (outcome: CallOutcome): Settlement =>
  Match.value(outcome).pipe(
    Match.when("ok", (): Settlement => "accept"),
    Match.when("client_error", (): Settlement => "discard"),
    Match.whenOr("failed", "open", (): Settlement => "requeue"),
    Match.exhaustive,
  );

/** What one attempt came to. `status` is `none` when the breaker turned the call away and none was made. */
type Attempt = { readonly outcome: CallOutcome; readonly status: string };
const TURNED_AWAY: Attempt = { outcome: "open", status: "none" };

/**
 * Held before handing a breaker-open rejection back to the broker. Without it a message rejected instantly by
 * an open breaker goes straight back to the queue and to this same consumer, spinning against its own
 * in-memory breaker at whatever rate the broker redelivers and hammering the broker instead of the third
 * party. Jittered so breakers that open together do not requeue in lockstep.
 */
const OPEN_REQUEUE_DELAY_MIN_MS = 100;
const OPEN_REQUEUE_DELAY_SPREAD_MS = 300;

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handler below (amqplib's callback, not an Effect fiber) can still update
  // metrics through this process's services.
  const services = yield* Effect.context<HttpClient.HttpClient>();
  const runInContext = Effect.runPromiseWith(services);

  // One breaker per process, shared across every message: a fresh one per call would never accumulate a failure count.
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

  const callUpstream = (key: string): Promise<Upstream.CallStatus> =>
    runInContext(
      setInFlight(1).pipe(
        Effect.andThen(Upstream.call(`${cfg.egressAddr}${cfg.apiPath}`, key)),
        Effect.ensuring(setInFlight(-1)),
      ),
    );

  // Logged at most once a second: a publisher that starts sending `gzip` by mistake, or a third party that
  // starts refusing every request, would otherwise empty the queue into the dead-letter queue without a trace.
  // The counters carry the volume.
  let lastLoggedAt = 0;
  const warnAtMostOncePerSecond = (message: () => string): void => {
    O.map(
      O.liftPredicate(Date.now(), (now) => now - lastLoggedAt >= 1000),
      (now) => {
        lastLoggedAt = now;
        return runInContext(Effect.logWarning(message()));
      },
    );
  };

  const attempt = async (key: string): Promise<Settlement> => {
    const { outcome, status } = await breaker.execute(() => callUpstream(key)).then(
      (answer): Attempt => ({ outcome: Breaker.classify(answer), status: String(answer) }),
      // Breaker.isBrokenCircuitError: rejected locally, no call attempted —
      // this replica's own breaker is open. Anything else threw, which the
      // breaker counts as a failure and so does this.
      (err): Attempt => (Breaker.isBrokenCircuitError(err) ? TURNED_AWAY : { outcome: "failed", status: "error" }),
    );

    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome, status }), 1));
    await Match.value(outcome).pipe(
      Match.when("open", () => sleep(OPEN_REQUEUE_DELAY_MIN_MS + Math.random() * OPEN_REQUEUE_DELAY_SPREAD_MS)),
      Match.when("client_error", () =>
        warnAtMostOncePerSecond(() => `${cfg.apiId}/consumer: third party refused message_id ${key} with ${status}, dead-lettering it`),
      ),
      Match.orElse(() => Promise.resolve()),
    );
    return decide(outcome);
  };

  // A body that declares a content type, encoding or message type this daemon cannot read, does not decode, or
  // carries no `message_id` to use as its idempotency key was never published by this fleet: discard it rather
  // than spend the delivery budget on something no retry can fix.
  const discard = (reason: "format" | "malformed" | "keyless", delivery: DeliveryInfo): Promise<Settlement> => {
    runInContext(Metric.update(Metric.withAttributes(Telemetry.discarded, { reason }), 1));
    warnAtMostOncePerSecond(() => {
      const declared = (o: O.Option<string>) => O.getOrElse(o, () => "none");
      return (
        `${cfg.apiId}/consumer: discarding a ${reason} delivery — message_id ${declared(delivery.messageId)}, ` +
        `type ${declared(delivery.type)}, content-type ${declared(delivery.contentType)}, ` +
        `content-encoding ${declared(delivery.contentEncoding)}`
      );
    });
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
