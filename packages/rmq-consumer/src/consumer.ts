import { Effect, Metric, Option as O } from "effect";
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
import * as Telemetry from "./Telemetry.ts";
import * as Upstream from "./Upstream.ts";
import type { CallOutcome } from "./Upstream.ts";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * The base scenario: one competing-consumer daemon with no circuit awareness. It knows nothing about the other
 * daemons or whether the third party is degrading, only whether its own last call succeeded. A failed call is
 * handed back with `requeue`, and the broker's `x-delivery-limit` (see `workQueueOptions`) eventually dead-letters it.
 */

export type ConsumerConfig = {
  readonly apiId: string;
  /** The one address a real client would be given — no replica names, no LB it can see through. */
  readonly egressAddr: string;
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
};

/**
 * Whether a call outcome is accepted or handed back to the broker: a total function of the one thing that
 * matters, so it is testable without a broker or fetch.
 */
export const decide = (outcome: CallOutcome): Settlement =>
  outcome === "ok" ? "accept" : "requeue";

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

  let inFlight = 0;
  const setInFlight = (delta: 1 | -1) =>
    Effect.suspend(() => Metric.update(Telemetry.inFlight, (inFlight += delta)));

  const attempt = (key: string): Promise<Settlement> =>
    runInContext(
      setInFlight(1).pipe(
        Effect.andThen(Upstream.call(`${cfg.egressAddr}${cfg.apiPath}`, key)),
        Effect.ensuring(setInFlight(-1)),
        Effect.tap((outcome) => Metric.update(Metric.withAttributes(Telemetry.calls, { outcome }), 1)),
        Effect.map(decide),
      ),
    );

  // A body that declares a content type, encoding or message type this daemon cannot read, does not decode, or
  // carries no `message_id` to use as its idempotency key was never published by this fleet: discard it rather
  // than spend the delivery budget on something no retry can fix. It is logged, at most once a second, because
  // a publisher that starts sending `gzip` by mistake would otherwise empty the queue into the dead-letter queue
  // without a trace; the counter carries the volume.
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

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue}`,
  );
});
