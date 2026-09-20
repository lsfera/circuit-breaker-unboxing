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
 * The base scenario: one competing-consumer daemon, no circuit awareness at
 * all. It knows nothing about the other daemons in its own fleet and nothing
 * about whether the third party is degrading — only whether *its own* last
 * call succeeded. A failed call is handed back to the broker with `requeue`,
 * and the broker's own `x-delivery-limit` (see `workQueueOptions`) is what
 * eventually dead-letters it. Nothing here backs off, coordinates, or stops.
 * That absence is the point: it is what the rest of this article series is
 * about adding, one piece at a time.
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
 * Whether a call outcome should be accepted or handed back to the broker.
 * Pulled out as a total function of the one thing that matters — pure,
 * exhaustively testable, no broker or fetch involved.
 */
export const decide = (outcome: CallOutcome): Settlement =>
  outcome === "ok" ? "accept" : "requeue";

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

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue}`,
  );
});
