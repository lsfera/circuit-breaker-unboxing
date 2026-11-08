import { Effect } from "effect";
import { Rmq } from "./Client.ts";
import type { RmqError, SendOptions } from "./Client.ts";
import { MAX_DELAY_SECONDS } from "./DelayedDelivery.ts";
import { rabbitMqVersionAtLeast } from "./RabbitMqVersion.ts";

/**
 * Delivery held by the queue itself. From RabbitMQ 4.4 a quorum queue holds a message until the time in its
 * `x-opt-delivery-time` header (epoch milliseconds, absolute) and only then makes it deliverable: no chain of
 * TTL queues, no exchange bindings, and nothing to declare beyond the destination queue. The same promise as
 * `DelayedDelivery.ts` (confirmed once the broker holds it, durable across a restart) for the same call shape,
 * so a caller picks one by `supportedBy(rmq.brokerVersion)`.
 *
 * The time is the publisher's clock read against the broker's: skew between the two moves every delay by that much,
 * where the chain counted its delay on the broker alone.
 */

/** The first release with delayed messages on a quorum queue. */
export const MINIMUM_RABBITMQ_VERSION = "4.4.0";

/** The header RabbitMQ reads a message's delivery time from. */
export const DELIVERY_TIME_HEADER = "x-opt-delivery-time";

export const supportedBy = (brokerVersion: string): boolean =>
  rabbitMqVersionAtLeast(brokerVersion, MINIMUM_RABBITMQ_VERSION);

/** Whole seconds, at least 1, at most what `DelayedDelivery` counts to: one ceiling for a hold on either mechanism. */
const clampSeconds = (seconds: number): number => Math.min(MAX_DELAY_SECONDS, Math.max(1, Math.ceil(seconds)));

/**
 * A queue that can hold delayed messages: a quorum queue, the only kind that does. Idempotent and safe to declare from
 * every process, as long as each passes the same `args`.
 */
export const declare = Effect.fnUntraced(function*(queue: string, args: Record<string, unknown> = {}) {
  const rmq = yield* Rmq;
  return yield* rmq.declareQueue(queue, { args: { ...args, "x-queue-type": "quorum" } });
});

/**
 * Deliver `body` to `queue` in `seconds`. Published straight to the queue (the default exchange), so nothing needs
 * binding. Confirmed once the queue holds it, which is when the delay is a promise.
 */
export const sendDelayed = Effect.fnUntraced(function*(
  queue: string,
  seconds: number,
  body: string,
  options?: SendOptions
): Effect.fn.Return<void, RmqError, Rmq> {
  const rmq = yield* Rmq;
  const publisher = yield* rmq.publisherToQueue(queue);
  yield* rmq.send(publisher, body, {
    ...options,
    headers: { ...options?.headers, [DELIVERY_TIME_HEADER]: Date.now() + clampSeconds(seconds) * 1000 }
  });
});
