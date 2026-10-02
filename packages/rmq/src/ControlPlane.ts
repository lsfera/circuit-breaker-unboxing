/**
 * Naming conventions for the work-queue shape shared by the producer and the competing-consumer fleet that
 * drains it: a durable work queue with a dead-letter destination, the broker's own delivery-limit budget, and
 * the work message's format.
 */

import { Option as O, Schema } from "effect";

/**
 * What a work message says, declared once so the producer's encoder and the daemons' decoder cannot drift.
 * `n` must be an integer: it keeps the idempotency key stable across a redelivery. Decoded to an `Option`,
 * not thrown: a body that is not a work message is an answer ("discard it"). Unknown fields are ignored, so a
 * newer producer can add one without breaking an older daemon.
 */
const WorkMessage = Schema.Struct({ apiId: Schema.String, n: Schema.Int });

/**
 * The AMQP `content_type` a work publisher declares and a daemon negotiates on before decoding. RabbitMQ
 * neither validates nor uses it and AMQP has no `Accept`, so negotiation is the reader's: a daemon reads
 * `application/json` (parameters ignored), unencoded, and a message that declares nothing (older or raw
 * publishers). A message declaring *something else* is one this fleet did not publish and is discarded to the
 * dead-letter queue unread, not guessed at.
 */
export const WORK_CONTENT_TYPE = "application/json";

/** The AMQP `type` of a work message, dot-separated by RabbitMQ's convention. A daemon declines any other type. */
export const WORK_MESSAGE_TYPE = "egress.work";

const mediaType = (contentType: string): string => contentType.split(";")[0]!.trim().toLowerCase();

/** `content_encoding` may list several, comma-separated; only "nothing applied" is readable. */
const unencoded = (contentEncoding: string): boolean =>
  contentEncoding.split(",").every((encoding) => ["", "identity"].includes(encoding.trim().toLowerCase()));

export const readsWorkFormat = (declared: {
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  readonly type: O.Option<string>;
}): boolean =>
  O.match(declared.contentType, { onNone: () => true, onSome: (t) => mediaType(t) === WORK_CONTENT_TYPE }) &&
  O.match(declared.contentEncoding, { onNone: () => true, onSome: unencoded }) &&
  O.match(declared.type, { onNone: () => true, onSome: (t) => t === WORK_MESSAGE_TYPE });

const WorkMessageJson = Schema.fromJsonString(WorkMessage);
export const encodeWorkMessage = Schema.encodeSync(WorkMessageJson);
export const decodeWorkMessage = Schema.decodeUnknownOption(WorkMessageJson);

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

/** Where work that could not be completed ends up. */
export const deadLetterQueueFor = (apiId: string): string => `${apiId}.work.dead`;

/**
 * One dead-letter destination for every queue this fleet declares, so anything unhandleable lands somewhere
 * you can count and replay from. Must be declared identically by every process that touches a queue:
 * RabbitMQ rejects a redeclare whose arguments differ, and container startup is unordered.
 */
const deadLetterArgs = (apiId: string): Record<string, unknown> => ({
  "x-dead-letter-exchange": "",
  "x-dead-letter-routing-key": deadLetterQueueFor(apiId),
});

/** Identical to `deadLetterArgs` today; named separately because here it is designed behaviour, not a backstop. */
const workQueueArgs = deadLetterArgs;

/**
 * Attempts before the broker parks a message. The budget belongs to the queue, not the daemon: an in-process
 * counter is lost when the message moves to another consumer, which is what an outage causes. Low because
 * RabbitMQ redelivers with no backoff, so every extra attempt is load on a failing upstream.
 */
export const WORK_DELIVERY_LIMIT = 3;

/**
 * Durability is decided here so producer and daemons cannot disagree: a mismatch is a redeclare conflict
 * (`409 inequivalent arg 'durable'`), so changing a flag means deleting the queue first. Always durable:
 * RabbitMQ 4.3 refuses a transient queue that is not exclusive by closing the whole connection (541).
 */
export const workQueueOptions = (apiId: string) => ({
  args: {
    ...workQueueArgs(apiId),
    "x-queue-type": "quorum",
    "x-delivery-limit": WORK_DELIVERY_LIMIT,
    // At-least-once: the default (at-most-once) drops a dead letter the target queue does not take. Quorum
    // queues require reject-publish for it.
    "x-dead-letter-strategy": "at-least-once",
    "x-overflow": "reject-publish",
  },
  durable: true,
});

/**
 * The end of the line, so nothing may leave it except by being moved. `x-delivery-limit: -1`: a quorum queue
 * left alone has a limit of 20, and a queue with no dead-letter target at its limit *drops* the message.
 */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

