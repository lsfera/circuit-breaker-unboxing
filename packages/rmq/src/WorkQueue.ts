/**
 * The work-queue shape a producer and a competing-consumer fleet share: a
 * durable work queue with a dead-letter destination, the broker's own
 * delivery-limit budget, and the idempotency-key convention a caller and a
 * fake third party can agree on.
 */

import { Option as O, Schema } from "effect";

/**
 * The payments idempotency key, as the third party receives it: an HTTP header
 * on the call it authorizes. A retry that reuses the same key protects the
 * third party from being charged twice for one logical attempt.
 *
 * On the broker it is the AMQP `message_id`, not a header of our own: the
 * standard property for "which message is this", visible to RabbitMQ's own
 * tooling and kept when a message is dead-lettered. The publisher assigns it
 * once (`workMessageId`) and a republish must carry it explicitly, or the
 * replay is a new message with a new key.
 */
export const IDEMPOTENCY_KEY_HTTP_HEADER = "x-idempotency-key";

/**
 * A work message's identity: unique to the producer run that made it, and
 * stable for the life of the message. `n` alone restarts at zero with every
 * process, so a restarted producer would reuse the key of different work and a
 * third party would drop it as a duplicate. The last `:` splits run from
 * sequence, which is what the fake third party's audit reads.
 */
export const workMessageId = (run: string, n: number): string => `${run}:${n}`;

/**
 * What a work message says, declared once so the producer's encoder and the
 * daemons' decoder cannot drift apart. `n` is what makes the idempotency key
 * stable across a broker redelivery of the same message, so it has to be an
 * integer: anything else is not a message this fleet published.
 *
 * The wire form is JSON text, decoded to an `Option` rather than thrown: a body
 * that is not a work message is an answer ("discard it"), not an exception.
 * Fields beyond these are ignored, so a newer producer can add one without
 * breaking an older daemon.
 */
export const WorkMessage = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
export type WorkMessage = typeof WorkMessage.Type;

/**
 * What a publisher of work messages declares as its AMQP `content_type`, and
 * so what a daemon negotiates on before it decodes.
 *
 * RabbitMQ neither validates nor uses `content_type` and `content_encoding`
 * (https://www.rabbitmq.com/docs/consumers#content-type-and-encoding): the
 * publisher sets them, the consumer is expected to respect them, and nothing
 * between them checks. AMQP has no `Accept`, so negotiation here is the
 * reader's: a daemon reads `application/json` (parameters such as `charset`
 * ignored), unencoded (no `content_encoding`, or `identity`), and a message that
 * declares nothing at all, because publishers that predate the declaration, and
 * anything publishing raw, say nothing. A message that declares *something
 * else* (another type, or `gzip` that nothing here inflates) is one this fleet
 * did not publish and cannot read, and is discarded to the dead-letter queue
 * unread rather than guessed at.
 */
export const WORK_CONTENT_TYPE = "application/json";

/**
 * The AMQP `type` of a work message: what kind of message it is, dot-separated
 * by RabbitMQ's own convention. A daemon that receives another type has been
 * sent something it does not handle, which the publisher-side guidance says to
 * log; here it is also declined.
 */
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
 * One dead-letter destination for *every* queue this fleet declares, so anything
 * unhandleable lands somewhere you can count and replay from.
 *
 * Must be declared identically by every process that touches a queue: RabbitMQ
 * rejects a redeclare whose arguments differ, and container startup is unordered.
 */
const deadLetterArgs = (apiId: string): Record<string, unknown> => ({
  "x-dead-letter-exchange": "",
  "x-dead-letter-routing-key": deadLetterQueueFor(apiId),
});

/** Identical to `deadLetterArgs` today; named separately because here it is designed behaviour, not a backstop. */
const workQueueArgs = deadLetterArgs;

/**
 * Attempts before the broker parks a message. The budget belongs to the queue,
 * not the daemon: an in-process counter is lost the moment the message moves to
 * another consumer, which is what an outage causes.
 *
 * Low because RabbitMQ redelivers with no backoff, so every extra attempt is
 * load on a failing upstream. A redrive republishes the body, so a replayed
 * message starts a fresh budget — three attempts per outage, not three ever.
 */
export const WORK_DELIVERY_LIMIT = 3;

/**
 * Durability, decided here so producer and daemons cannot disagree — a mismatch
 * is a redeclare conflict (`409 inequivalent arg 'durable'`), so changing a flag
 * on a broker that already holds the queue means deleting it first.
 *
 * Every queue is durable. RabbitMQ 4.3 refuses a transient queue that is not
 * exclusive, and refuses it by closing the whole connection (541), so one such
 * declare takes the daemon down. The control and floor queues cannot be
 * exclusive — the floor is shared by the fleet — so they are durable classic
 * queues whose `x-expires` does the cleanup transience used to. Everything else
 * is a quorum queue, which could never be transient anyway.
 */
export const workQueueOptions = (apiId: string) => ({
  args: {
    ...workQueueArgs(apiId),
    "x-queue-type": "quorum",
    "x-delivery-limit": WORK_DELIVERY_LIMIT,
    // At-least-once: the default (at-most-once) drops a dead letter the target
    // queue does not take. Not what lost the 1,570 — see deadLetterQueueOptions.
    // Quorum queues require reject-publish for it.
    "x-dead-letter-strategy": "at-least-once",
    "x-overflow": "reject-publish",
  },
  durable: true,
});

/**
 * The end of the line, so nothing may ever leave it except by being moved.
 *
 * `x-delivery-limit: -1`, because a quorum queue left alone has a limit of 20, and
 * a queue with no dead-letter target at its limit *drops* the message
 * (`dead_letter_strategy="disabled"`). Every redrive pass hands back what it did not
 * move — its channel closing counts — so the old default quietly lost dead letters:
 * 1,570 in one chaos run, and 0 of 50 survived 22 channel closes on this broker
 * where -1 kept all 50 through 25.
 */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

