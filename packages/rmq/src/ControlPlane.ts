/**
 * Naming conventions for the work-queue shape shared by the producer and the competing-consumer fleet that
 * drains it: a durable work queue with a dead-letter destination, the broker's own delivery-limit budget,
 * the idempotency-key convention a caller and a third party agree on, and, as of article 3, the redrive-trigger
 * and parked-queue naming `Redrive.ts` and `consumer.ts` use to recover `<api>.work.dead`.
 */

import { Option as O, Schema } from "effect";

/** The payments idempotency key, as the third party receives it. On the broker it's the AMQP `message_id`. */
export const IDEMPOTENCY_KEY_HTTP_HEADER = "x-idempotency-key";

/** A work message's identity, stable for its life. `n` alone would collide across producer restarts. */
export const workMessageId = (run: string, n: number): string => `${run}:${n}`;

/** What a work message says, declared once so encoder and decoder can't drift. Unknown fields are ignored. */
export const WorkMessage = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
export type WorkMessage = typeof WorkMessage.Type;

/** The AMQP `content_type` a work publisher declares. AMQP has no `Accept`, so negotiation is the reader's. */
export const WORK_CONTENT_TYPE = "application/json";

/** The AMQP `type` of a work message, dot-separated by RabbitMQ's convention. A daemon declines any other type. */
export const WORK_MESSAGE_TYPE = "egress.work";

const mediaType = (contentType: string): string => contentType.split(";")[0]!.trim().toLowerCase();

/** `content_encoding` may list several, comma-separated; only "nothing applied" is readable. */
const unencoded = (contentEncoding: string): boolean =>
  contentEncoding.split(",").every((encoding) => ["", "identity"].includes(encoding.trim().toLowerCase()));

/** What a delivery declared about itself; `None` wherever the publisher said nothing, which is read as a match. */
type Declared = {
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  readonly type: O.Option<string>;
};

const readsFormat =
  (contentType: string, messageType: string) =>
  (declared: Declared): boolean =>
    O.match(declared.contentType, { onNone: () => true, onSome: (t) => mediaType(t) === contentType }) &&
    O.match(declared.contentEncoding, { onNone: () => true, onSome: unencoded }) &&
    O.match(declared.type, { onNone: () => true, onSome: (t) => t === messageType });

export const readsWorkFormat = readsFormat(WORK_CONTENT_TYPE, WORK_MESSAGE_TYPE);

const WorkMessageJson = Schema.fromJsonString(WorkMessage);
export const encodeWorkMessage = Schema.encodeSync(WorkMessageJson);
export const decodeWorkMessage = Schema.decodeUnknownOption(WorkMessageJson);

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

/** Where work that could not be completed ends up. */
export const deadLetterQueueFor = (apiId: string): string => `${apiId}.work.dead`;

/** Must be declared identically by every process: RabbitMQ rejects a redeclare whose arguments differ. */
const deadLetterArgs = (apiId: string): Record<string, unknown> => ({
  "x-dead-letter-exchange": "",
  "x-dead-letter-routing-key": deadLetterQueueFor(apiId),
});

/** Identical to `deadLetterArgs` today; named separately because here it is designed behaviour, not a backstop. */
const workQueueArgs = deadLetterArgs;

/**
 * Article 3's redrive election. `x-single-active-consumer` means the broker delivers to one bound consumer
 * and holds the rest as backups, promoting automatically if the active one disconnects — no leader-election
 * code of this project's own. Nothing is ever published here but the trigger itself (see `consumer.ts`).
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

export const redriveTriggerQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-single-active-consumer": true },
  durable: true,
});

/**
 * Where a message goes once it has been redriven `MAX_REDRIVES` times without succeeding — treated as poison
 * rather than unlucky. Terminal like the dead-letter queue, and for the same reason never delivery-limited.
 */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/**
 * Stamped on a redriven message so the next pass can tell a message caught in its second outage from one
 * that has failed every time. Absent means zero — dead-lettered by the broker directly, never yet redriven.
 */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/**
 * Redrives before a message is treated as poison rather than unlucky. Each redrive republishes onto the work
 * queue, granting a fresh `WORK_DELIVERY_LIMIT`-attempt budget — so this bounds outages survived, not attempts.
 */
export const MAX_REDRIVES = 5;

/**
 * Attempts before the broker parks a message. Belongs to the queue, not the daemon — an in-process counter is
 * lost the moment a message moves to another consumer, which is what an outage causes.
 */
export const WORK_DELIVERY_LIMIT = 3;

/** Always durable: RabbitMQ 4.3 closes the whole connection (541) on a transient queue that isn't exclusive. */
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

/** `x-delivery-limit: -1`: a quorum queue's default limit (20) would silently drop a message at its cap. */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});
