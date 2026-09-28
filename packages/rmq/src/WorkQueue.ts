/**
 * The work-queue shape a producer and a competing-consumer fleet share: a durable work queue with a dead-letter
 * destination, the broker's own delivery-limit budget, and the message identity that serves as idempotency key.
 */

import { Schema } from "effect";

/**
 * A work message's identity, its AMQP `message_id` and the idempotency key the application hands the third party.
 * A republish must carry it explicitly, or the replay is a new message with a new key. Unique to the producer
 * run, stable for the life of the message. `n` alone restarts at zero with each process, so a restarted producer
 * would reuse the key of different work and a third party would drop it as a duplicate. The last `:` splits run from sequence (the fake third party's audit reads it).
 */
export const workMessageId = (run: string, n: number): string => `${run}:${n}`;

/**
 * What the producer publishes. A consumer declares its own contract (`packages/consumer`), which must accept this
 * shape: `n` an integer, since it keeps the idempotency key stable across a redelivery.
 */
export const WorkMessage = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
export type WorkMessage = typeof WorkMessage.Type;

const WorkMessageJson = Schema.fromJsonString(WorkMessage);
export const encodeWorkMessage = Schema.encodeSync(WorkMessageJson);

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
 * The redrive election: `x-single-active-consumer` delivers to one bound consumer and holds the rest as backups,
 * promoting one if the active one disconnects. Nothing is published here but the trigger itself.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

export const redriveTriggerQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-single-active-consumer": true },
  durable: true,
});

/**
 * Poison, parked for a human: what the consumer cannot read, what the third party refused, and what the redrive
 * gave up on after `MAX_REDRIVES`. Terminal, like the dead-letter queue.
 */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

/** Why a message was parked: `refused-<status>`, `unreadable-<format|malformed|keyless>`, or `redriven-too-often`. */
export const PARKED_REASON_HEADER = "x-egress-parked-reason";

export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/** Stamped on a redriven message; absent means it has never been redriven. */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/**
 * Redrives before a message is treated as poison rather than unlucky. Each one grants a fresh
 * `WORK_DELIVERY_LIMIT`, so this bounds outages survived, not attempts.
 */
export const MAX_REDRIVES = 5;

/**
 * Counted returns (`requeue`) the broker allows: a message is delivered `WORK_DELIVERY_LIMIT + 1` times, and the
 * return after the last dead-letters it (DeadLetter.test.ts pins the count). The budget belongs to the queue, not
 * the daemon: an in-process counter is lost when the message moves to another consumer, which is what an outage
 * causes. Low because RabbitMQ redelivers with no backoff, so every extra attempt is load on a failing upstream.
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

