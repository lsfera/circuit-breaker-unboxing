/**
 * The work-queue shape a producer and a competing-consumer fleet share: a durable work queue with a dead-letter
 * destination and the broker's own delivery-limit budget.
 */

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
 * Counted returns (`requeue`) the broker allows: a message is delivered `WORK_DELIVERY_LIMIT + 1` times, and the
 * return after the last dead-letters it (DeadLetter.test.ts pins the count). The budget belongs to the queue, not
 * the daemon: an in-process counter is lost when the message moves to another consumer, which is what an outage
 * causes. Low because RabbitMQ redelivers with no backoff, so every extra attempt is load on a failing upstream.
 */
export const WORK_DELIVERY_LIMIT = 3;

/**
 * Durability is decided here so producer and daemons cannot disagree: a mismatch is a redeclare conflict
 * (406 `PRECONDITION_FAILED`, `inequivalent arg 'durable'`), so changing a flag means deleting the queue first. Always durable:
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

