/**
 * Naming conventions for the work-queue shape shared by the producer and any
 * competing-consumer fleet that drains it. This used to also carry the
 * `circuit.control` exchange (SAC queues, election triggers, the published
 * circuit event) for the breaker's control plane — removed on this branch
 * along with the packages that used it (`@egress/aggregator`,
 * `@egress/domain`, the circuit-aware `@egress/rmq-consumer`). What's left is
 * the generic part: a durable work queue with a dead-letter destination, the
 * broker's own delivery-limit budget, and the idempotency-key convention a
 * caller and a fake third party can agree on.
 */

/**
 * The payments idempotency key: an AMQP header inbound, an HTTP header on the
 * call it authorizes. A retry that reuses the same key protects the third
 * party from being charged twice for one logical attempt.
 */
export const IDEMPOTENCY_KEY_HEADER = "x-idempotency-key";

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

