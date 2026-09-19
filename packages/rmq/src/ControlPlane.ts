/**
 * Naming conventions for the work-queue shape shared by the producer and any
 * competing-consumer fleet that drains it, plus (as of article 4) the
 * `circuit.control` exchange every replica's breaker transitions go out on.
 * Article 1 removed this exchange's full master-branch shape — SAC election
 * queues, the domain package, an HA aggregator — along with the packages
 * that used it. Article 4 restores only the naming convention below, for a
 * single-instance, notification-only aggregator (`@egress/aggregator`); the
 * rest stays out of scope. See that package and this branch's README for why.
 */

/**
 * The payments idempotency key: an AMQP header inbound, an HTTP header on the
 * call it authorizes. A retry that reuses the same key protects the third
 * party from being charged twice for one logical attempt.
 */
export const IDEMPOTENCY_KEY_HEADER = "x-idempotency-key";

/**
 * Every replica's breaker transitions go out here, and `@egress/aggregator`
 * is the only subscriber. A topic exchange (not fanout) because the routing
 * key already carries the `apiId` a binding might one day want to filter
 * on — this deployment only ever runs one, but the exchange type shouldn't
 * have to change the day a second one shows up. Every declarer must pass
 * `{ durable: true }` — `declareTopicExchange`'s own default is `false`,
 * and a redeclare that disagrees with what's on the broker is a connection-
 * closing `406 PRECONDITION-FAILED`, not a warning.
 */
export const CONTROL_EXCHANGE = "circuit.control";

/** One routing key per API — `circuit.*` binds every one a deployment runs. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

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
 * Article 5's redrive election. A durable quorum queue whose only job is
 * picking exactly one replica: `x-single-active-consumer` means the broker
 * delivers to one bound consumer and holds the rest as backups, promoting
 * automatically if the active one disconnects. Nothing is ever published
 * here but the trigger itself (see `consumer.ts`), so unlike master's
 * version of this queue there is no malformed-payload case to dead-letter
 * defensively against — the only thing that can ever land on it is a
 * trigger this same codebase minted.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

export const redriveTriggerQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-single-active-consumer": true },
  durable: true,
});

/**
 * Where a message goes once it has been redriven `MAX_REDRIVES` times
 * without succeeding — treated as poison rather than unlucky, so the
 * elected redriver's periodic passes stop replaying it forever. Terminal
 * like the dead-letter queue, and for the same reason never allowed to drop
 * at a delivery limit.
 */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/**
 * Stamped on a redriven message so the next pass can tell a message caught
 * in its second outage from one that has failed every single time. Absent
 * means zero — a message dead-lettered by the broker directly, never yet
 * redriven.
 */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/**
 * Redrives before a message is treated as poison rather than unlucky. Each
 * redrive republishes onto the work queue, which grants a fresh
 * `WORK_DELIVERY_LIMIT`-attempt budget — so this bounds outages survived,
 * not attempts: five outages' worth of the third party rejecting the same
 * message is enough evidence it will never be accepted.
 */
export const MAX_REDRIVES = 5;

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

