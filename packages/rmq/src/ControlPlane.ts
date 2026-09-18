import { Result, Schema } from "effect";
import { readerFor } from "@egress/domain/Model.ts";
import type { CircuitEvent, DecodeFailure } from "@egress/domain/Model.ts";

/**
 * Naming conventions for the circuit.control control plane, shared by
 * @egress/aggregator's AmqpControlPlaneSink (publisher side) and
 * @egress/rmq-consumer's daemon fleet (consumer side), so the two are never
 * tempted to drift apart on topology.
 */

export const CONTROL_EXCHANGE = "circuit.control";

/**
 * The payments idempotency key: an AMQP header inbound, an HTTP header on the
 * call it authorizes. Minted by the consumer on a message's first call
 * attempt, not by the producer — a key only protects the third party if every
 * retry of the same attempt sends the same one, and a broker requeue hands
 * back the original message with no way to add a header to it. So a failed
 * call is retried by republishing with the key carried forward — see
 * `packages/rmq-consumer/src/Attempts.ts` — and this is the one name that
 * republish, the redrive, and the call itself all import rather than repeat.
 */
export const IDEMPOTENCY_KEY_HEADER = "x-idempotency-key";

/**
 * How many times a message has been *called* — not delivered — since it was
 * last a fresh message. Distinct from the broker's own `x-delivery-count`:
 * that counts deliveries of the same message, and a failed call now retries
 * by republishing a new message rather than requeuing the old one. Absent
 * means zero. Reset (omitted) whenever `REDRIVE_COUNT_HEADER` advances — a
 * redrive is a fresh outage, and deserves a fresh call budget.
 */
export const ATTEMPTS_HEADER = "x-egress-attempts";

/**
 * Where a message came from, stamped by anything that moves a message
 * between queues without going through the broker's own dead-lettering —
 * the daemon's own republish to `<api>.work.dead` once `ATTEMPTS_HEADER`
 * exhausts `WORK_DELIVERY_LIMIT`, and Redrive.ts's parking of a non-work
 * message. Read by Redrive.ts as the fallback for `delivery.deadLetter`,
 * which is only populated when the *broker* did the dead-lettering — a
 * message published straight onto the queue carries no `x-first-death-*` at
 * all, and without this it would look unattributable and never get redriven.
 */
export const ORIGIN_QUEUE_HEADER = "x-egress-origin-queue";
export const ORIGIN_REASON_HEADER = "x-egress-origin-reason";

/** One routing key per API: a fleet binds only its own and never sees other APIs' events. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

/**
 * Reserved on `CONTROL_EXCHANGE` for `@egress/aggregator`'s liveness heartbeat
 * — a publish-and-confirm with nothing behind it, independent of any real
 * CircuitEvent. `routingKeyFor` always returns `circuit.<apiId>`, so a key
 * with no `circuit.` prefix at all can never collide with a real API's
 * binding, present or future, however that binding is shaped. An unroutable
 * message on a topic exchange is simply dropped once confirmed; nothing
 * downstream ever sees this. Lives here, not in the aggregator package that
 * publishes it, so the daemon fleet — every other reader of this
 * exchange's topology — sees it too, same as every other reserved name on
 * this exchange.
 */
export const HEARTBEAT_ROUTING_KEY = "__heartbeat__";

/** Every daemon process gets its own queue on the control exchange, never shared. */
export const controlQueueFor = (apiId: string, instanceId: string): string =>
  `${apiId}.control.${instanceId}`;

/** The always-idle SAC queue that elects the HALF_OPEN prober: one per API, contended by that API's whole fleet. */
export const probeTriggerQueueFor = (apiId: string): string => `${apiId}.probe-trigger`;

/**
 * The redrive election, deliberately a separate queue from `probe-trigger`: the
 * two elections are independent, and sharing one would let a single daemon's
 * failure take out both.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

/** Where work that could not be completed ends up. */
export const deadLetterQueueFor = (apiId: string): string => `${apiId}.work.dead`;

/**
 * Where a message goes once it has been redriven `MAX_REDRIVES` times without
 * succeeding — a true poison message, told apart from a message that only
 * failed because the outage it was caught in hadn't ended yet. The periodic
 * sweep would otherwise replay it forever: dead-letter, redrive, three more
 * calls, dead-letter again.
 */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

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
 * A single-active-consumer queue that also dead-letters, so a malformed trigger
 * is kept. The dead-letter queue itself gets no target — that would be a cycle.
 */
const sacQueueArgs = (apiId: string): Record<string, unknown> => ({
  ...deadLetterArgs(apiId),
  "x-single-active-consumer": true,
});

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

/** Terminal like the dead-letter queue, and for the same reason never allowed to drop at a delivery limit. */
export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/**
 * Stamped on a redriven message so the next redrive can tell a message caught
 * in its second outage from one that has failed every single time. Absent
 * means zero — a message dead-lettered by the broker directly, never yet
 * redriven.
 */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/**
 * Redrives before a message is treated as poison rather than unlucky. Each
 * redrive grants a fresh `WORK_DELIVERY_LIMIT`-attempt budget, so this bounds
 * outages survived, not attempts: five outages' worth of retrying the same
 * message is enough evidence that the upstream will never accept it.
 */
export const MAX_REDRIVES = 5;

/**
 * How long a control queue may sit with no consumer before the broker deletes
 * it. A daemon that leaves the fleet — a scale-down, a replaced instance id —
 * otherwise leaves a queue that is still bound to `circuit.control` and still
 * being published into, with nobody reading it. Measured: three messages in
 * forty seconds, which is one snapshot interval, growing for as long as the
 * broker lives.
 *
 * Ten minutes, because it has to outlast a reconnect. `@egress/rmq` recovers
 * for about five minutes before giving up (see ADR 005), and a queue deleted
 * mid-recovery would be redeclared empty on reconnect — harmless, since a
 * daemon rebuilds its view from the next snapshot, but pointless churn.
 */
export const CONTROL_QUEUE_EXPIRES_MS = 600_000;

/**
 * The floor queue. One per API, bound to `circuit.control` alongside the
 * per-daemon control queues, and single-active-consumer — so every published
 * event also lands in exactly one daemon's lap, and that daemon is the one
 * running while the fraction alone would have selected nobody.
 *
 * Bound rather than published to: the election needs no traffic of its own,
 * and a snapshot every `snapshotMs` is already a heartbeat.
 */
export const floorQueueFor = (apiId: string) => `${apiId}.floor`;

export const floorQueueOptions = () => ({
  args: {
    "x-single-active-consumer": true,
    // Self-deleting for the same reason as a control queue: one left behind
    // by a departed fleet should not keep filling.
    "x-expires": CONTROL_QUEUE_EXPIRES_MS,
    // The lease is short, so an event nobody took is worthless within seconds.
    "x-message-ttl": 30_000,
    "x-max-length": 16,
  },
  durable: true,
});

export const controlQueueOptions = (apiId: string) => ({
  args: { ...deadLetterArgs(apiId), "x-expires": CONTROL_QUEUE_EXPIRES_MS },
  durable: true,
});

export const sacQueueOptions = (apiId: string) => ({
  args: { ...sacQueueArgs(apiId), "x-queue-type": "quorum" },
  durable: true,
});

/** Durable so the topology itself survives, even though what it feeds does not need to. */
export const CONTROL_EXCHANGE_OPTIONS = { durable: true };

export const encodeCircuitEvent = (event: CircuitEvent): string => JSON.stringify(event);

/**
 * The body on both SAC queues. Every daemon publishes one per transition so a
 * trigger still arrives when some are down; the elected consumer dedupes on the
 * sequence.
 *
 * `Natural` is load-bearing: the dedupe is `sequence <= probedSequence`, and any
 * comparison against `NaN` is false, so a sequence that cannot be ordered must
 * not decode. See docs/decisions/007-message-contracts.md.
 */
const ElectionTrigger = Schema.Struct({ sequence: Schema.Natural });
type ElectionTrigger = typeof ElectionTrigger.Type;

export const encodeElectionTrigger = (trigger: ElectionTrigger): string =>
  JSON.stringify(trigger);

export const decodeElectionTrigger: (
  body: string,
) => Result.Result<ElectionTrigger, DecodeFailure> = readerFor(ElectionTrigger);
