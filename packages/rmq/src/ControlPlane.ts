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

/** One routing key per API: a fleet binds only its own and never sees other APIs' events. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

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
  },
  durable: true,
});

/** No delivery limit: this queue is the end of the line. */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum" },
  durable: true,
});

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
