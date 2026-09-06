import { Schema } from "effect";
import { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Naming conventions for the circuit.control control plane, shared by
 * @egress/aggregator's AmqpControlPlaneSink (publisher side) and
 * @egress/rmq-consumer's daemon fleet (consumer side), so the two are never
 * tempted to drift apart on topology.
 */

export const CONTROL_EXCHANGE = "circuit.control";

/** One routing key per API: a fleet binds only its own and never sees other APIs' events. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

/** Every daemon process gets its own queue on the control exchange — not shared, not durable. */
export const controlQueueFor = (apiId: string, instanceId: string): string =>
  `${apiId}.control.${instanceId}`;

/** The always-idle SAC queue that elects the HALF_OPEN prober: one per API, contended by that API's whole fleet. */
export const probeTriggerQueueFor = (apiId: string): string => `${apiId}.probe-trigger`;

/**
 * The second SAC coordination queue, same shape as the prober election and
 * for the same reason: recovering the dead-letter queue is a job exactly one
 * daemon may do. Five daemons each replaying the same backlog would turn a
 * recovery into a fivefold burst at a third party that just came back.
 *
 * Separate from `probe-trigger` rather than reusing it, because the two
 * elections are independent — the daemon that happens to be the elected
 * prober has no particular claim on being the one that redrives, and
 * coupling them would mean a single daemon's failure took out both.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

/**
 * Where work that could not be completed ends up.
 *
 * The event side of this repo proves a delivery contract end to end; the
 * work side used to accept every message whether or not the call behind it
 * succeeded, which quietly threw the failures away. A dead-letter queue is
 * what makes the two halves comparable: a failed call is now a message you
 * can count, look at in the management UI, and replay, instead of an
 * increment in a counter nobody can act on.
 */
export const deadLetterQueueFor = (apiId: string): string => `${apiId}.work.dead`;

/**
 * The dead-letter target, for *every* queue this API's fleet declares rather
 * than only the work queue.
 *
 * One canonical destination is the point. A control message that fails to
 * decode, a malformed election trigger, a work message whose call failed —
 * all of them are "something arrived that could not be handled", and all of
 * them should end up in one place you can look at, count, and replay from.
 * The alternative is what this repo had: the work queue dead-lettered
 * properly while every other queue silently dropped whatever it rejected,
 * which is the same silent loss the work queue was fixed to avoid, just in
 * the corner nobody looks at.
 *
 * Routing through the default exchange (`""`) with the dead-letter queue's
 * own name as the routing key is the plainest form of this: no extra
 * exchange to declare, no binding to keep in step.
 *
 * Declared identically by every process that touches a given queue —
 * producer and daemons alike — because RabbitMQ rejects a redeclare whose
 * arguments differ from the existing queue's, and there is no ordering
 * between those containers at startup.
 */
export const deadLetterArgs = (apiId: string): Record<string, unknown> => ({
  "x-dead-letter-exchange": "",
  "x-dead-letter-routing-key": deadLetterQueueFor(apiId),
});

/**
 * The work queue's arguments — identical to `deadLetterArgs` today, kept
 * separately named because here dead-lettering is designed behaviour rather
 * than a backstop.
 */
export const workQueueArgs = deadLetterArgs;

/**
 * A single-active-consumer queue that also dead-letters. The SAC queues carry
 * election triggers, so nothing routine is ever rejected on them — but a
 * malformed trigger is exactly the kind of thing worth keeping rather than
 * dropping, and it costs one argument to say so.
 *
 * The dead-letter queue itself is deliberately not given a target: a queue
 * that dead-letters to itself is a cycle, and it is the end of the line by
 * definition.
 */
export const sacQueueArgs = (apiId: string): Record<string, unknown> => ({
  ...deadLetterArgs(apiId),
  "x-single-active-consumer": true,
});

/**
 * How many times a unit of work is attempted before the broker parks it.
 *
 * The budget is a queue property, not a client capability — which is why this
 * repo twice concluded it could not be expressed. A quorum queue with
 * `x-delivery-limit` counts the redeliveries itself and dead-letters at the
 * limit, through the same client that still reports `deliveryCount: 0` on
 * every delivery. Measured with the handler returning `requeue` every time:
 * four deliveries, then the dead-letter queue with `reason "delivery_limit"`.
 * See docs/decisions/001-amqp-client.md.
 *
 * Three rather than more because RabbitMQ redelivers immediately with no
 * backoff, so every extra attempt is load on a third party that is already
 * failing. What ends the amplification is the circuit opening, which stops the
 * daemons consuming at all.
 *
 * It composes with `REDRIVE_ON_CLOSE` by resetting: a redrive republishes the
 * body, so a replayed message arrives as a new one with a fresh budget.
 * Measured through one redrive cycle — eight deliveries, two parkings, both
 * `reason "delivery_limit"`. Three attempts per outage, not three ever.
 */
export const WORK_DELIVERY_LIMIT = 3;

/**
 * Which queues survive a broker restart, decided here so producer and daemons
 * cannot disagree — a durability mismatch is a redeclare conflict exactly like
 * a mismatched argument. Measured in both directions: the broker answers
 * `409 "inequivalent arg 'durable'"` when the flag differs from the existing
 * queue, so flipping it on a broker that already holds the queues means
 * deleting them first.
 *
 * The split is about what a restarting consumer can rebuild for itself. A
 * control queue is a live subscription — a daemon that comes back learns the
 * state from the aggregator's next snapshot — so keeping those events buys
 * nothing and risks a queue growing behind a daemon that never returns. It
 * stays classic and transient, one per daemon, and dies with it.
 *
 * Everything else is `quorum`, the second half of the same question: `durable`
 * decides what survives the broker process, `x-queue-type` what survives
 * losing the node the queue lives on. A quorum queue cannot be transient
 * (measured: `400 "invalid property 'non-durable'"`), so the two are one
 * decision. Work and dead-letter queues hold work nothing can reconstruct; the
 * election queues are always empty, which makes quorum free for them.
 */
export const workQueueOptions = (apiId: string) => ({
  args: {
    ...workQueueArgs(apiId),
    "x-queue-type": "quorum",
    "x-delivery-limit": WORK_DELIVERY_LIMIT,
  },
  durable: true,
});

/**
 * No delivery limit of its own: this queue is the end of the line, nothing
 * requeues on it, and a message dropped here would be the silent loss the
 * dead-letter queue exists to prevent.
 */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum" },
  durable: true,
});

export const controlQueueOptions = (apiId: string) => ({
  args: deadLetterArgs(apiId),
  durable: false,
});

export const sacQueueOptions = (apiId: string) => ({
  args: { ...sacQueueArgs(apiId), "x-queue-type": "quorum" },
  durable: true,
});

/** Durable so the topology itself survives, even though what it feeds does not need to. */
export const CONTROL_EXCHANGE_OPTIONS = { durable: true };

export const encodeCircuitEvent = (event: CircuitEvent): string => JSON.stringify(event);

const decode = Schema.decodeUnknownOption(CircuitEvent);

/** The decode path @egress/subscriber uses, so the control plane and the HTTP/SSE path cannot disagree on what a valid event is. */
export const decodeCircuitEvent = (body: string) => {
  try {
    return decode(JSON.parse(body));
  } catch {
    return decode(undefined); // malformed JSON decodes to None, same as a schema mismatch
  }
};
