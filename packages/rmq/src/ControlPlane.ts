import { Schema } from "effect";
import { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Naming conventions for the circuit.control control plane, shared by
 * @egress/aggregator's AmqpControlPlaneSink (publisher side) and
 * @egress/rmq-consumer's daemon fleet (consumer side), so the two are never
 * tempted to drift apart on topology.
 */

export const CONTROL_EXCHANGE = "circuit.control";

/** One routing key per API — daemons for `payments-provider` bind only this, never see other APIs' events. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

/** Each daemon fleet's own queue on the control exchange. Not durable, not shared — every daemon process gets its own. */
export const controlQueueFor = (apiId: string, instanceId: string): string =>
  `${apiId}.control.${instanceId}`;

/** The always-idle SAC coordination queue for HALF_OPEN prober election — one per API, shared by every daemon in that API's fleet. */
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

/** The work queue's arguments. Identical to `deadLetterArgs` today, kept as its own name because the work queue is the one whose dead-lettering is a designed behaviour rather than a backstop. */
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

export const encodeCircuitEvent = (event: CircuitEvent): string => JSON.stringify(event);

const decode = Schema.decodeUnknownOption(CircuitEvent);

/** Same decode path @egress/subscriber uses — the control plane and the HTTP/SSE path never disagree on what a valid event looks like. */
export const decodeCircuitEvent = (body: string) => {
  try {
    return decode(JSON.parse(body));
  } catch {
    return decode(undefined); // malformed JSON decodes to None, same as a schema mismatch
  }
};
