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

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

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
