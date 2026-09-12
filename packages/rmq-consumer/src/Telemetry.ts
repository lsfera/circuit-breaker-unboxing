import { Metric } from "effect";

/**
 * Everything the daemon fleet exposes to Prometheus — the peer of
 * @egress/aggregator's Telemetry.ts, deliberately the same shape.
 *
 * `egress_daemon_target_active` next to `egress_daemon_self_active` is the pair
 * that makes a daemon which has gone deaf to the control plane obvious; on its
 * own, that failure looks exactly like a circuit that has not moved.
 *
 * Published by a flush loop in daemon.ts rather than at each call site — see
 * Tally.ts.
 */

export const circuitState = Metric.gauge("egress_daemon_circuit_state", {
  description:
    "The circuit state this daemon last received (0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN). " +
    "Diverging from egress_circuit_state means the daemon has stopped hearing the control plane.",
});

export const targetFraction = Metric.gauge("egress_daemon_target_fraction", {
  description:
    "What proportion of the fleet should be pulling work right now, 0 to 1. " +
    "Summed against egress_daemon_self_active across the fleet, the gap between " +
    "intended and actual is the cost of selecting by hash rather than by index — " +
    "see ADR 013. A persistent gap is a fleet too small for the fraction to land.",
});

export const floorHeld = Metric.gauge("egress_daemon_floor_held", {
  description:
    "1 on the single daemon the broker has elected to run regardless of the " +
    "fraction. Should sum to exactly 1 across the fleet whenever the target is " +
    "non-zero; 0 means a DEGRADED fleet could stop entirely.",
});

export const selfActive = Metric.gauge("egress_daemon_self_active", {
  description: "1 if this daemon currently holds a work connection, 0 if it is idle.",
});

export const inFlight = Metric.gauge("egress_daemon_in_flight", {
  description: "Third-party calls this daemon has open right now.",
});

export const calls = Metric.counter("egress_daemon_calls_total", {
  description: "Third-party calls made through the egress listener, by outcome.",
});

export const deadLettered = Metric.counter("egress_daemon_dead_lettered_total", {
  description:
    "Work messages rejected to the dead-letter queue because their call failed. " +
    "Before this existed the same messages were accepted and silently lost.",
});

export const redriven = Metric.counter("egress_daemon_redriven_total", {
  description:
    "Dead-lettered work messages replayed onto the work queue after recovery. " +
    "Without this the dead-letter queue only ever grows: it is where failed work " +
    "is preserved, and preserving it is not the same as recovering it.",
});

export const undecodable = Metric.counter("egress_daemon_undecodable_total", {
  description:
    "Messages this daemon could not read — a control event that failed the " +
    "published schema, a malformed election trigger — rejected onto the canonical " +
    "dead-letter queue rather than accepted. Non-zero means someone is publishing " +
    "something the fleet does not understand, and the evidence is still on the queue.",
});

export const probes = Metric.counter("egress_daemon_probes_total", {
  description: "HALF_OPEN probes this daemon was elected to run by the broker.",
});

/**
 * The delivery contract, checked on the AMQP transport by a different family
 * of processes than the one that checks it over HTTP.
 *
 * `/api/subscriber` proves the aggregator's webhook stream is gapless; these
 * two prove the same thing for `circuit.control`, from five independent
 * consumers. A sequence that repeats or skips here is the same class of bug
 * — and, notably, exactly what a leader that resumed from a stale in-memory
 * sequence would look like from the outside.
 */
export const controlEvents = Metric.counter("egress_daemon_control_events_total", {
  description: "Control-plane events this daemon decoded, by event type.",
});

export const controlGaps = Metric.counter("egress_daemon_control_gaps_total", {
  description: "Sequence gaps observed in the state_changed stream on circuit.control.",
});

export const controlDuplicates = Metric.counter("egress_daemon_control_duplicates_total", {
  description: "Repeated sequences observed in the state_changed stream on circuit.control.",
});

