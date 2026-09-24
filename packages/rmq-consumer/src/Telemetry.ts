import { Metric } from "effect";

/**
 * Everything the daemon fleet exposes to Prometheus — the peer of
 * @egress/aggregator's Telemetry.ts, deliberately the same shape.
 *
 * `egress_daemon_target_fraction` against `rabbitmq_detailed_queue_consumers`
 * (the broker's own count of consumers on `<api>.work`) is the pair that makes
 * a daemon which has gone deaf to the control plane obvious; on its own, that
 * failure looks exactly like a circuit that has not moved. The daemon used to
 * publish its own view of that second half as `egress_daemon_self_active`, but
 * the broker already knows who is consuming — see docs/operations.md.
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
    "Against rabbitmq_detailed_queue_consumers on the work queue, the gap between " +
    "intended and actual is the cost of selecting by hash rather than by index — " +
    "see ADR 013. A persistent gap is a fleet too small for the fraction to land.",
});

export const floorHeld = Metric.gauge("egress_daemon_floor_held", {
  description:
    "1 on the single daemon the broker has elected to run regardless of the " +
    "fraction. Should sum to exactly 1 across the fleet whenever the target is " +
    "non-zero; 0 means a DEGRADED fleet could stop entirely.",
});

export const calls = Metric.counter("egress_daemon_calls_total", {
  description: "Third-party calls made through the egress listener, by outcome.",
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
export const controlGaps = Metric.counter("egress_daemon_control_gaps_total", {
  description: "Sequence gaps observed in the state_changed stream on circuit.control.",
});

export const controlDuplicates = Metric.counter("egress_daemon_control_duplicates_total", {
  description: "Repeated sequences observed in the state_changed stream on circuit.control.",
});


export const discarded = Metric.counter("egress_daemon_discarded_total", {
  description:
    "Work deliveries this daemon refused to spend a call on, by reason: `format` (a content " +
    "type, encoding or message type it does not read), `malformed` (a body that does not " +
    "decode as a work message) or `keyless` (no `message_id` to use as the idempotency key). " +
    "Each goes to the dead-letter queue unread.",
});

export const controlStale = Metric.counter("egress_daemon_control_stale_total", {
  description:
    "Control events this daemon ignored because an event it had already applied " +
    "out-ranks them: an older leader's lease, or a sequence behind the one applied. " +
    "Non-zero around a failover is a paused leader resuming; anywhere else it is worth a look.",
});
