import { Metric } from "effect";

/**
 * The daemons' metrics, flushed by daemon.ts. `egress_daemon_target_fraction`
 * against the broker's `rabbitmq_detailed_queue_consumers` is what exposes a
 * deaf daemon.
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

/** The delivery contract on `circuit.control`; a stale resumed leader shows here. */
export const controlGaps = Metric.counter("egress_daemon_control_gaps_total", {
  description: "Sequence gaps observed in the state_changed stream on circuit.control.",
});

export const controlDuplicates = Metric.counter("egress_daemon_control_duplicates_total", {
  description: "Repeated sequences observed in the state_changed stream on circuit.control.",
});

export const concurrencyLimit = Metric.gauge("egress_daemon_concurrency_limit", {
  description:
    "How many third-party calls this daemon currently lets itself have open at once: the " +
    "limit learned from 429s (Envoy shedding, or the third party's own), never above MAX_IN_FLIGHT.",
});

export const discarded = Metric.counter("egress_daemon_discarded_total", {
  description:
    "Work deliveries this daemon refused to spend a call on, by reason: `format` (a content " +
    "type, encoding or message type it does not read), `malformed` (a body that does not " +
    "decode as a work message) or `keyless` (no `message_id` to use as the idempotency key). " +
    "Each is parked unread, stamped `unreadable-<reason>`.",
});

export const controlStale = Metric.counter("egress_daemon_control_stale_total", {
  description:
    "Control events this daemon ignored because an event it had already applied " +
    "out-ranks them: an older leader's lease, or a sequence behind the one applied. " +
    "Non-zero around a failover is a paused leader resuming; anywhere else it is worth a look.",
});

/** How much the daemon believes its circuit: the gauge's values, in one place. */
export const CONTROL_KNOWLEDGE_CODE = { heard: 0, unheard: 1, silent: 2 } as const;

export const controlKnowledge = Metric.gauge("egress_daemon_control_knowledge", {
  description:
    "Whether this daemon's circuit is worth believing (0=heard 1=unheard since start 2=silent). " +
    "Unheard, it works nobody; silent — no control event for a minute — it falls back to a " +
    "quarter of the fleet by position, whatever the last circuit said (ADR 019).",
});
