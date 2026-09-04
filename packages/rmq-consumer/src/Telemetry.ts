import { Metric } from "effect";

/**
 * Everything the daemon fleet exposes to Prometheus, in one place — the peer
 * of @egress/aggregator's Telemetry.ts, and deliberately the same shape.
 *
 * Until this existed the fleet was the one half of the system with no
 * metrics at all: its counters lived in closure variables and reached the
 * outside world only through `docker logs`. That is not a cosmetic gap. The
 * bug that cost the most time here was a daemon that went *deaf* — container
 * up, CPU idle, everything looking normal — and it hid because the only
 * evidence a daemon was alive was a log line it emitted while handling an
 * event, so "gone deaf" and "nothing happened" looked identical. A heartbeat
 * log made it findable; `egress_daemon_target_active` next to
 * `egress_daemon_self_active` makes it a five-second read on a dashboard.
 *
 * Values are published by a flush loop in daemon.ts rather than at each call
 * site, because the message path is a plain async function running a few
 * hundred times a second and a fiber per metric write would be the most
 * expensive thing in it. Counters are updated by delta, which is exactly
 * what a Prometheus counter is.
 */

export const circuitState = Metric.gauge("egress_daemon_circuit_state", {
  description:
    "The circuit state this daemon last received (0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN). " +
    "Diverging from egress_circuit_state means the daemon has stopped hearing the control plane.",
});

export const targetActive = Metric.gauge("egress_daemon_target_active", {
  description: "How many daemons this one believes should be pulling work right now.",
});

export const fleetSize = Metric.gauge("egress_daemon_fleet_size", {
  description: "Fleet size this daemon was configured with.",
});

export const selfActive = Metric.gauge("egress_daemon_self_active", {
  description: "1 if this daemon currently holds a work connection, 0 if it is idle.",
});

export const inFlight = Metric.gauge("egress_daemon_in_flight", {
  description: "Third-party calls this daemon has open right now.",
});

export const queued = Metric.gauge("egress_daemon_queued", {
  description:
    "Deliveries parked at the concurrency gate, unsettled — the point at which " +
    "backpressure has reached the broker and credit stops being replenished.",
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

export const published = Metric.counter("egress_producer_published_total", {
  description: "Work messages published onto the work queue, by API.",
});
