import { Metric } from "effect";

/**
 * All metrics the demo emits, in one place. Every metric is plain `effect`
 * Metric — no separate client, no separate port. `PrometheusMetrics.layerHttp`
 * in main.ts reads this same in-process registry and formats it; there is
 * nothing here to keep in sync by hand.
 *
 * Per-API series use `Metric.withAttributes(metric, { apiId })` at the call
 * site rather than baking `apiId` in up front, because the API set is not
 * known until the fleet reports it.
 */

/**
 * 0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN — a stepped line per API in
 * Grafana. Re-exported rather than defined here: @egress/rmq-consumer's
 * daemons publish their own view of the same state, and the two lines are
 * only comparable if they share one encoding.
 */
export { STATE_CODE } from "@egress/domain/Model.ts";

export const circuitState = Metric.gauge("egress_circuit_state", {
  description:
    "Published breaker state per API (0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN).",
});

export const circuitHealthyEndpoints = Metric.gauge(
  "egress_circuit_healthy_endpoints",
  { description: "Fleet-averaged healthy endpoint count per API." },
);

export const circuitTotalEndpoints = Metric.gauge(
  "egress_circuit_total_endpoints",
  { description: "Total endpoint count per API." },
);

/**
 * Fleet-wide sum of `outlier_detection.ejections_active`, the one Envoy
 * signal that says "a host was ejected" as opposed to "membership changed".
 * Not an input to the breaker — see ApiSnapshot's note — but the first thing
 * worth looking at when a DEGRADED is unexplained.
 */
export const circuitEjectionsActive = Metric.gauge("egress_circuit_ejections_active", {
  description: "Ejected hosts summed across every reporting replica, per API.",
});

export const circuitReportingReplicas = Metric.gauge(
  "egress_circuit_reporting_replicas",
  { description: "Replicas currently reporting for this API (within replicaTimeoutMs)." },
);

/**
 * Replicas that stopped contributing to the fleet's verdict, by how they went.
 *
 * The gauge above is the *denominator* every quorum here is computed against —
 * `votes.DOWN / live.length` — and until this counter existed it could change
 * without anything saying so. A replica pushing without a node id, a stream
 * that died, an admin port that stopped answering: all three removed a replica
 * from the fleet in silence, and a fraction over a smaller denominator is a
 * weaker claim wearing the same number.
 *
 * Counted on the *departure*, not per failed poll, so one unreachable replica
 * is one increment rather than four a second. See
 * docs/decisions/009-what-the-quorum-is-a-quorum-of.md.
 */
export const replicasLost = Metric.counter("egress_fleet_replica_lost_total", {
  description:
    "Replicas that stopped contributing to a fleet quorum, by reason (no-node-id, went-quiet, unreachable).",
});

export const circuitSequence = Metric.gauge("egress_circuit_sequence", {
  description: "Last published sequence number per API.",
});

export const circuitTransitions = Metric.counter(
  "egress_circuit_transitions_total",
  { description: "Published state_changed events, by API, from-state and reason." },
);

export const circuitSnapshots = Metric.counter("egress_circuit_snapshots_total", {
  description: "Periodic egress.circuit.snapshot republishes, by API.",
});

export const fleetPollDuration = Metric.timer("egress_fleet_poll_duration_ms", {
  description: "Time to poll and parse every replica once per tick.",
});

export const isLeader = Metric.gauge("egress_aggregator_is_leader", {
  description:
    "1 if this aggregator instance currently holds the publishing lease, 0 otherwise.",
});

/**
 * The liveness signal for the control loop itself, incremented every tick by
 * every instance whether or not it leads.
 *
 * There was no such signal, and its absence is what made the failure it now
 * detects invisible: when the loop died, `/metrics` kept serving the last
 * values it had, `egress_aggregator_is_leader` stayed pinned at 1, and every
 * gauge simply stopped moving — which looks exactly like a quiet system.
 * `rate(egress_aggregator_ticks_total[1m]) == 0` is the alert that says
 * otherwise.
 */
export const ticks = Metric.counter("egress_aggregator_ticks_total", {
  description: "Control-loop iterations, by instance. Zero rate means the loop is gone.",
});

export const coordinationErrors = Metric.counter(
  "egress_aggregator_coordination_errors_total",
  {
    description:
      "Ticks that could not reach the coordinator and stood down. Sustained non-zero " +
      "means no instance is publishing, because none of them can confirm it holds the lease.",
  },
);

export const fencingConflicts = Metric.counter(
  "egress_aggregator_fencing_conflicts_total",
  {
    description:
      "Checkpoint writes rejected because a newer lease holder already took over, by API.",
  },
);

export const webhookDelivered = Metric.counter(
  "egress_webhook_delivered_total",
  { description: "Events the sink delivered successfully, by API." },
);

export const webhookFailed = Metric.counter("egress_webhook_failed_total", {
  description: "Delivery attempts (including retries) that did not succeed, by API.",
});

export const webhookDeadLettered = Metric.counter(
  "egress_webhook_dead_lettered_total",
  { description: "Events that exhausted retries and were dead-lettered, by API." },
);

export const webhookDeliveryDuration = Metric.timer(
  "egress_webhook_delivery_duration_ms",
  { description: "Latency of successful deliveries, including any retries." },
);

/**
 * The durable outbox (Outbox.ts), which is where an event goes when the
 * subscriber will not take it.
 *
 * Depth is the one to alert on: it is zero in the healthy case, and a value
 * that stays non-zero is a subscriber that has stopped taking events while
 * everything else looks fine. `dropped_total` moving at all means the bound
 * was hit and a subscriber has permanently missed events — the gap it will
 * see is deliberate, and this is where it becomes visible from the publisher's
 * side too.
 */
export const outboxDepth = Metric.gauge("egress_webhook_outbox_depth", {
  description: "Events waiting in the durable outbox for a subscriber that is not taking them, by API.",
});

export const outboxReplayed = Metric.counter("egress_webhook_outbox_replayed_total", {
  description: "Events delivered from the outbox after an earlier attempt failed, by API.",
});

export const outboxDropped = Metric.counter("egress_webhook_outbox_dropped_total", {
  description: "Oldest events discarded because the outbox hit its per-API bound, by API.",
});

/**
 * These three read the delivery contract from OUTSIDE the process — the same
 * boundary Http.ts's `Integrity` tracker checks — so a gap or duplicate here
 * is a real, observable break of the per-API sequence guarantee, not a
 * simulated one.
 */
export const subscriberReceived = Metric.counter(
  "egress_subscriber_events_received_total",
  { description: "Events the demo subscriber endpoint has received." },
);

export const subscriberGaps = Metric.counter("egress_subscriber_gaps_total", {
  description: "Sequence gaps detected at the subscriber, by API.",
});

export const subscriberDuplicates = Metric.counter(
  "egress_subscriber_duplicates_total",
  { description: "Non-snapshot events delivered with a repeated sequence, by API." },
);
