import { Metric } from "effect";
import type { State } from "@egress/domain/Model.ts";

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

/** 0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN — a stepped line per API in Grafana. */
export const STATE_CODE: Record<State, number> = {
  CLOSED: 0,
  DEGRADED: 1,
  OPEN: 2,
  HALF_OPEN: 3,
};

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

export const circuitReportingReplicas = Metric.gauge(
  "egress_circuit_reporting_replicas",
  { description: "Replicas currently reporting for this API (within replicaTimeoutMs)." },
);

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
