import { Metric } from "effect";

/** Every metric, in effect's in-process registry; per-API series add `apiId` at the call site. */

/** 0=CLOSED 1=DEGRADED 2=OPEN 3=HALF_OPEN, shared with the daemons' gauge. */
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

/** Not a breaker input; the first thing to check when a DEGRADED is unexplained. */
export const circuitEjectionsActive = Metric.gauge("egress_circuit_ejections_active", {
  description: "Ejected hosts summed across every reporting replica, per API.",
});

export const circuitReportingReplicas = Metric.gauge(
  "egress_circuit_reporting_replicas",
  { description: "Replicas currently reporting for this API (within replicaTimeoutMs)." },
);

/** The quorum's denominator must not shrink silently (ADR 009). Counted per departure. */
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

/** Every tick, leader or not: tells a standby that is not ready from one that lost the race. */
export const controlPlaneReady = Metric.gauge("egress_aggregator_control_plane_ready", {
  description:
    "1 if this instance's control-plane sink can currently deliver, 0 otherwise. " +
    "A leader that drops to 0 steps down instead of publishing into nothing.",
});

/** A dead loop leaves every gauge frozen at its last value; a zero rate is what says so. */
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

/** Depth is the alert: zero when healthy. `dropped_total` moving means a subscriber has a permanent gap. */
export const outboxDepth = Metric.gauge("egress_webhook_outbox_depth", {
  description: "Events waiting in the durable outbox for a subscriber that is not taking them, by API.",
});

export const outboxReplayed = Metric.counter("egress_webhook_outbox_replayed_total", {
  description: "Events delivered from the outbox after an earlier attempt failed, by API.",
});

export const outboxDropped = Metric.counter("egress_webhook_outbox_dropped_total", {
  description: "Oldest events discarded because the outbox hit its per-API bound, by API.",
});

/** The delivery contract read from outside the process (Http.ts). */
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

export const consoleStreams = Metric.gauge("egress_console_streams", {
  description: "Browsers connected to /api/stream on this instance.",
});

/** 2.5/s or 0; a rate rising with `egress_console_streams` means frames are built per connection again. */
export const consoleFramesBuilt = Metric.counter("egress_console_frames_built_total", {
  description: "State frames built for the console, once per interval while any console is connected.",
});

export const consoleAttentionStreams = Metric.gauge("egress_console_attention_streams", {
  description: "Browsers connected to the attention view on this instance.",
});

/** Only when something changed; flat for a quiet fleet. */
export const consoleAttentionBuilt = Metric.counter("egress_console_attention_built_total", {
  description: "Attention-view patches published, once per interval something in the view changed.",
});
