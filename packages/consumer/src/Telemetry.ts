import { Metric } from "effect";

/**
 * Everything this fleet exposes to Prometheus. `breakerState` is a gauge per
 * replica — Prometheus's own `instance` label (from the DNS-SD scrape) is
 * what turns this into "one line per breaker" on the dashboard, and nothing
 * here aggregates them: `@egress/aggregator` does that from a different
 * signal entirely (`circuit.control`, see `consumer.ts`'s `onStateChange`),
 * off this process and off the hot path.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description: "Calls made toward the third party, by outcome (ok, failed, or open — rejected locally by this replica's own breaker, no call made).",
});

export const inFlight = Metric.gauge("egress_consumer_in_flight", {
  description: "Third-party calls this daemon currently has open.",
});

export const breakerState = Metric.gauge("egress_consumer_breaker_state", {
  description: "This replica's own breaker state (0=closed 1=open 2=half-open). Compare across instances — nothing here makes them agree.",
});

export const breakerTrips = Metric.counter("egress_consumer_breaker_trips_total", {
  description: "Times this replica's breaker has opened.",
});

export const redrives = Metric.counter("egress_consumer_redrives_total", {
  description: "Dead-lettered messages this replica's redrive passes have moved back onto the work queue (outcome=moved) or given up on as poison (outcome=parked). Only ever nonzero on whichever replica RabbitMQ currently elects active on the redrive-trigger queue.",
});
