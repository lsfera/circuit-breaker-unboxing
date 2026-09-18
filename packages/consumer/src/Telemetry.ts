import { Metric } from "effect";

/**
 * Everything this fleet exposes to Prometheus. `breakerState` is a gauge per
 * replica — Prometheus's own `instance` label (from the DNS-SD scrape) is
 * what turns this into "one line per breaker" on the dashboard, which is the
 * whole point: nothing here aggregates the five replicas into one verdict.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description: "Calls made toward the third party, by outcome (ok, failed, or open — rejected locally by this replica's own breaker, no call made).",
});

export const discarded = Metric.counter("egress_consumer_discarded_total", {
  description: "Deliveries this replica refused to spend a call on, by reason: `format` (a content type, encoding or message type it does not read) `malformed` (a body that does not decode as a work message) or `keyless` (no `message_id` to use as the idempotency key). Each goes to the dead-letter queue unread.",
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
