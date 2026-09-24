import { Metric } from "effect";

/**
 * Everything this fleet exposes to Prometheus. `breakerState` is a gauge per replica; the scrape's `instance`
 * label makes it one line per breaker on the dashboard.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description: "Calls made toward the third party, by outcome — ok; failed (counts against the breaker); client_error (a 4xx other than 408 and 429: the third party is up and refused the request, so it is dead-lettered and does not count); or open (rejected locally by this replica's own breaker or lost the probe-permit race, no call made) — and by status: the HTTP status, or timeout / network when none came, or none for open.",
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

export const redrives = Metric.counter("egress_consumer_redrives_total", {
  description: "Dead-lettered messages this replica's redrive passes have moved back onto the work queue (outcome=moved) or given up on as poison (outcome=parked). Only ever nonzero on whichever replica RabbitMQ currently elects active on the redrive-trigger queue.",
});
