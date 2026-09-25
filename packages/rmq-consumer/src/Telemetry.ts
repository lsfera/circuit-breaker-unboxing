import { Metric } from "effect";

/**
 * Everything this fleet exposes to Prometheus. `breakerState` is a gauge per replica; the scrape's `instance`
 * label makes it one line per breaker on the dashboard.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description: "Calls made toward the third party, by outcome — ok; failed (counts toward tripping the breaker); throttled (a 429 while the concurrency limit adapts: the third party is full, not broken, so it is released uncounted and does not count toward tripping); or client_error (a 4xx other than 408 and 429: the third party is up and refused the request, so it is parked and does not count) — and by status: the HTTP status, or timeout / network when none came.",
});

export const discarded = Metric.counter("egress_consumer_discarded_total", {
  description: "Deliveries this replica refused to spend a call on, by reason: `format` (a content type, encoding or message type it does not read) `malformed` (a body that does not decode as a work message) or `keyless` (no `message_id` to use as the idempotency key). Each is parked on `work.parked` unread, with `x-egress-parked-reason`.",
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

export const permitLost = Metric.counter("egress_consumer_probe_permit_lost_total", {
  description: "Half-open probes this replica did not make because another replica held the fleet's one probe permit. The message is released, not charged, and the breaker holds again at the same attempt.",
});

export const redrives = Metric.counter("egress_consumer_redrives_total", {
  description: "Dead-lettered messages this replica's redrive passes moved back onto the work queue (outcome=moved) or gave up on as poison (outcome=parked). Nonzero only on the replica RabbitMQ elects active on the redrive-trigger queue.",
});

export const concurrencyLimit = Metric.gauge("egress_consumer_concurrency_limit", {
  description: "How many third-party calls this replica currently lets itself have open at once — the limit learned from the third party's 429s, never above MAX_IN_FLIGHT.",
});
