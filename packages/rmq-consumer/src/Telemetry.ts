import { Metric } from "effect";

/**
 * Everything the SDK exposes to Prometheus. `consumer` labels what belongs to one consumer (its `<key>.work`);
 * `dependency` what belongs to one dependency's breaker, shared by every consumer that calls it. The scrape's
 * `instance` label tells replicas apart.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description:
    "Calls a consumer made to a dependency, by consumer, dependency, outcome — ok; failed (counts toward tripping that dependency's breaker); throttled (the dependency is full, not broken, while the concurrency limit adapts: released uncounted); client_error (the dependency answered and refused this message: parked, does not count) — and status: the reason the application's classifier gave (an HTTP status, an SQL error), or timeout / defect when the SDK decided first."
});

export const discarded = Metric.counter("egress_consumer_discarded_total", {
  description:
    "Deliveries a consumer refused to act on, by consumer and reason: `format` (a content type, encoding or message type its negotiation declines) or `malformed` (a body that does not decode as its contract). Each is parked on `work.parked` unread, with `x-egress-parked-reason`."
});

export const inFlight = Metric.gauge("egress_consumer_in_flight", {
  description: "Actions a consumer currently has running, by consumer."
});

export const breakerState = Metric.gauge("egress_consumer_breaker_state", {
  description:
    "This replica's breaker for a dependency (0=closed 1=open 2=half-open), by dependency. Compare across instances — nothing here makes them agree."
});

export const breakerTrips = Metric.counter("egress_consumer_breaker_trips_total", {
  description: "Times this replica's breaker for a dependency has opened, by dependency."
});

export const permitLost = Metric.counter("egress_consumer_probe_permit_lost_total", {
  description:
    "Half-open probes this replica did not make because another replica held the dependency's one probe permit, by dependency. The message is released, not charged, and the breaker holds again at the same attempt."
});

export const redrives = Metric.counter("egress_consumer_redrives_total", {
  description:
    "Dead-lettered messages a consumer's redrive passes moved back onto its work queue (outcome=moved) or gave up on as poison (outcome=parked), by consumer. Nonzero only on the replica RabbitMQ elects active on that consumer's redrive-trigger queue."
});

export const concurrencyLimit = Metric.gauge("egress_consumer_concurrency_limit", {
  description:
    "How many actions a consumer currently lets itself run at once — the limit learned from its dependencies' throttled answers, never above MAX_IN_FLIGHT — by consumer."
});
