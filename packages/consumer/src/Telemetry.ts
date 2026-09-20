import { Metric } from "effect";

/**
 * Everything this fleet exposes to Prometheus. Deliberately two metrics, not
 * the dozen `@egress/rmq-consumer` grew once a control plane existed to react
 * to: there is no circuit state to publish, no target fraction, no floor —
 * only whether a call to the third party succeeded, and how many are in
 * flight right now.
 */

export const calls = Metric.counter("egress_consumer_calls_total", {
  description: "Calls made straight to the third party, by outcome (ok or failed).",
});

export const discarded = Metric.counter("egress_consumer_discarded_total", {
  description: "Deliveries this replica refused to spend a call on, by reason: `format` (a content type, encoding or message type it does not read) `malformed` (a body that does not decode as a work message) or `keyless` (no `message_id` to use as the idempotency key). Each goes to the dead-letter queue unread.",
});

export const inFlight = Metric.gauge("egress_consumer_in_flight", {
  description: "Third-party calls this daemon currently has open.",
});
