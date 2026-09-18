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

export const inFlight = Metric.gauge("egress_consumer_in_flight", {
  description: "Third-party calls this daemon currently has open.",
});
