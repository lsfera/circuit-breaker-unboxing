import { Metric } from "effect";

/**
 * What the publisher SDK exposes to Prometheus: only what RabbitMQ cannot report. Messages published, confirmed and
 * returned unroutable are the broker's to count, per exchange (`rabbitmq_detailed_exchange_messages_*_total`, the
 * `exchange_metrics` family); a message refused before it was sent, or lost with its channel, never reached it.
 */

export const failed = Metric.counter("egress_producer_failed_total", {
  description:
    "Messages in publications that failed where the broker cannot count them, by exchange and reason: contract_refused (the contract refused a message, so none of the publication was sent) or broker_failed (nacked, or lost with the channel before its confirm). Unroutable messages are the broker's: rabbitmq_detailed_exchange_messages_unroutable_returned_total."
});
