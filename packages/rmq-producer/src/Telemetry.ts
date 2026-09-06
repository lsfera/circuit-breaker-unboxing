import { Metric } from "effect";

/**
 * One counter, in its own file for the same reason the daemon fleet's metrics
 * are in theirs: every metric a component emits is declared in one place, so
 * "what does this expose" is a file rather than a grep.
 */
export const published = Metric.counter("egress_producer_published_total", {
  description: "Work messages published onto the work queue, by API.",
});
