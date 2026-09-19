import { Metric } from "effect";

/**
 * What this process exposes to Prometheus — two gauges, one series per
 * `apiId` (there's only ever one running today, but nothing here assumes
 * that). Contrast with `@egress/consumer`'s `breakerState`: that one is
 * intentionally never aggregated across replicas: this one is the
 * aggregation, published once, computed here and nowhere else.
 */

export const verdictState = Metric.gauge("egress_fleet_verdict_state", {
  description:
    "This aggregator's one published verdict per apiId (0=closed 1=open), folded from every replica's own reported breaker state. Not read back into any replica's own decision — see README.md.",
});

export const openFraction = Metric.gauge("egress_fleet_open_fraction", {
  description:
    "Share of the known (non-stale) fleet, per apiId, currently reporting open or half_open — the number verdictState's threshold is applied to.",
});

export const knownReplicas = Metric.gauge("egress_fleet_known_replicas", {
  description:
    "Size of the pruned registry openFraction's denominator is computed from, per apiId. The same fraction means something different as this moves — watch it alongside openFraction, not instead of it.",
});
