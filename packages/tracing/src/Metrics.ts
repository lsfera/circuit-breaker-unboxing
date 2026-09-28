import { Effect } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";

/**
 * The `/metrics` every process here serves, from the in-process `effect` registry. Its logger is disabled:
 * Prometheus scrapes every 2s, and an access-log line per scrape would bury the log a misbehaving daemon is read from.
 */
const metricsResponse = PrometheusMetrics.format().pipe(
  Effect.map((body) =>
    HttpServerResponse.text(body, {
      contentType: "text/plain; version=0.0.4; charset=utf-8",
    }),
  ),
  HttpMiddleware.withLoggerDisabled,
);

/** The same thing as a route, for a process whose HTTP surface is only this. */
export const MetricsRoute = HttpRouter.use((router) =>
  router.add("GET", "/metrics", metricsResponse),
);
