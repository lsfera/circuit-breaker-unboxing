import { Effect } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/http";
import { PrometheusMetrics } from "effect/observability";

/**
 * The `/metrics` every process here serves, from the same in-process `effect`
 * registry, in one place rather than three.
 *
 * The logger is disabled for it and that matters more than it looks: Prometheus
 * scrapes every 2s, and a daemon's log is what you actually read when one
 * misbehaves — an access log line per scrape buries the heartbeat that exists
 * precisely so a deaf daemon is visible.
 */
export const metricsResponse = PrometheusMetrics.format().pipe(
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
