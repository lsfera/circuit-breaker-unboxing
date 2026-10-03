import { Effect } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/http";
import { PrometheusMetrics } from "effect/observability";

/** Not access-logged: a line per 2s scrape buries the heartbeat that shows a deaf daemon. */
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
