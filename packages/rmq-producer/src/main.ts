import { Config, Effect, Layer } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { RmqLive } from "@egress/rmq/Client.ts";
import { brokerAddress, load, PositiveInt } from "@egress/config/Settings.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runProducer } from "./producer.ts";

/**
 * The producer, its own component rather than a role inside the daemon process:
 *
 *   node --experimental-strip-types src/main.ts
 *
 * It shares nothing with the fleet — no control queue, no policy, no elections —
 * and never reads the circuit state, which is the point of the scenario.
 */

const settings = load(
  "rmq-producer",
  Config.all({
    broker: brokerAddress("RMQ").pipe(Config.withDefault({ host: "127.0.0.1", port: 5672 })),
    apiId: Config.nonEmptyString("API_ID").pipe(Config.withDefault("payments-provider")),
    /**
     * Fixed, and deliberately never lowered in reaction to the circuit — the
     * backlog this builds during an outage is the thing the fleet has to
     * survive.
     */
    ratePerSecond: Config.schema(PositiveInt, "RATE_PER_SECOND").pipe(Config.withDefault(200)),
    metricsPort: Config.port("METRICS_PORT").pipe(Config.withDefault(9464)),
  }),
);

const program = runProducer(settings);

/**
 * Identical to the aggregator's and the daemon's `/metrics` route: one
 * registry, one exposition format, nothing extra to keep in sync. The logger
 * is disabled for it because Prometheus scrapes every 2s and an access log
 * line per scrape buries everything worth reading.
 */
const MetricsRoute = HttpRouter.use((router) =>
  router.add(
    "GET",
    "/metrics",
    PrometheusMetrics.format().pipe(
      Effect.map((body) =>
        HttpServerResponse.text(body, {
          contentType: "text/plain; version=0.0.4; charset=utf-8",
        }),
      ),
      HttpMiddleware.withLoggerDisabled,
    ),
  ),
);

/**
 * Scoped fiber for the lifetime of the server, the same shape the aggregator's
 * tick loop and the daemon use. Failing setup — a broker that never comes up, a
 * queue redeclared with different arguments — is a defect rather than something
 * to recover from, hence `orDie` and the restart policy on the container.
 */
const Producer = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(program)));

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(Producer, MetricsRoute).pipe(Layer.provide(RmqLive(settings.broker))),
).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
  // Every trace in this repo starts in this process. Without an OTLP endpoint
  // this installs no tracer at all — see @egress/tracing.
  Layer.provide(TracingLive("rmq-producer")),
);

NodeRuntime.runMain(Layer.launch(MainLayer));
