import { Config, Effect, Layer, Option as O, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { RmqLive } from "@egress/rmq/Client.ts";
import { BrokerAddress, brokerAddress, PositiveInt } from "@egress/config/Settings.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runProducer } from "./producer.ts";

const decodeBroker = Schema.decodeUnknownOption(BrokerAddress);

/**
 * The producer, its own component rather than a role inside the daemon process:
 *
 *   node --experimental-strip-types src/main.ts
 *
 * It shares nothing with the fleet — no control queue, no policy, no elections —
 * and never reads the circuit state, which is the point of the scenario.
 */

/** Every setting this process takes, declared once — flags with the environment behind them. */
const flags = {
  broker: Flag.string("rmq").pipe(
    Flag.filterMap(
      (raw) => O.map(decodeBroker(raw), ([host, , port]) => ({ host, port })),
      (raw) => `expected host:port, got ${raw}`,
    ),
    Flag.withFallbackConfig(brokerAddress("RMQ")),
    Flag.withDefault({ host: "127.0.0.1", port: 5672 }),
    Flag.withDescription("Broker to publish work onto"),
  ),
  apiId: Flag.string("api-id").pipe(
    Flag.withFallbackConfig(Config.nonEmptyString("API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Whose work queue to fill"),
  ),
  /**
   * Fixed, and deliberately never lowered in reaction to the circuit — the
   * backlog this builds during an outage is the thing the fleet has to survive.
   */
  ratePerSecond: Flag.integer("rate").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "RATE_PER_SECOND")),
    Flag.withDefault(200),
    Flag.withDescription("Messages published per second, regardless of circuit state"),
  ),
  metricsPort: Flag.integer("metrics-port").pipe(
    Flag.withFallbackConfig(Config.port("METRICS_PORT")),
    Flag.withDefault(9464),
    Flag.withDescription("Port /metrics is served on"),
  ),
};

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
const producer = Command.make("rmq-producer", flags, (settings) => {
  const Producer = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(runProducer(settings))));

  return Layer.launch(
    HttpRouter.serve(
      Layer.provideMerge(Producer, MetricsRoute).pipe(Layer.provide(RmqLive(settings.broker))),
    ).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
      // Every trace in this repo starts in this process. Without an OTLP
      // endpoint this installs no tracer at all — see @egress/tracing.
      Layer.provide(TracingLive("rmq-producer")),
    ),
  );
});

Command.run(producer, { version: "0.1.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
