import { Config, Effect, Layer } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { RmqLive } from "@egress/rmq/Client.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
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

/** Every setting this process takes, declared once — flags with the environment behind them. */
const flags = {
  broker: brokerFlag("Broker to publish work onto"),
  apiId: Flag.String("api-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Whose work queue to fill"),
  ),
  /**
   * Fixed, and deliberately never lowered in reaction to the circuit — the
   * backlog this builds during an outage is the thing the fleet has to survive.
   */
  ratePerSecond: Flag.Int("rate").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "RATE_PER_SECOND")),
    Flag.withDefault(200),
    Flag.withDescription("Messages published per second, regardless of circuit state"),
  ),
  metricsPort: metricsPortFlag,
};

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

Command.run(producer, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
