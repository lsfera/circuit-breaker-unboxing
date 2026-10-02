import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { brokerFlag, metricsPortFlag, PositiveInt, setting, VERSION } from "@egress/config/Settings.ts";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { Config, Effect, Layer } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { createServer } from "node:http";
import { runProducer } from "./producer.ts";

/** The producer (`node src/main.ts`). It shares nothing with the fleet and never reads breaker state. */

/** Every setting this process takes, declared once — flags with the environment behind them. */
const flags = {
  broker: brokerFlag("Broker to publish work onto"),
  apiId: Flag.String("api-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Whose work queue to fill")
  ),
  /** Fixed, and deliberately never lowered in reaction to the third party: the backlog an outage builds is what the fleet has to survive. */
  ratePerSecond: setting(Flag.Int("rate"), PositiveInt, "RATE_PER_SECOND").pipe(
    Flag.withDefault(200),
    Flag.withDescription("Messages published per second, regardless of circuit state")
  ),
  format: Flag.Literals("format", ["json", "protobuf"]).pipe(
    Flag.withFallbackConfig(Config.Literals(["json", "protobuf"], "WORK_FORMAT")),
    Flag.withDefault("json"),
    Flag.withDescription(
      "How each body is written: JSON, or protobuf (`message Work { string api_id = 1; int64 n = 2; }`)"
    )
  ),
  metricsPort: metricsPortFlag
};

/** Failing setup (a broker that never comes up, a queue redeclared with different arguments) ends the process, for the container's restart policy. */
const producer = Command.make("rmq-producer", flags, (settings) =>
  launchWithRmq(
    HttpRouter.serve(MetricsRoute).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
      // Every trace starts in this process; without an OTLP endpoint no tracer is installed.
      Layer.provideMerge(TracingLive("rmq-producer")),
      Layer.provideMerge(Rmq.layer(settings.broker))
    ),
    runProducer(settings)
  ));

Command.run(producer, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain
);
