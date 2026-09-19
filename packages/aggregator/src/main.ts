import { Config, Data, Deferred, Effect, Layer, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runAggregator } from "./aggregator.ts";

/**
 * The aggregator, its own component — single instance, no scaling, no HA.
 * Subscribes to every replica's `circuit.control` transitions and publishes
 * one verdict per apiId. Never in the request path: nothing here is called
 * by, or calls into, any consumer's own breaker decision.
 *
 *   node src/main.ts
 */

/** A share of the fleet, 0 to 1 inclusive — same shape as Tracing.ts's own sampling ratio. */
const Fraction = Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

const flags = {
  broker: brokerFlag("Broker to subscribe to circuit.control on"),
  verdictThreshold: Flag.Finite("verdict-threshold").pipe(
    Flag.withSchema(Fraction),
    Flag.withFallbackConfig(Config.schema(Fraction, "VERDICT_THRESHOLD")),
    Flag.withDefault(0.5),
    Flag.withDescription("Share of the known fleet reporting open/half_open before the verdict itself opens"),
  ),
  stalenessMs: Flag.Int("staleness-ms").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "STALENESS_MS")),
    Flag.withDefault(60_000),
    Flag.withDescription("How long a replica may go unheard-from before its last vote stops counting"),
  ),
  metricsPort: metricsPortFlag,
};

/** Why this process stopped, when it stops itself rather than losing the broker. */
class Fatal extends Data.TaggedError("Fatal")<{ readonly reason: string }> {
  override get message(): string {
    return this.reason;
  }
}

const aggregator = Command.make("aggregator", flags, (settings) => {
  const fatal = Deferred.makeUnsafe<never, Fatal>();
  const stop = (reason: string) => Effect.asVoid(Deferred.fail(fatal, new Fatal({ reason })));

  const Aggregator = Layer.effectDiscard(
    Effect.forkScoped(
      Effect.orDie(
        runAggregator({
          verdictThreshold: settings.verdictThreshold,
          stalenessMs: settings.stalenessMs,
        }),
      ).pipe(
        Effect.catchDefect((defect) =>
          Effect.logFatal("aggregator died, restarting the process", defect).pipe(
            Effect.andThen(stop("aggregator died")),
          ),
        ),
      ),
    ),
  );

  return launchWithRmq(
    HttpRouter.serve(Layer.provideMerge(Aggregator, MetricsRoute)).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
      Layer.provide(TracingLive("aggregator")),
      Layer.provideMerge(Rmq.layer(settings.broker)),
    ),
    Deferred.await(fatal),
  );
});

Command.run(aggregator, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
