import { Config, Data, Deferred, Effect, Layer, Option as O, Schema } from "effect";
import { Command, Flag } from "effect/cli";
import { HttpRouter } from "effect/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runDaemon } from "./daemon.ts";

/**
 * One daemon per process. Settings come from the environment (these are
 * containers), each also a flag; `/metrics` is served from the same registry.
 */

// Strictly between: a decrease of 0 or 1 is a limit that collapses or never moves.
const OpenFraction = Schema.Finite.check(
  Schema.isBetween({ minimum: 0, maximum: 1, exclusiveMinimum: true, exclusiveMaximum: true }),
);

const flags = {
  broker: brokerFlag("Broker to consume work and circuit.control from"),
  apiId: Flag.String("api-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Which API's work queue and control events this daemon follows"),
  ),
  instanceId: Flag.String("instance-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("INSTANCE_ID")),
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Names this daemon's control queue and fixes its share of the work"),
  ),
  egressAddr: Flag.String("egress-addr").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("EGRESS_ADDR")),
    Flag.withDefault("http://envoy:10000"),
    Flag.withDescription("One address; the daemon never learns Envoy is a fleet"),
  ),
  apiPath: Flag.String("api-path").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_PATH")),
    Flag.withDefault("/payments"),
    Flag.withDescription("Route on that address for this API"),
  ),
  /** Zero is excluded with everything else: the consumer would hold no deliveries. */
  maxInFlight: Flag.Int("max-in-flight").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "MAX_IN_FLIGHT")),
    Flag.withDefault(32),
    Flag.withDescription("Concurrent third-party calls, applied as the work consumer's prefetch"),
  ),
  redriveOnClose: Flag.Boolean("redrive-on-close").pipe(
    Flag.withFallbackConfig(Config.Boolean("REDRIVE_ON_CLOSE")),
    Flag.withDefault(false),
    Flag.withDescription("Replay the dead-letter queue when the circuit closes"),
  ),
  /** The bound a redrive pass respects. Unbounded, it republished two messages 17,703 times. */
  redriveMax: Flag.Int("redrive-max").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "REDRIVE_MAX")),
    Flag.withDefault(5000),
    Flag.withDescription("Messages moved per redrive pass"),
  ),
  adaptiveLimit: Flag.Boolean("adaptive-limit").pipe(
    Flag.withFallbackConfig(Config.Boolean("ADAPTIVE_LIMIT")),
    Flag.withDefault(true),
    Flag.withDescription(
      "Shrink this daemon's concurrent-call limit on a 429 and grow it back on a 200; off, the limit stays at MAX_IN_FLIGHT",
    ),
  ),
  limitMin: Flag.Int("limit-min").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "LIMIT_MIN")),
    Flag.withDefault(1),
    Flag.withDescription("Floor the adaptive limit never goes below"),
  ),
  limitDecrease: Flag.Finite("limit-decrease").pipe(
    Flag.withSchema(OpenFraction),
    Flag.withFallbackConfig(Config.schema(OpenFraction, "LIMIT_DECREASE")),
    Flag.withDefault(0.7),
    Flag.withDescription("What the limit is multiplied by on a 429 (once per round trip, not once per 429)"),
  ),
  metricsPort: metricsPortFlag,
};

/** Why this process stopped, when it stops itself rather than losing the broker. */
class Fatal extends Data.TaggedError("Fatal")<{ readonly reason: string }> {
  override get message(): string {
    return this.reason;
  }
}

/**
 * A setup failure (a broker that never comes up, a mismatched redeclare) is a
 * defect. Forked into the layer's scope it would be invisible to `Layer.launch`,
 * so it fails `fatal`, which `launchWithRmq` races against the connection (ADR 005).
 */
const daemon = Command.make("rmq-daemon", flags, (settings) => {
  const fatal = Deferred.makeUnsafe<never, Fatal>();
  const stop = (reason: string) => Effect.asVoid(Deferred.fail(fatal, new Fatal({ reason })));

  const Daemon = Layer.effectDiscard(
    Effect.forkScoped(
      Effect.orDie(
        runDaemon({
          ...settings,
          limit: settings.adaptiveLimit
            ? O.some({
                min: Math.min(settings.limitMin, settings.maxInFlight),
                max: settings.maxInFlight,
                decrease: settings.limitDecrease,
              })
            : O.none(),
        }),
      ).pipe(
        Effect.catchDefect((defect) =>
          Effect.logFatal("daemon died, restarting the process", defect).pipe(
            Effect.andThen(stop("daemon died")),
          ),
        ),
      ),
    ),
  );

  // `launchWithRmq`, not `Layer.launch`: a broker this process can no longer
  // reach ends it, and `provideMerge` is what keeps the one connection visible
  // to the launcher rather than sealed inside the graph.
  return launchWithRmq(
    HttpRouter.serve(Layer.provideMerge(Daemon, MetricsRoute)).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
      // All five daemons report as one service: which daemon is an attribute of
      // a span, not a different system.
      Layer.provide(TracingLive("rmq-daemon")),
      Layer.provideMerge(Rmq.layer(settings.broker)),
    ),
    Deferred.await(fatal),
  );
});

Command.run(daemon, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
