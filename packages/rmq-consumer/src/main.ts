import { Config, Effect, Layer } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runDaemon } from "./daemon.ts";

/**
 * One daemon, one process:
 *
 *   node --experimental-strip-types src/main.ts
 *
 * Settings come from the environment because these are containers rather than
 * commands someone types; `DAEMON_INDEX` is the one value that differs between
 * otherwise identical daemon containers. Also serves `/metrics` from the same
 * in-process registry the aggregator uses, so the fleet and the circuit it
 * reacts to land on one dashboard.
 */

/**
 * Every setting this process takes, declared once.
 *
 * A flag with the environment behind it: containers set `FLEET_SIZE` and get
 * exactly what they always did, while `--fleet-size` works when someone runs
 * this by hand — and `--help` lists the lot, which is the part that was
 * missing. Reading main.ts was the documentation before.
 */
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
  metricsPort: metricsPortFlag,
};

/**
 * The graph is built *from* the settings, so it is built inside the command's
 * handler. Nothing below an unbuilt layer is built, so a value this process
 * cannot use still stops it before a socket is opened.
 *
 * The daemon itself runs as a scoped fiber for the lifetime of the server, the
 * same shape the aggregator's tick loop uses: interruption is structural, and
 * failing setup — a broker that never comes up, a queue redeclared with
 * different arguments — is a defect rather than something to recover from,
 * hence `orDie` and the restart policy on these containers.
 */
const daemon = Command.make("rmq-daemon", flags, (settings) => {
  const Daemon = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(runDaemon(settings))));

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
  );
});

Command.run(daemon, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
