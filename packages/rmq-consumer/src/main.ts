import { Config, Effect, Layer, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { launchWithRmq, RmqLive } from "@egress/rmq/Client.ts";
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
  /**
   * `Natural`, not `PositiveInt`: this one is a 0-based position, and index 0
   * is the daemon that stays active longest. Below zero it is nothing.
   */
  index: Flag.Int("index").pipe(
    Flag.withSchema(Schema.Natural),
    Flag.withFallbackConfig(Config.schema(Schema.Natural, "DAEMON_INDEX")),
    Flag.withDefault(0),
    Flag.withDescription("0-based position in the fleet; differs per container"),
  ),
  /**
   * The value every daemon derives its own share of the work from. A fleet size
   * that is not a positive integer made `activeIndices` produce an empty set, so
   * every daemon idled while the queue filled — five containers up, healthy, and
   * doing nothing.
   */
  fleetSize: Flag.Int("fleet-size").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "FLEET_SIZE")),
    Flag.withDefault(5),
    Flag.withDescription("How many daemons the fleet has; every one must agree"),
  ),
  instanceId: Flag.String("instance-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("INSTANCE_ID")),
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Names this daemon's own control queue"),
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
  // A daemon is active when its index falls under the fleet's target, so an
  // index at or past `fleetSize` is never active — it starts, connects,
  // consumes nothing and reports healthy for as long as it runs. That is the
  // shape of a scale-up where `FLEET_SIZE` was raised on the new container and
  // not on the others, and it is silent, so it is refused here instead.
  if (settings.index >= settings.fleetSize) {
    return Effect.die(
      new Error(
        `--index ${settings.index} is outside a fleet of ${settings.fleetSize}: ` +
          `indices are 0-based, so the last one is ${settings.fleetSize - 1}. ` +
          `Scaling the fleet means raising FLEET_SIZE on every daemon, not only the new one.`,
      ),
    );
  }

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
      Layer.provideMerge(RmqLive(settings.broker)),
    ),
  );
});

Command.run(daemon, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
