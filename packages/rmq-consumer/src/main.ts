import { Config, Effect, Layer, Schema } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { RmqLive } from "@egress/rmq/Client.ts";
import { brokerAddress, load, PositiveInt } from "@egress/config/Settings.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runDaemon } from "./daemon.ts";

/**
 * One daemon, one process:
 *
 *   node --experimental-strip-types src/main.ts
 *
 * This used to dispatch on argv between `daemon` and `producer`. The producer
 * is its own component now (@egress/rmq-producer) because it shares nothing
 * with this one — no control queue, no policy, no elections, and it never
 * reads the circuit state, which is the entire point of the scenario.
 *
 * Everything comes from the environment rather than flags, because in this
 * repo these are containers rather than commands someone types — see the
 * rmq-daemon-* services in docker-compose.yml. DAEMON_INDEX is the one value
 * that differs between otherwise identical daemon containers.
 *
 * Declared once and decoded at boot rather than read with `Number(env(...))`,
 * which is what this used to do. A daemon is the worst place in this repo for
 * a `NaN`: it does not crash, it idles. See @egress/config for what each
 * mistyped variable was measured doing.
 *
 * It also serves `/metrics` on METRICS_PORT from the same in-process `effect`
 * registry @egress/aggregator uses, scraped by the same Prometheus. That is
 * what puts the fleet's behaviour on the same dashboard as the circuit it is
 * reacting to, instead of in a second tool on a second screen.
 */

const settings = load(
  "rmq-daemon",
  Config.all({
    broker: brokerAddress("RMQ").pipe(Config.withDefault({ host: "127.0.0.1", port: 5672 })),
    apiId: Config.nonEmptyString("API_ID").pipe(Config.withDefault("payments-provider")),
    /**
     * `Natural`, not `PositiveInt`: this one is a 0-based position, and index
     * 0 is the daemon that stays active longest. Below zero it is nothing.
     */
    index: Config.schema(Schema.Natural, "DAEMON_INDEX").pipe(Config.withDefault(0)),
    /**
     * The value every daemon derives its own share of the work from. A fleet
     * size that is not a positive integer made `activeIndices` produce an
     * empty set, so every daemon idled while the queue filled — five
     * containers up, healthy, and doing nothing.
     */
    fleetSize: Config.schema(PositiveInt, "FLEET_SIZE").pipe(Config.withDefault(5)),
    instanceId: Config.nonEmptyString("INSTANCE_ID").pipe(Config.withDefault(randomUUID())),
    // One address, no replica names — the same string a real client of this API
    // would be configured with.
    egressAddr: Config.nonEmptyString("EGRESS_ADDR").pipe(
      Config.withDefault("http://envoy:10000"),
    ),
    apiPath: Config.nonEmptyString("API_PATH").pipe(Config.withDefault("/payments")),
    /** Zero is excluded with everything else: the gate would never admit a call. */
    maxInFlight: Config.schema(PositiveInt, "MAX_IN_FLIGHT").pipe(Config.withDefault(32)),
    // Off unless asked for: replaying work that failed during an outage is a
    // decision about this workload, not a property of the queue. `1`, `yes` and
    // `on` turn it on too — the old `=== "true"` silently did not.
    redriveOnClose: Config.boolean("REDRIVE_ON_CLOSE").pipe(Config.withDefault(false)),
    /** The bound a redrive pass respects. Unbounded, it republished two messages 17,703 times. */
    redriveMax: Config.schema(PositiveInt, "REDRIVE_MAX").pipe(Config.withDefault(5000)),
    metricsPort: Config.port("METRICS_PORT").pipe(Config.withDefault(9464)),
  }),
);

const program = runDaemon(settings);

/**
 * Identical to @egress/aggregator's `/metrics` route, deliberately: one
 * registry, one exposition format, nothing extra to keep in sync.
 *
 * The logger is disabled for it, and that matters more here than it looks.
 * Prometheus scrapes every 2s, and a daemon's log is the thing you actually
 * read when one of them misbehaves — an access log line per scrape would
 * bury the heartbeat that exists precisely so a deaf daemon is visible.
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
 * The daemon runs as a scoped fiber for the lifetime of the server, the same
 * shape the aggregator's tick loop uses: interruption is structural, and
 * failing setup (a broker that never comes up, a queue redeclared with
 * different arguments) is a defect rather than something to recover from —
 * hence `orDie`, and hence the restart policy on these containers rather
 * than a process sitting there half-wired.
 */
const Daemon = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(program)));

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(Daemon, MetricsRoute).pipe(Layer.provide(RmqLive(settings.broker))),
).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
  // All five daemons report as one service: which daemon is an attribute of a
  // span, not a different system.
  Layer.provide(TracingLive("rmq-daemon")),
);

NodeRuntime.runMain(Layer.launch(MainLayer));
