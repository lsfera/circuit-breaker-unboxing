import { Effect, Layer } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { RmqLive } from "@egress/rmq/Client.ts";
import { runDaemon } from "./daemon.ts";
import { runProducer } from "./producer.ts";

/**
 * Role dispatch for the RabbitMQ side of the scenario:
 *
 *   node --experimental-strip-types src/main.ts daemon
 *   node --experimental-strip-types src/main.ts producer
 *
 * Everything else comes from the environment rather than flags, because in
 * this repo these are containers, not commands someone types — see the
 * rmq-daemon-* / rmq-producer services in docker-compose.yml. DAEMON_INDEX
 * is the one value that differs between the otherwise identical daemon
 * containers.
 *
 * Either role also serves `/metrics` on METRICS_PORT, read from the same
 * in-process `effect` Metric registry @egress/aggregator uses, and scraped
 * by the same Prometheus. That is what puts the fleet's behaviour on the
 * same dashboard as the circuit it is reacting to, instead of in a second
 * tool on a second screen.
 */

const role = process.argv[2] ?? "daemon";

const env = (name: string, fallback: string) => process.env[name] ?? fallback;

const rmqAddr = env("RMQ", "127.0.0.1:5672");
const [rmqHost, rmqPort] = rmqAddr.split(":");
const connect = { host: rmqHost ?? "127.0.0.1", port: Number(rmqPort ?? 5672) };

const apiId = env("API_ID", "payments-provider");

const program =
  role === "producer"
    ? runProducer({
        apiId,
        ratePerSecond: Number(env("RATE_PER_SECOND", "200")),
      })
    : runDaemon({
        apiId,
        index: Number(env("DAEMON_INDEX", "0")),
        fleetSize: Number(env("FLEET_SIZE", "5")),
        instanceId: env("INSTANCE_ID", randomUUID()),
        connect,
        // One address, no replica names — the same string a real client of
        // this API would be configured with.
        egressAddr: env("EGRESS_ADDR", "http://envoy:10000"),
        apiPath: env("API_PATH", "/payments"),
        maxInFlight: Number(env("MAX_IN_FLIGHT", "32")),
        // Off unless asked for: replaying work that failed during an outage
        // is a decision about this workload, not a property of the queue.
        redriveOnClose: env("REDRIVE_ON_CLOSE", "false") === "true",
        redriveMax: Number(env("REDRIVE_MAX", "5000")),
      });

if (role !== "producer" && role !== "daemon") {
  console.error(`unknown role "${role}" — expected "daemon" or "producer"`);
  process.exit(1);
}

/**
 * Containment for exactly one library-level race, and nothing else.
 *
 * rhea throws `transfer after detach` synchronously from inside a socket
 * data callback when the broker's frames arrive for a link that has just
 * gone away. `daemon.ts` avoids provoking it (see the two-close comment in
 * `probeOnce`), but a connection torn down while frames are in flight can
 * still hit it, and the throw is unreachable from here: the client creates
 * a private rhea container per connection and never exposes it, so there is
 * no `error` listener to attach. Left alone it kills the process.
 *
 * Every other uncaught exception is still fatal, on purpose — a daemon that
 * swallows its own bugs is worse than one that restarts. That trade only
 * holds because the container actually does restart: see
 * `restart: unless-stopped` on the rmq-* services in docker-compose.yml,
 * without which "fatal" just means "gone".
 */
process.on("uncaughtException", (error) => {
  if (error instanceof Error && error.message === "transfer after detach") {
    console.warn(`[${role}] ignored rhea race: transfer after detach`);
    return;
  }
  throw error;
});

const METRICS_PORT = Number(env("METRICS_PORT", "9464"));

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
 * The role runs as a scoped fiber for the lifetime of the server, the same
 * shape the aggregator's tick loop uses: interruption is structural, and
 * failing setup (a broker that never comes up, a queue redeclared with
 * different arguments) is a defect rather than something to recover from —
 * hence `orDie`, and hence the restart policy on these containers rather
 * than a process sitting there half-wired.
 */
const RoleDaemon = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(program)));

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(RoleDaemon, MetricsRoute).pipe(Layer.provide(RmqLive(connect))),
).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port: METRICS_PORT })));

NodeRuntime.runMain(Layer.launch(MainLayer));
