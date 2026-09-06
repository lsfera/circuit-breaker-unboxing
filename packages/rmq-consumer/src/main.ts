import { Effect, Layer } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { RmqLive } from "@egress/rmq/Client.ts";
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
 * It also serves `/metrics` on METRICS_PORT from the same in-process `effect`
 * registry @egress/aggregator uses, scraped by the same Prometheus. That is
 * what puts the fleet's behaviour on the same dashboard as the circuit it is
 * reacting to, instead of in a second tool on a second screen.
 */

const env = (name: string, fallback: string) => process.env[name] ?? fallback;

const rmqAddr = env("RMQ", "127.0.0.1:5672");
const [rmqHost, rmqPort] = rmqAddr.split(":");
const connect = { host: rmqHost ?? "127.0.0.1", port: Number(rmqPort ?? 5672) };

const apiId = env("API_ID", "payments-provider");

const program = runDaemon({
  apiId,
  index: Number(env("DAEMON_INDEX", "0")),
  fleetSize: Number(env("FLEET_SIZE", "5")),
  instanceId: env("INSTANCE_ID", randomUUID()),
  connect,
  // One address, no replica names — the same string a real client of this API
  // would be configured with.
  egressAddr: env("EGRESS_ADDR", "http://envoy:10000"),
  apiPath: env("API_PATH", "/payments"),
  maxInFlight: Number(env("MAX_IN_FLIGHT", "32")),
  // Off unless asked for: replaying work that failed during an outage is a
  // decision about this workload, not a property of the queue.
  redriveOnClose: env("REDRIVE_ON_CLOSE", "false") === "true",
  redriveMax: Number(env("REDRIVE_MAX", "5000")),
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
 * The daemon runs as a scoped fiber for the lifetime of the server, the same
 * shape the aggregator's tick loop uses: interruption is structural, and
 * failing setup (a broker that never comes up, a queue redeclared with
 * different arguments) is a defect rather than something to recover from —
 * hence `orDie`, and hence the restart policy on these containers rather
 * than a process sitting there half-wired.
 */
const Daemon = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(program)));

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(Daemon, MetricsRoute).pipe(Layer.provide(RmqLive(connect))),
).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port: METRICS_PORT })));

NodeRuntime.runMain(Layer.launch(MainLayer));
