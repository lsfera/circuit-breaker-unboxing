import { Effect, Layer } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { RmqLive } from "@egress/rmq/Client.ts";
import { runProducer } from "./producer.ts";

/**
 * The producer, as its own component rather than a role inside the daemon
 * fleet's process.
 *
 *   node --experimental-strip-types src/main.ts
 *
 * It used to be `rmq-consumer/src/main.ts producer`, dispatched by an argv
 * switch next to the daemon. Same binary, two things that share nothing: the
 * producer has no control queue, no policy, no elections, and — the point of
 * the whole scenario — it never reads the circuit state. Keeping it in the
 * consumer package made that invisible, and made "what does a daemon depend
 * on" a question you had to answer by reading past a role check.
 *
 * Configuration comes from the environment rather than flags, because these
 * are containers rather than commands someone types — see `rmq-producer` in
 * docker-compose.yml.
 */

const env = (name: string, fallback: string) => process.env[name] ?? fallback;

const rmqAddr = env("RMQ", "127.0.0.1:5672");
const [rmqHost, rmqPort] = rmqAddr.split(":");
const connect = { host: rmqHost ?? "127.0.0.1", port: Number(rmqPort ?? 5672) };

const program = runProducer({
  apiId: env("API_ID", "payments-provider"),
  ratePerSecond: Number(env("RATE_PER_SECOND", "200")),
});

const METRICS_PORT = Number(env("METRICS_PORT", "9464"));

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
const Producer = Layer.effectDiscard(Effect.forkScoped(Effect.orDie(program)));

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(Producer, MetricsRoute).pipe(Layer.provide(RmqLive(connect))),
).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port: METRICS_PORT })));

NodeRuntime.runMain(Layer.launch(MainLayer));
