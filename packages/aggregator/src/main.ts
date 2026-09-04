import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { Rmq, RmqLive } from "@egress/rmq/Client.ts";
import { Aggregator, AggregatorLayer } from "./Aggregator.ts";
import { makeAmqpControlPlaneSink } from "./AmqpControlPlaneSink.ts";
import { HaSettings, InMemoryCoordinationLayer, RedisCoordinationLayer } from "./Coordination.ts";
import { combineSinks, EventBusLayer, EventSink, makeWebhookSink, NoopSinkLayer } from "./Events.ts";
import { EnvoyFleetLayer, SimFleetLayer } from "./FleetSource.ts";
import { HttpLive } from "./Http.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { ApiSpec } from "./FleetSource.ts";
import type { RedisLike } from "./Coordination.ts";

const args = new Map<string, string>();
for (const arg of process.argv.slice(2)) {
  // Split on the *first* `=` only. `split("=")` with array destructuring
  // silently truncates any value that contains one — a Redis URL with a
  // password, most obviously — and the resulting connection string is wrong
  // in a way that looks like a typo rather than a parser bug.
  const raw = arg.replace(/^--/, "");
  const eq = raw.indexOf("=");
  const k = eq === -1 ? raw : raw.slice(0, eq);
  const v = eq === -1 ? "true" : raw.slice(eq + 1);
  if (k) args.set(k, v);
}

const PORT = Number(args.get("port") ?? 8088);
const MODE = args.get("source") ?? "sim";
const REPLICAS = Number(args.get("replicas") ?? 5);

const APIS: ReadonlyArray<ApiSpec> = [
  { apiId: "payments-provider", endpoints: 6, rps: 900, failureRate: 0 },
  { apiId: "shipping-rates", endpoints: 4, rps: 300, failureRate: 0 },
  { apiId: "tax-calc", endpoints: 3, rps: 120, failureRate: 0 },
];

const FleetLayer =
  MODE === "sim"
    ? SimFleetLayer(APIS, REPLICAS)
    : EnvoyFleetLayer(
        (args.get("envoy") ?? "http://127.0.0.1:9901")
          .split(",")
          .map((adminUrl, i) => ({
            replicaId: `envoy-${String(i).padStart(2, "0")}`,
            adminUrl: adminUrl.trim(),
          })),
        APIS,
      );

/**
 * `--rmq=<host>:<port>` mounts AmqpControlPlaneSink alongside (not instead
 * of) the webhook sink, so the existing subscriber/delivery-integrity demo
 * keeps working unchanged while the RabbitMQ daemon fleet in
 * @egress/rmq-consumer also gets circuit.control events. `--no-webhook`
 * still drops the webhook side if only the RMQ path is wanted.
 */
const rmqAddr = args.get("rmq");
const webhookEnabled = !args.has("no-webhook");
const webhookUrl = `http://127.0.0.1:${PORT}/subscriber/webhook`;

// Self-contained regardless of branch: when --rmq is set, this Layer
// provides its own Rmq dependency internally (Layer.provide, scoped to just
// this sink) rather than threading Rmq through the outer AppLayer graph, so
// the two branches below have the same RIn = never shape either way.
const SinkLayer = rmqAddr
  ? (() => {
      const [host, port] = rmqAddr.split(":");
      return Layer.effect(
        EventSink,
        Effect.gen(function* () {
          const impls = webhookEnabled ? [yield* makeWebhookSink(webhookUrl)] : [];
          impls.push(yield* makeAmqpControlPlaneSink);
          return impls.length === 1 ? impls[0]! : combineSinks(impls);
        }),
      ).pipe(Layer.provide(RmqLive({ host: host ?? "127.0.0.1", port: Number(port ?? 5672) })));
    })()
  : webhookEnabled
    ? Layer.effect(EventSink, makeWebhookSink(webhookUrl))
    : NoopSinkLayer;

/**
 * Solo by default: one instance that always wins its own lease. That is not
 * a special case of the HA machinery, it is what running it produces when
 * there is only one instance — the same InMemoryCoordinationLayer a test
 * uses to exercise real failover between two instances in one process.
 * `--ha=redis` swaps this for `RedisCoordinationLayer`, used by
 * docker-compose.yml's two real `aggregator`/`aggregator-2` instances
 * against one shared `redis` service — the same coordination logic, now
 * actually contended over by two processes instead of one.
 */
const instanceId = args.get("instance-id") ?? randomUUID();

const asRedisLike = (redis: Redis): RedisLike => ({
  eval: (script, { keys, args: evalArgs }) =>
    redis.eval(script, keys.length, ...keys, ...evalArgs) as Promise<string | number | null>,
});

/**
 * The connection is acquired inside the layer's scope rather than built at
 * module load, so it is closed when the application shuts down instead of
 * being left to the process exiting. Everything else in this repo that owns
 * a socket does the same (see @egress/rmq's `makeRmq`); a client constructed
 * at import time is the one place that quietly did not.
 */
const CoordinationLayer =
  args.get("ha") === "redis"
    ? Layer.unwrap(
        Effect.acquireRelease(
          Effect.sync(() => new Redis(args.get("redis") ?? "redis://127.0.0.1:6379")),
          (redis) => Effect.promise(() => redis.quit().then(() => {}, () => {})),
        ).pipe(Effect.map((redis) => RedisCoordinationLayer(asRedisLike(redis)))),
      )
    : InMemoryCoordinationLayer;

const HaLayer = Layer.mergeAll(
  CoordinationLayer,
  Layer.succeed(HaSettings, {
    instanceId,
    leaseTtlMs: Number(args.get("lease-ttl-ms") ?? 5000),
  }),
);

/**
 * The dependency graph, declared once. Layer.provideMerge keeps FleetSource,
 * EventBus and EventSink in the output context because the HTTP routes read
 * them directly.
 */
const AppLayer = HttpLive.pipe(
  Layer.provideMerge(AggregatorLayer),
  Layer.provideMerge(
    Layer.mergeAll(FleetLayer, EventBusLayer, SinkLayer, HaLayer),
  ),
  Layer.provide(Layer.succeed(Config, defaultConfig)),
);

/**
 * The aggregator loop runs as a scoped fiber for the lifetime of the server.
 * Interruption is structural: when the server layer shuts down, the scope
 * closes and the loop stops. No interval handle to remember.
 */
const AggregatorDaemon = Layer.effectDiscard(
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    yield* Effect.forkScoped(agg.run);
    yield* Effect.log(
      `egress circuit breaker console  source=${MODE}` +
        (MODE === "sim" ? ` replicas=${REPLICAS}` : "") +
        `  instance=${instanceId}  ha=${args.get("ha") ?? "memory"}` +
        (rmqAddr ? `  rmq=${rmqAddr}` : "") +
        `  http://localhost:${PORT}`,
    );
  }),
);

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(AggregatorDaemon, AppLayer),
).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port: PORT })));

NodeRuntime.runMain(Layer.launch(MainLayer));
