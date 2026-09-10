// `Config` is aliased to `Flags` because @egress/domain also exports a `Config`
// (the breaker's tuning knobs) and both belong in this file. `Flags` is also
// the honest name here: this process is configured by argv, not the
// environment — see the provider at the bottom of the settings block.
import { Effect, Layer, Option as O, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { RmqLive } from "@egress/rmq/Client.ts";
import { PositiveInt, rmqFlag } from "@egress/config/Settings.ts";
import { Aggregator, AggregatorLayer } from "./Aggregator.ts";
import { makeAmqpControlPlaneSink } from "./AmqpControlPlaneSink.ts";
import { HaSettings, InMemoryCoordinationLayer, RedisCoordinationLayer } from "./Coordination.ts";
import { InMemoryOutboxLayer, RedisOutboxLayer } from "./Outbox.ts";
import { combineSinks, EventBusLayer, EventSink, makeWebhookSink, NoopSinkLayer } from "./Events.ts";
import { EnvoyFleetLayer, SimFleetLayer } from "./FleetSource.ts";
import { EnvoyPushFleetLayer } from "./EnvoyPushSource.ts";
import { HttpLive } from "./Http.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { ApiSpec } from "./FleetSource.ts";
import type { RedisLike } from "./Coordination.ts";

/**
 * Every flag this process takes, declared once. A flag this process does not
 * take is refused rather than ignored — `--sorce=envoy-push` stops the process
 * instead of leaving `source` at `sim` and pointing the simulator at real
 * Envoy replicas. See docs/decisions/008-configuration-is-a-boundary.md.
 */
const flags = {
  port: Flag.integer("port").pipe(
    Flag.withDefault(8088),
    Flag.withDescription("Port for the console, API and /metrics"),
  ),
  source: Flag.choice("source", ["sim", "envoy", "envoy-push"] as const).pipe(
    Flag.withDefault("sim" as const),
    Flag.withDescription("Where replica reports come from"),
  ),
  replicas: Flag.integer("replicas").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withDefault(5),
    Flag.withDescription("Simulated replica count (--source=sim only)"),
  ),
  apis: Flag.integer("apis").pipe(
    Flag.withSchema(Schema.Natural),
    Flag.withDefault(0),
    Flag.withDescription("Replace the three named APIs with N synthetic ones; 0 keeps them"),
  ),
  pushPort: Flag.integer("push-port").pipe(
    Flag.withDefault(9900),
    Flag.withDescription("Where Envoy pushes stats (--source=envoy-push)"),
  ),
  envoy: Flag.string("envoy").pipe(
    Flag.withDefault("http://127.0.0.1:9901"),
    Flag.withDescription("Comma-separated Envoy admin URLs (--source=envoy)"),
  ),
  /** Absent means "no control-plane sink", which is a different thing from a bad address. */
  rmq: rmqFlag.pipe(
    Flag.optional,
    Flag.withDescription("host:port of the broker to publish circuit.control to"),
  ),
  noWebhook: Flag.boolean("no-webhook").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Drop the webhook sink, leaving only --rmq"),
  ),
  ha: Flag.choice("ha", ["memory", "redis"] as const).pipe(
    Flag.withDefault("memory" as const),
    Flag.withDescription("Coordination backend for the publishing lease"),
  ),
  redis: Flag.string("redis").pipe(
    Flag.withDefault("redis://127.0.0.1:6379"),
    Flag.withDescription("Redis URL (--ha=redis)"),
  ),
  instanceId: Flag.string("instance-id").pipe(
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Identity in the lease; must differ between instances"),
  ),
  leaseTtlMs: Flag.integer("lease-ttl-ms").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withDefault(5000),
    Flag.withDescription("How long a lease survives without renewal"),
  ),
};


const NAMED_APIS: ReadonlyArray<ApiSpec> = [
  { apiId: "payments-provider", endpoints: 6, rps: 900, failureRate: 0 },
  { apiId: "shipping-rates", endpoints: 4, rps: 300, failureRate: 0 },
  { apiId: "tax-calc", endpoints: 3, rps: 120, failureRate: 0 },
];

/**
 * `--apis=N` replaces the three named APIs with N synthetic ones, for finding
 * out what this costs at a size nobody has run it at.
 *
 * Synthetic rather than N more real Envoy clusters on purpose: the question is
 * what the *aggregator* costs per API per tick — polling or decoding, stepping
 * N breakers, publishing, checkpointing, and the metric cardinality that comes
 * with it — and standing up two hundred real upstreams to ask it would measure
 * the load generator instead. Only meaningful with `--source=sim`.
 */
const syntheticApis = (count: number): ReadonlyArray<ApiSpec> =>
  Array.from({ length: count }, (_, i) => ({
    apiId: `synthetic-${String(i).padStart(3, "0")}`,
    endpoints: 6,
    rps: 300,
    failureRate: 0,
  }));

/**
 * The graph is built *from* the flags, so it is built inside an Effect that
 * can fail. Nothing below an unbuilt layer is built, so a flag this process
 * cannot use still stops it before a socket is opened — what changed is that
 * the failure is reported by `runMain` like any other startup failure, rather
 * than by a library module calling `process.exit` while being imported.
 */
const aggregator = Command.make("aggregator", flags, (settings) => {
  const APIS: ReadonlyArray<ApiSpec> =
    settings.apis > 0 ? syntheticApis(settings.apis) : NAMED_APIS;

  /**
   * Three ingestion layers, one interface. `sim` simulates; `envoy` polls admin
   * `/stats`; `envoy-push` receives the gRPC sink, so a replica this process was
   * never told about still reports.
   *
   * Their detection latencies are tuned in different files: push is Envoy's
   * `stats_flush_interval`, poll is `tickMs`.
   */
  const FleetLayer =
    settings.source === "sim"
      ? SimFleetLayer(APIS, settings.replicas)
      : settings.source === "envoy-push"
        ? EnvoyPushFleetLayer(settings.pushPort, APIS)
        : EnvoyFleetLayer(
            settings.envoy
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
  const webhookEnabled = !settings.noWebhook;
  const webhookUrl = `http://127.0.0.1:${settings.port}/subscriber/webhook`;

  // Self-contained regardless of branch: when --rmq is set, this Layer
  // provides its own Rmq dependency internally (Layer.provide, scoped to just
  // this sink) rather than threading Rmq through the outer AppLayer graph, so
  // the two branches below have the same RIn = never shape either way.
  const SinkLayer = O.isSome(settings.rmq)
    ? Layer.effect(
        EventSink,
        Effect.gen(function* () {
          const impls = webhookEnabled ? [yield* makeWebhookSink(webhookUrl)] : [];
          impls.push(yield* makeAmqpControlPlaneSink);
          return impls.length === 1 ? impls[0]! : combineSinks(impls);
        }),
      ).pipe(Layer.provide(RmqLive(settings.rmq.value)))
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
    settings.ha === "redis"
      ? Layer.unwrap(
          Effect.acquireRelease(
            Effect.sync(
              () =>
                new Redis(settings.redis, {
                  // Fail fast rather than queue. ioredis defaults to holding
                  // commands in an offline queue and retrying a request across
                  // twenty reconnection attempts, which turns "the coordinator
                  // is unreachable" from an error into a hang — and a tick that
                  // hangs is a control loop that has stopped without saying so.
                  // Coordination.ts bounds this too, because the port must not
                  // depend on which client is behind it; this is the same
                  // decision made where the client actually is.
                  maxRetriesPerRequest: 1,
                  enableOfflineQueue: false,
                  connectTimeout: 1000,
                }),
            ),
            (redis) => Effect.promise(() => redis.quit().then(() => {}, () => {})),
          ).pipe(
            Effect.map((redis) => {
              // One connection, two stores. The lease and the outbox are the
              // same kind of state — small, durable, and only interesting to the
              // instance that holds the lease — so they share a client rather
              // than opening a second one to the same server.
              const like = asRedisLike(redis);
              return Layer.mergeAll(RedisCoordinationLayer(like), RedisOutboxLayer(like));
            }),
          ),
        )
      : Layer.mergeAll(InMemoryCoordinationLayer, InMemoryOutboxLayer);

  const HaLayer = Layer.mergeAll(
    CoordinationLayer,
    Layer.succeed(HaSettings, {
      instanceId: settings.instanceId,
      leaseTtlMs: settings.leaseTtlMs,
    }),
  );

  /**
   * The dependency graph, declared once. Layer.provideMerge keeps FleetSource,
   * EventBus and EventSink in the output context because the HTTP routes read
   * them directly.
   */
  const AppLayer = HttpLive.pipe(
    Layer.provideMerge(AggregatorLayer),
    // The sink layer sits *above* HaLayer rather than beside it: the webhook
    // sink needs the Outbox, which is part of the same durable state the lease
    // lives in.
    Layer.provideMerge(Layer.mergeAll(FleetLayer, EventBusLayer, SinkLayer)),
    Layer.provideMerge(HaLayer),
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
      /**
       * A coordination outage is handled inside the tick and the loop keeps
       * running. A *defect* is a bug, and the one thing it must not do is end
       * this fiber quietly: the HTTP server would carry on answering 200 with
       * whatever the gauges last held, which is indistinguishable from a system
       * where nothing is happening. Same stance as the daemon fleet's — crash
       * and let the restart policy do its job, rather than swallow it.
       */
      yield* Effect.forkScoped(
        agg.run.pipe(
          Effect.catchDefect((defect) =>
            Effect.logFatal(
              `${settings.instanceId}: control loop died, restarting the process`,
              defect,
            ).pipe(
              Effect.andThen(Effect.sync(() => process.exit(1))),
            ),
          ),
        ),
      );
      yield* Effect.log(
        `egress circuit breaker console  source=${settings.source}` +
          (settings.source === "sim" ? ` replicas=${settings.replicas}` : "") +
          `  instance=${settings.instanceId}  ha=${settings.ha}` +
          O.match(settings.rmq, {
            onNone: () => "",
            onSome: ({ host, port }) => `  rmq=${host}:${port}`,
          }) +
          `  http://localhost:${settings.port}`,
      );
    }),
  );

  return Layer.launch(
    HttpRouter.serve(Layer.provideMerge(AggregatorDaemon, AppLayer)).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.port })),
    ),
  );
});

Command.run(aggregator, { version: "0.1.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
