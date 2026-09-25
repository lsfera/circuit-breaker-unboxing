// `Config` as `Flags`: @egress/domain exports the breaker's `Config` too.
import { Data, Deferred, Effect, Layer, Option as O, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { Rmq } from "@egress/rmq/Client.ts";
import { PositiveInt, rmqFlag, VERSION } from "@egress/config/Settings.ts";
import { Aggregator } from "./Aggregator.ts";
import { makeAmqpControlPlaneSink } from "./AmqpControlPlaneSink.ts";
import { HaSettings, InMemoryCoordinationLayer, RedisCoordinationLayer } from "./Coordination.ts";
import { InMemoryOutboxLayer, RedisOutboxLayer } from "./Outbox.ts";
import { combineSinks, EventBus, EventSink, makeWebhookSink, NoopSinkLayer } from "./Events.ts";
import { EnvoyFleetLayer, SimFleetLayer } from "./FleetSource.ts";
import { EnvoyPushFleetLayer } from "./EnvoyPushSource.ts";
import { HttpLive } from "./Http.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { ApiSpec } from "./FleetSource.ts";
import type { RedisLike } from "./Coordination.ts";

/** An unknown flag stops the process rather than being ignored (ADR 008). */
const flags = {
  port: Flag.Int("port").pipe(
    Flag.withDefault(8088),
    Flag.withDescription("Port for the console, API and /metrics"),
  ),
  source: Flag.Literals("source", ["sim", "envoy", "envoy-push"] as const).pipe(
    Flag.withDefault("sim" as const),
    Flag.withDescription("Where replica reports come from"),
  ),
  replicas: Flag.Int("replicas").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withDefault(5),
    Flag.withDescription("Simulated replica count (--source=sim only)"),
  ),
  apis: Flag.Int("apis").pipe(
    Flag.withSchema(Schema.Natural),
    Flag.withDefault(0),
    Flag.withDescription("Replace the three named APIs with N synthetic ones; 0 keeps them"),
  ),
  pushPort: Flag.Int("push-port").pipe(
    Flag.withDefault(9900),
    Flag.withDescription("Where Envoy pushes stats (--source=envoy-push)"),
  ),
  envoy: Flag.String("envoy").pipe(
    Flag.withDefault("http://127.0.0.1:9901"),
    Flag.withDescription("Comma-separated Envoy admin URLs (--source=envoy)"),
  ),
  /** Absent means "no control-plane sink", which is a different thing from a bad address. */
  rmq: rmqFlag.pipe(
    Flag.optional,
    Flag.withDescription("host:port of the broker to publish circuit.control to"),
  ),
  noWebhook: Flag.Boolean("no-webhook").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Drop the webhook sink, leaving only --rmq"),
  ),
  ha: Flag.Literals("ha", ["memory", "redis"] as const).pipe(
    Flag.withDefault("memory" as const),
    Flag.withDescription("Coordination backend for the publishing lease"),
  ),
  redis: Flag.String("redis").pipe(
    Flag.withDefault("redis://127.0.0.1:6379"),
    Flag.withDescription("Redis URL (--ha=redis)"),
  ),
  instanceId: Flag.String("instance-id").pipe(
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Identity in the lease; must differ between instances"),
  ),
  leaseTtlMs: Flag.Int("lease-ttl-ms").pipe(
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

/** `--apis=N`: N synthetic APIs, to measure the aggregator per API rather than a load generator. Sim only. */
const syntheticApis = (count: number): ReadonlyArray<ApiSpec> =>
  Array.from({ length: count }, (_, i) => ({
    apiId: `synthetic-${String(i).padStart(3, "0")}`,
    endpoints: 6,
    rps: 300,
    failureRate: 0,
  }));

/** Why this process stopped, when it stops itself. */
class Fatal extends Data.TaggedError("Fatal")<{ readonly reason: string }> {
  override get message(): string {
    return this.reason;
  }
}

const aggregator = Command.make("aggregator", flags, (settings) => {
  /**
   * A dead control loop or an unreachable broker ends the process through this:
   * a fiber forked into a scope cannot end `Layer.launch` (ADR 005).
   */
  const fatal = Deferred.makeUnsafe<never, Fatal>();
  const stop = (reason: string) => Effect.asVoid(Deferred.fail(fatal, new Fatal({ reason })));

  const APIS: ReadonlyArray<ApiSpec> =
    settings.apis > 0 ? syntheticApis(settings.apis) : NAMED_APIS;

  /** `sim`, `envoy` (polls `/stats`) or `envoy-push` (the gRPC sink, so unknown replicas still report). */
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

  /** `--rmq` adds the AMQP sink beside the webhook sink; `--no-webhook` drops the latter. */
  const webhookEnabled = !settings.noWebhook;
  const webhookUrl = `http://127.0.0.1:${settings.port}/subscriber/webhook`;

  // Provides its own Rmq, so both branches have RIn = never.
  const SinkLayer = O.match(settings.rmq, {
    onNone: () =>
      webhookEnabled ? Layer.effect(EventSink, makeWebhookSink(webhookUrl)) : NoopSinkLayer,
    onSome: (broker) =>
      Layer.effect(
        EventSink,
        Effect.gen(function* () {
          const amqp = yield* makeAmqpControlPlaneSink;
          const impls = [
            ...(webhookEnabled ? [yield* makeWebhookSink(webhookUrl)] : []),
            amqp,
          ];
          // Otherwise a dead broker is silent: 200s served, the fleet hears nothing.
          const rmq = yield* Rmq;
          yield* Effect.forkScoped(
            Effect.catch(rmq.lost, (error) =>
              Effect.logFatal(`${settings.instanceId}: control plane lost`, error).pipe(
                Effect.andThen(stop(`control plane lost: ${error.message}`)),
              ),
            ),
          );
          return impls.length === 1 ? impls[0]! : combineSinks(impls);
        }),
      ).pipe(Layer.provide(Rmq.layer(broker))),
  });

  /** Solo by default (in-memory, always wins its lease); `--ha=redis` for the compose pair. */
  const asRedisLike = (redis: Redis): RedisLike => ({
    eval: (script, { keys, args: evalArgs }) =>
      redis.eval(script, keys.length, ...keys, ...evalArgs) as Promise<string | number | null>,
  });

  const CoordinationLayer =
    settings.ha === "redis"
      ? Layer.unwrap(
          Effect.acquireRelease(
            Effect.sync(
              () =>
                new Redis(settings.redis, {
                  // Fail fast: ioredis's default retries turn an outage into a hung tick.
                  maxRetriesPerRequest: 1,
                  enableOfflineQueue: false,
                  connectTimeout: 1000,
                }),
            ),
            (redis) => Effect.promise(() => redis.quit().then(() => {}, () => {})),
          ).pipe(
            Effect.map((redis) => {
              // One connection for the lease and the outbox.
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

  const AppLayer = HttpLive.pipe(
    Layer.provideMerge(Aggregator.layer),
    // The sink layer sits *above* HaLayer rather than beside it: the webhook
    // sink needs the Outbox, which is part of the same durable state the lease
    // lives in.
    Layer.provideMerge(Layer.mergeAll(FleetLayer, EventBus.layer, SinkLayer)),
    Layer.provideMerge(HaLayer),
    Layer.provide(Layer.succeed(Config, defaultConfig)),
  );

  const AggregatorDaemon = Layer.effectDiscard(
    Effect.gen(function* () {
      const agg = yield* Aggregator;
      // A defect must end the process, not leave it serving frozen gauges.
      yield* Effect.forkScoped(
        agg.run.pipe(
          Effect.catchDefect((defect) =>
            Effect.logFatal(
              `${settings.instanceId}: control loop died, restarting the process`,
              defect,
            ).pipe(Effect.andThen(stop("control loop died"))),
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

  // `Layer.build` and then wait, rather than `Layer.launch`, which waits
  // forever: the difference is that this process can now end itself through
  // the ordinary failure path instead of calling `process.exit`.
  return Effect.scoped(
    Effect.flatMap(
      Layer.build(
        HttpRouter.serve(Layer.provideMerge(AggregatorDaemon, AppLayer)).pipe(
          Layer.provide(NodeHttpServer.layer(createServer, { port: settings.port })),
        ),
      ),
      () => Deferred.await(fatal),
    ),
  );
});

Command.run(aggregator, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
