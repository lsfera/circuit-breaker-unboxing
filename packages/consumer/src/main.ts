import { Config, Data, Deferred, Effect, Layer, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient, HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import { MAX_DELAY_SECONDS } from "@egress/rmq/DelayedDelivery.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runConsumer } from "./consumer.ts";

/**
 * The consumer daemon, its own component like `rmq-producer`:
 *
 *   node src/main.ts
 *
 * One address, no replica names — a client of whatever the third party
 * exposes, exactly like `producer.ts` is a client of the broker. Each
 * process gets its own circuit breaker (Breaker.ts) whose open state is a
 * message in the broker's delay chain; there is still no control queue, no
 * policy, no elections, because there is nothing yet to make five replicas'
 * breakers agree with each other.
 */

const flags = {
  broker: brokerFlag("Broker to consume work from"),
  apiId: Flag.String("api-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Whose work queue to drain"),
  ),
  egressAddr: Flag.String("egress-addr").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("EGRESS_ADDR")),
    Flag.withDefault("http://flaky-upstream:8080"),
    Flag.withDescription("The one address the third party is reached at"),
  ),
  apiPath: Flag.String("api-path").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("API_PATH")),
    Flag.withDefault("/payments"),
    Flag.withDescription("The route on egressAddr for this API"),
  ),
  maxInFlight: Flag.Int("max-in-flight").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "MAX_IN_FLIGHT")),
    Flag.withDefault(20),
    Flag.withDescription("Concurrent third-party calls this daemon allows itself"),
  ),
  breakerThreshold: Flag.Int("breaker-threshold").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "BREAKER_THRESHOLD")),
    Flag.withDefault(5),
    Flag.withDescription("Consecutive failures before this replica's breaker opens"),
  ),
  breakerInitialDelaySeconds: Flag.Int("breaker-initial-delay-seconds").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "BREAKER_INITIAL_DELAY_SECONDS")),
    Flag.withDefault(1),
    Flag.withDescription("First hold after the breaker opens, before its half-open probe"),
  ),
  breakerMaxDelaySeconds: Flag.Int("breaker-max-delay-seconds").pipe(
    Flag.withSchema(PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS))),
    Flag.withFallbackConfig(
      Config.schema(PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS)), "BREAKER_MAX_DELAY_SECONDS"),
    ),
    Flag.withDefault(86_400),
    Flag.withDescription(`Ceiling the hold grows to; the delay chain counts to ${MAX_DELAY_SECONDS}`),
  ),
  replicaId: Flag.String("replica-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("REPLICA_ID")),
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Names this replica's wake queue; unique per process start by default, so a token sent by a process that has since died reaches a queue nobody reads instead of waking its successor"),
  ),
  metricsPort: metricsPortFlag,
};

/** Why this process stopped, when it stops itself rather than losing the broker. */
class Fatal extends Data.TaggedError("Fatal")<{ readonly reason: string }> {
  override get message(): string {
    return this.reason;
  }
}

const consumer = Command.make("consumer", flags, (settings) => {
  const fatal = Deferred.makeUnsafe<never, Fatal>();
  const stop = (reason: string) => Effect.asVoid(Deferred.fail(fatal, new Fatal({ reason })));

  const consumerConfig = {
    apiId: settings.apiId,
    egressAddr: settings.egressAddr,
    apiPath: settings.apiPath,
    maxInFlight: settings.maxInFlight,
    replicaId: settings.replicaId,
    breaker: {
      consecutiveFailures: settings.breakerThreshold,
      initialDelaySeconds: settings.breakerInitialDelaySeconds,
      maxDelaySeconds: settings.breakerMaxDelaySeconds,
    },
  };

  const Consumer = Layer.effectDiscard(
    Effect.forkScoped(
      Effect.orDie(runConsumer(consumerConfig)).pipe(
        Effect.catchDefect((defect) =>
          Effect.logFatal("consumer died, restarting the process", defect).pipe(
            Effect.andThen(stop("consumer died")),
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer));

  return launchWithRmq(
    HttpRouter.serve(Layer.provideMerge(Consumer, MetricsRoute)).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
      Layer.provide(TracingLive("consumer")),
      Layer.provideMerge(Rmq.layer(settings.broker)),
    ),
    Deferred.await(fatal),
  );
});

Command.run(consumer, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
