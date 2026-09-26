import { Config, Data, Deferred, Effect, Layer, Option as O, Predicate, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import type { DeliveryInfo } from "@egress/rmq/Client.ts";
import { MAX_DELAY_SECONDS } from "@egress/rmq/DelayedDelivery.ts";
import { brokerFlag, metricsPortFlag, PositiveInt, VERSION } from "@egress/config/Settings.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { runApplication } from "./consumer.ts";
import type { ConsumerSpec } from "./consumer.ts";
import * as Dep from "./Dependency.ts";
import type { AnyDependency, Gated } from "./Dependency.ts";
import type { Negotiate } from "./Negotiation.ts";

/**
 * The consumer SDK. An application declares its contracts, what it does with each message, and the dependencies
 * that work calls; the SDK runs the rest — one breaker per dependency whose open state lives in RabbitMQ, the
 * fleet's probe permits, the redrive of dead letters, a concurrency limit learned from `throttled` answers, parking,
 * metrics and tracing:
 *
 *   const payments = Consumer.For(Payment, negotiate).bind((payment, metadata) => …, [ThirdParty, Database]);
 *   Consumer.run({ consumers: { "payments-provider": payments }, layer });
 */

export type { Outcome } from "./Breaker.ts";
export type { Declared, Negotiate, Parser } from "./Negotiation.ts";
export { accept } from "./Negotiation.ts";
export type { Verdict } from "./Dependency.ts";
export { Halted, Rejected } from "./Dependency.ts";

/** RabbitMQ's side of a delivery: `message_id`, `type`, content type and encoding, headers, delivery count, dead-letter origin, publish time. */
export type Metadata = DeliveryInfo;

/**
 * A dependency the action calls, with its own breaker, probe permit and timeout (2s unless given). Its breaker
 * follows the application's `BREAKER_*` settings except for what it sets itself (`breaker`). Wrapping an
 * effect in it runs the effect under the timeout, judges its Exit with `classify`, tells this dependency's breaker,
 * and halts the action on anything but `ok`. Its name labels metrics and names its queues.
 */
export const Dependency = Dep.make;
export type Dependency<Name extends string, A, E> = Dep.Dependency<Name, A, E>;
export type { BreakerOverrides } from "./Dependency.ts";

declare const RegistrationTypeId: unique symbol;

/** A contract, how it is read, what is done with each message, and the breakers that gate it. Needs `R` from the application. */
export interface Registration<R> {
  readonly [RegistrationTypeId]: () => R;
  readonly spec: (key: string) => ConsumerSpec;
}

type BreakerOf<D extends ReadonlyArray<AnyDependency>> =
  D[number] extends infer X ? (X extends Dep.Dependency<infer Name, any, any> ? Gated<Name> : never) : never;

type ServicesOf<C> = C extends Registration<infer R> ? R : never;

/**
 * A consumer of messages of `message`. What it reads is its decision, so negotiation has no default: `accept`
 * covers media types, or any `Negotiate` will do.
 */
export const For = <T>(message: Schema.Codec<T, any, never, unknown>, negotiate: Negotiate) => {
  const decode = Schema.decodeUnknownOption(message);
  return {
    /**
     * `dependencies` are the breakers this consumer answers to: any of them open stops it. A dependency the
     * action calls but this list omits leaves its breaker unprovided, which fails to compile at `run`.
     */
    bind: <Out, E, R, const D extends ReadonlyArray<AnyDependency>>(
      action: (payload: T, metadata: Metadata) => Effect.Effect<Out, E, R>,
      dependencies: D,
    ): Registration<Exclude<R, BreakerOf<D>>> =>
      ({
        spec: (key: string): ConsumerSpec => ({
          key,
          negotiate,
          decode,
          action: action as ConsumerSpec["action"],
          dependencies,
        }),
      }) as Registration<Exclude<R, BreakerOf<D>>>,
  };
};

// Strictly between: a decrease of 0 or 1 is a limit that collapses or never moves.
const OpenFraction = Schema.Finite.check(
  Schema.isBetween({ minimum: 0, maximum: 1, exclusiveMinimum: true, exclusiveMaximum: true }),
);

/**
 * The SDK's own settings, each a flag with an environment fallback. An application adds its own beside them
 * (`run`'s `flags`); both appear in one `--help`, so a name here is taken.
 */
export const flags = {
  broker: brokerFlag("Broker to consume work from"),
  maxInFlight: Flag.Int("max-in-flight").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "MAX_IN_FLIGHT")),
    Flag.withDefault(20),
    Flag.withDescription("Concurrent actions each consumer allows itself"),
  ),
  breakerThreshold: Flag.Int("breaker-threshold").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "BREAKER_THRESHOLD")),
    Flag.withDefault(5),
    Flag.withDescription("Consecutive failures at a dependency before its breaker opens, for a dependency that sets none of its own"),
  ),
  breakerInitialDelaySeconds: Flag.Int("breaker-initial-delay-seconds").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "BREAKER_INITIAL_DELAY_SECONDS")),
    Flag.withDefault(1),
    Flag.withDescription("First hold after a breaker opens, before its half-open probe, for a dependency that sets none of its own"),
  ),
  breakerMaxDelaySeconds: Flag.Int("breaker-max-delay-seconds").pipe(
    Flag.withSchema(PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS))),
    Flag.withFallbackConfig(
      Config.schema(PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS)), "BREAKER_MAX_DELAY_SECONDS"),
    ),
    Flag.withDefault(86_400),
    Flag.withDescription(`Ceiling a hold grows to, for a dependency that sets none of its own; the delay chain counts to ${MAX_DELAY_SECONDS}`),
  ),
  replicaId: Flag.String("replica-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("REPLICA_ID")),
    Flag.withDefault(randomUUID()),
    Flag.withDescription("Names this replica's wake queues; unique per process start by default, so a token sent by a process that has since died reaches a queue nobody reads instead of waking its successor"),
  ),
  adaptiveLimit: Flag.Boolean("adaptive-limit").pipe(
    Flag.withFallbackConfig(Config.Boolean("ADAPTIVE_LIMIT")),
    Flag.withDefault(true),
    Flag.withDescription(
      "Shrink a consumer's concurrent-action limit when a dependency answers throttled and grow it back while it answers ok; off, throttled is a plain failure and the limit stays at MAX_IN_FLIGHT",
    ),
  ),
  limitMin: Flag.Int("limit-min").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withFallbackConfig(Config.schema(PositiveInt, "LIMIT_MIN")),
    Flag.withDefault(1),
    Flag.withDescription("Floor the adaptive limit never goes below; setting it to MAX_IN_FLIGHT keeps throttled handling but stops the limit adapting"),
  ),
  limitDecrease: Flag.Finite("limit-decrease").pipe(
    Flag.withSchema(OpenFraction),
    Flag.withFallbackConfig(Config.schema(OpenFraction, "LIMIT_DECREASE")),
    Flag.withDefault(0.7),
    Flag.withDescription("What the limit is multiplied by on a throttled answer (once per round trip, not once per answer)"),
  ),
  metricsPort: metricsPortFlag,
};

/** Why this process stopped, when it stops itself rather than losing the broker. */
class Fatal extends Data.TaggedError("Fatal")<{ readonly reason: string }> {
  override get message(): string {
    return this.reason;
  }
}

/** What the SDK's flags decode to. */
export type Settings = Command.Command.Config.Infer<typeof flags>;

export type Application<C extends Record<string, Registration<any>>, F extends Command.Command.Config> = {
  /** A key names that consumer's queues: `<key>.work` and its dead-letter, parked and redrive-trigger queues. */
  readonly consumers: C;
  /** The application's own settings, parsed with the SDK's and listed in the same `--help`. */
  readonly flags?: F;
  /**
   * The services the actions need, built once at startup — from the application's settings, if it has any. If it
   * fails (a missing setting, a database that will not connect) the process stops before consuming.
   */
  readonly layer:
    | Layer.Layer<ServicesOf<C[keyof C]>, unknown>
    | ((settings: Command.Command.Config.Infer<F>) => Layer.Layer<ServicesOf<C[keyof C]>, unknown>);
};

/**
 * The application as a command, for one that wants to extend it — a description, subcommands — before running it
 * with `launch`. `name` (default `consumer`) names the command, the tracing service, and the dependencies' queues,
 * which every consumer in the application shares.
 */
export const command = <const C extends Record<string, Registration<any>>, const F extends Command.Command.Config = {}>(
  app: Application<C, F>,
  options: { readonly name?: string } = {},
) => {
  const name = options.name ?? "consumer";
  const consumers = Object.entries(app.consumers).map(([key, registration]) => registration.spec(key));

  return Command.make(name, { sdk: flags, app: (app.flags ?? {}) as F }, ({ sdk: settings, app: own }) => {
    const fatal = Deferred.makeUnsafe<never, Fatal>();
    const stop = (reason: string) => Effect.asVoid(Deferred.fail(fatal, new Fatal({ reason })));
    const services = Predicate.isFunction(app.layer) ? app.layer(own as Command.Command.Config.Infer<F>) : app.layer;

    const Application = Layer.effectDiscard(
      Effect.forkScoped(
        Effect.orDie(
          runApplication({
            name,
            maxInFlight: settings.maxInFlight,
            replicaId: settings.replicaId,
            breaker: {
              consecutiveFailures: settings.breakerThreshold,
              initialDelaySeconds: settings.breakerInitialDelaySeconds,
              maxDelaySeconds: settings.breakerMaxDelaySeconds,
            },
            limit: settings.adaptiveLimit
              ? O.some({
                  min: Math.min(settings.limitMin, settings.maxInFlight),
                  max: settings.maxInFlight,
                  decrease: settings.limitDecrease,
                })
              : O.none(),
            consumers,
          }),
        ).pipe(
          Effect.catchDefect((defect) =>
            Effect.logFatal(`${name} died, restarting the process`, defect).pipe(
              Effect.andThen(stop(`${name} died`)),
            ),
          ),
        ),
      ),
    ).pipe(Layer.provide(services as Layer.Layer<any, unknown>));

    return launchWithRmq(
      HttpRouter.serve(Layer.provideMerge(Application, MetricsRoute)).pipe(
        Layer.provide(NodeHttpServer.layer(createServer, { port: settings.metricsPort })),
        Layer.provide(TracingLive(name)),
        Layer.provideMerge(Rmq.layer(settings.broker)),
      ),
      Deferred.await(fatal),
    );
  });
};

/** Run a command built by `command`, extended or not, as this process's main. */
export const launch = (cmd: Command.Command<string, any, any, unknown, NodeServices.NodeServices>): void =>
  Command.run(cmd, { version: VERSION }).pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain);

/** Run every consumer of one application in this process, until the broker is lost or a breaker cannot operate. */
export const run = <const C extends Record<string, Registration<any>>, const F extends Command.Command.Config = {}>(
  app: Application<C, F>,
  options: { readonly name?: string } = {},
): void => launch(command(app, options));
