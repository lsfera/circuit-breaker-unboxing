import {
  brokerFlag,
  connectionFlags,
  connectionOf,
  metricsFlag,
  metricsPortFlag,
  PositiveInt,
  setting,
  telemetryFlag
} from "@egress/config/Settings.ts";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import type { BindingArgs, DeliveryInfo, RmqConnectOptions } from "@egress/rmq/Client.ts";
import * as Contract from "@egress/rmq/Contract.ts";
import { MAX_DELAY_SECONDS } from "@egress/rmq/DelayedDelivery.ts";
import type { Negotiate } from "@egress/rmq/Negotiation.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { Config, Layer, Option as O, Predicate, Schema } from "effect";
import type { Effect } from "effect";
import { Command, Flag } from "effect/cli";
import { HttpRouter } from "effect/http";
import type { HttpServer } from "effect/http";
import { runApplication } from "./consumer.ts";
import type { ConsumerSpec } from "./consumer.ts";
import * as Dep from "./Dependency.ts";
import type { AnyDependency, Gated } from "./Dependency.ts";

/**
 * The consumer SDK: an application declares contracts, actions and dependencies; the SDK runs breakers, probe
 * permits, redrive, the adaptive limit, parking, metrics and tracing.
 *
 *   const payments = Consumer.For(Payment, negotiate).bind((payment, metadata) => …, [ThirdParty, Database]);
 *   run({ consumers: { "payments-provider": payments }, layer });   // `run` from `@egress/rmq-consumer/node`
 *
 * Nothing here names a runtime: what one provides (the `/metrics` server, the process's main) is a `Platform`,
 * and each runtime's entry point (`./node`) supplies it and runs the command.
 *
 * TODO: a Promise entry point (`./promise`) beside this one, for plain JavaScript, after a design pass on how a
 * dependency is described without Effect; and a built, publishable package. See docs/plain-js-sdk-plan.md.
 */

export type { BindingArgs, ExchangeOptions, RmqConnectOptions } from "@egress/rmq/Client.ts";
export type { Contract, Exchange, ExchangeInput, Route, RouteInput } from "@egress/rmq/Contract.ts";
export type { Declared, Negotiate, Parser } from "@egress/rmq/Negotiation.ts";
export { accept, bytes, text } from "@egress/rmq/Negotiation.ts";
export type { BinaryCodec } from "@egress/rmq/Negotiation.ts";
export type { BreakerPolicy, BreakerPolicyFactory, BreakerPolicyState, Outcome } from "./Breaker.ts";
export type { Verdict } from "./Dependency.ts";
export { Halted, Rejected } from "./Dependency.ts";

/** RabbitMQ's side of a delivery: `message_id`, `type`, content type and encoding, headers, delivery count, dead-letter origin, publish time. */
export type Metadata = DeliveryInfo;

/**
 * A dependency the action calls, with its own breaker, probe permit and timeout (2s unless given). Its breaker
 * follows the application's `BREAKER_*` settings except for what it sets itself (`breaker`). `breakerPolicy`
 * is required and creates an SDK `BreakerPolicy` for this dependency. Wrapping an
 * effect in it runs the effect under the timeout, judges its value or error with `classify`, tells this dependency's breaker,
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

type BreakerOf<D extends ReadonlyArray<AnyDependency>> = D[number] extends infer X
  ? (X extends Dep.Dependency<infer Name, any, any> ? Gated<Name> : never)
  : never;

type ServicesOf<C> = C extends Registration<infer R> ? R : never;

/**
 * How a consumer of a contract binds its `<key>.work` to the contract's exchange, when not as the contract says
 * (`Contract.binding`: its route if it has one, else the whole exchange). `routingKey` and `args`, the binding's
 * arguments a headers exchange matches on (`{ "x-match": "all", … }`), each replace the contract's when given.
 */
export type Binding = {
  readonly routingKey?: string;
  readonly args?: BindingArgs;
};

/**
 * A consumer of a `Contract`'s messages, read in its formats: the contract its publishers write with
 * (`@egress/rmq-producer`). Its `<key>.work` is bound to the contract's exchange, by `binding` when given. Or of
 * messages of a schema, read by a negotiation of its own and bound to nothing: what it reads is its decision, so
 * negotiation then has no default — `accept` covers media types, or any `Negotiate` will do.
 */
export function For<T>(contract: Contract.Contract<T>, options?: { readonly binding?: Binding; }): Binder<T>;
export function For<T>(message: Schema.Codec<T, any, never, unknown>, negotiate: Negotiate): Binder<T>;
export function For<T>(
  message: Contract.Contract<T> | Schema.Codec<T, any, never, unknown>,
  second?: Negotiate | { readonly binding?: Binding; }
): Binder<T> {
  if (!Contract.isContract(message)) return bindTo(message, second as Negotiate, O.none());
  const binding = (second as { readonly binding?: Binding; } | undefined)?.binding ?? {};
  const byContract = Contract.binding(message);
  return bindTo(
    message.schema,
    Contract.negotiate(message),
    O.some({
      exchange: message.exchange,
      routingKey: binding.routingKey ?? byContract.routingKey,
      args: binding.args ?? byContract.args
    })
  );
}

type Binder<T> = ReturnType<typeof bindTo<T>>;

const bindTo = <T>(
  message: Schema.Codec<T, any, never, unknown>,
  negotiate: Negotiate,
  source: ConsumerSpec["source"]
) => {
  const decode = Schema.decodeUnknownOption(message);
  return {
    /**
     * `dependencies` are the breakers this consumer answers to: any of them open stops it. A dependency the
     * action calls but this list omits leaves its breaker unprovided, which fails to compile at `run`.
     */
    bind: <Out, E, R, const D extends ReadonlyArray<AnyDependency>>(
      action: (payload: T, metadata: Metadata) => Effect.Effect<Out, E, R>,
      dependencies: D
    ): Registration<Exclude<R, BreakerOf<D>>> =>
      ({
        spec: (key: string): ConsumerSpec => ({
          key,
          negotiate,
          decode,
          action: action as ConsumerSpec["action"],
          dependencies,
          source
        })
      }) as Registration<Exclude<R, BreakerOf<D>>>
  };
};

// Strictly between: a decrease of 0 or 1 is a limit that collapses or never moves.
const OpenFraction = Schema.Finite.check(
  Schema.isBetween({ minimum: 0, maximum: 1, exclusiveMinimum: true, exclusiveMaximum: true })
);

/**
 * The SDK's own settings, each a flag with an environment fallback. An application adds its own beside them
 * (`run`'s `flags`); both appear in one `--help`, so a name here is taken.
 */
export const flags = {
  broker: brokerFlag("Broker to consume work from"),
  ...connectionFlags,
  maxInFlight: setting(Flag.Int("max-in-flight"), PositiveInt, "MAX_IN_FLIGHT").pipe(
    Flag.withDefault(20),
    Flag.withDescription("Concurrent actions each consumer allows itself")
  ),
  breakerInitialDelaySeconds: setting(
    Flag.Int("breaker-initial-delay-seconds"),
    PositiveInt,
    "BREAKER_INITIAL_DELAY_SECONDS"
  ).pipe(
    Flag.withDefault(1),
    Flag.withDescription(
      "First hold after a breaker opens, before its half-open probe, for a dependency that sets none of its own"
    )
  ),
  breakerMaxDelaySeconds: setting(
    Flag.Int("breaker-max-delay-seconds"),
    Dep.MaxDelaySeconds,
    "BREAKER_MAX_DELAY_SECONDS"
  ).pipe(
    Flag.withDefault(86_400),
    Flag.withDescription(
      `Ceiling a hold grows to, for a dependency that sets none of its own; the delay chain counts to ${MAX_DELAY_SECONDS}`
    )
  ),
  replicaId: Flag.String("replica-id").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("REPLICA_ID")),
    Flag.withDefault(crypto.randomUUID()),
    Flag.withDescription(
      "Names this replica's wake queues; unique per process start by default, so a token sent by a process that has since died reaches a queue nobody reads instead of waking its successor"
    )
  ),
  adaptiveLimit: Flag.Boolean("adaptive-limit").pipe(
    Flag.withFallbackConfig(Config.Boolean("ADAPTIVE_LIMIT")),
    Flag.withDefault(true),
    Flag.withDescription(
      "Shrink a consumer's concurrent-action limit when a dependency answers throttled and grow it back while it answers ok; off, throttled is a plain failure and the limit stays at MAX_IN_FLIGHT"
    )
  ),
  limitMin: setting(Flag.Int("limit-min"), PositiveInt, "LIMIT_MIN").pipe(
    Flag.withDefault(1),
    Flag.withDescription(
      "Floor the adaptive limit never goes below; setting it to MAX_IN_FLIGHT keeps throttled handling but stops the limit adapting"
    )
  ),
  limitDecrease: setting(Flag.Finite("limit-decrease"), OpenFraction, "LIMIT_DECREASE").pipe(
    Flag.withDefault(0.7),
    Flag.withDescription(
      "What the limit is multiplied by on a throttled answer (once per round trip, not once per answer)"
    )
  ),
  metricsPort: metricsPortFlag,
  metrics: metricsFlag,
  telemetry: telemetryFlag
};

/** What the SDK's flags decode to. */
export type Settings = Command.Command.Config.Infer<typeof flags>;

export type Application<C extends Record<string, Registration<any>>, F extends Command.Command.Config> = {
  /** A key names that consumer's queues: `<key>.work` and its dead-letter, parked and redrive-trigger queues. */
  readonly consumers: C;
  /** The application's own settings, parsed with the SDK's and listed in the same `--help`. */
  readonly flags?: F;
  /**
   * The connection beyond what the SDK's flags set (`--rmq`, `--rmq-vhost`, `--rmq-username`, `--rmq-password`,
   * `--rmq-tls`): certificates for TLS, a frame or channel limit, a different connection name. What it gives wins
   * over the flags; the function form reads the application's own settings.
   */
  readonly connection?:
    | Partial<RmqConnectOptions>
    | ((settings: Command.Command.Config.Infer<F>) => Partial<RmqConnectOptions>);
  /**
   * The services the actions need, built once at startup — from the application's settings, if it has any. If it
   * fails (a missing setting, a database that will not connect) the process stops before consuming.
   */
  readonly layer:
    | Layer.Layer<ServicesOf<C[keyof C]>, unknown>
    | ((settings: Command.Command.Config.Infer<F>) => Layer.Layer<ServicesOf<C[keyof C]>, unknown>);
};

/** What the runtime provides the SDK. */
export type Platform = {
  /** The server `/metrics` is served from, listening on `port`; built only when metrics are on. */
  readonly httpServer: (port: number) => Layer.Layer<HttpServer.HttpServer, unknown>;
};

/**
 * The application as a command, for one that wants to extend it — a description, subcommands — before running it
 * with a runtime's `launch`. `name` (default `consumer`) names the command, the tracing service, and the
 * dependencies' queues, which every consumer in the application shares.
 */
export const command = <const C extends Record<string, Registration<any>>, const F extends Command.Command.Config = {}>(
  app: Application<C, F>,
  platform: Platform,
  options: { readonly name?: string; } = {}
) => {
  const name = options.name ?? "consumer";
  const consumers = Object.entries(app.consumers).map(([key, registration]) => registration.spec(key));

  return Command.make(name, { sdk: flags, app: (app.flags ?? {}) as F }, ({ sdk: settings, app: own }) => {
    const mine = own as Command.Command.Config.Infer<F>;
    const services = Predicate.isFunction(app.layer) ? app.layer(mine) : app.layer;
    const connection = Predicate.isFunction(app.connection) ? app.connection(mine) : app.connection;
    const exposeMetrics = settings.metrics;
    const exposeTelemetry = settings.telemetry;
    return launchWithRmq(
      Layer.mergeAll(
        exposeMetrics
          ? HttpRouter.serve(MetricsRoute).pipe(Layer.provide(platform.httpServer(settings.metricsPort)))
          : Layer.empty,
        services
      ).pipe(
        Layer.provideMerge(exposeTelemetry ? TracingLive(name) : Layer.empty),
        Layer.provideMerge(Rmq.layer({ ...connectionOf(settings, name), ...connection }))
      ),
      runApplication({
        name,
        maxInFlight: settings.maxInFlight,
        replicaId: settings.replicaId,
        breaker: {
          initialDelaySeconds: settings.breakerInitialDelaySeconds,
          maxDelaySeconds: settings.breakerMaxDelaySeconds
        },
        limit: settings.adaptiveLimit
          ? O.some({
            min: Math.min(settings.limitMin, settings.maxInFlight),
            max: settings.maxInFlight,
            decrease: settings.limitDecrease
          })
          : O.none(),
        consumers
      })
    );
  });
};
