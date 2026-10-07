import {
  brokerFlag,
  connectionFlags,
  connectionOf,
  metricsFlag,
  metricsPortFlag,
  telemetryFlag
} from "@egress/config/Settings.ts";
import { launchWithRmq, Rmq } from "@egress/rmq/Client.ts";
import type { RmqConnectOptions } from "@egress/rmq/Client.ts";
import { MetricsRoute } from "@egress/tracing/Metrics.ts";
import { TracingLive } from "@egress/tracing/Tracing.ts";
import { Layer, Predicate } from "effect";
import type { Effect } from "effect";
import { Command } from "effect/cli";
import { HttpRouter } from "effect/http";
import type { HttpServer } from "effect/http";

/**
 * The publisher SDK: an application names the `Contract` its messages follow; the SDK declares the contract's
 * exchange, writes each message in the contract's format, stamps its `message_id`, and publishes to the exchange in
 * confirmed batches, traced and counted.
 *
 *   const payments = yield* Producer.publisher(Payment, { format: "application/json" });
 *   const id = yield* payments.publish(Producer.one({ apiId: "payments-provider", n: 0 }));
 *   const ids = yield* payments.publish(Producer.batch([{ apiId: "payments-provider", n: 1 }, …]));
 *   run({ main: (settings) => … });   // `run` from `@egress/rmq-producer/node`
 *
 * A publisher knows the exchange and no queue: each consumer binds its own (`@egress/rmq-consumer` binds
 * `<key>.work`). It shares the contract with the consumer SDK and nothing else, and never reads breaker state.
 * Nothing here names a runtime: what one provides (the `/metrics` server, the process's main) is a `Platform`, and
 * each runtime's entry point (`./node`) supplies it and runs the command.
 *
 * TODO: a Promise entry point (`./promise`) beside this one, for plain JavaScript, taking contracts from any
 * Standard Schema; and a built, publishable package. See docs/plain-js-sdk-plan.md.
 */

export type { ExchangeOptions, RmqConnectOptions } from "@egress/rmq/Client.ts";
export type { Contract, Exchange, ExchangeInput, Route, RouteInput } from "@egress/rmq/Contract.ts";
export { batch, BrokerFailed, ContractRefused, make as publisher, one, PublishError, Unroutable } from "./Publisher.ts";
export type { Batch, One, Publication, Publisher, PublisherOptions, Routing } from "./Publisher.ts";

/**
 * The SDK's own settings, each a flag with an environment fallback. An application adds its own beside them
 * (`run`'s `flags`); both appear in one `--help`, so a name here is taken.
 */
export const flags = {
  broker: brokerFlag("Broker to publish work onto"),
  ...connectionFlags,
  metricsPort: metricsPortFlag,
  metrics: metricsFlag,
  telemetry: telemetryFlag
};

/** What the SDK's flags decode to. */
export type Settings = Command.Command.Config.Infer<typeof flags>;

export type Application<F extends Command.Command.Config> = {
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
   * What the process does, given its settings: make its publishers and publish. It runs until it ends or fails, or
   * the broker connection is lost; any of these ends the process, for the container's restart policy.
   */
  readonly main: (settings: Command.Command.Config.Infer<F>) => Effect.Effect<unknown, unknown, Rmq>;
};

/** What the runtime provides the SDK. */
export type Platform = {
  /** The server `/metrics` is served from, listening on `port`; built only when metrics are on. */
  readonly httpServer: (port: number) => Layer.Layer<HttpServer.HttpServer, unknown>;
};

/**
 * The application as a command, for one that wants to extend it before running it with a runtime's `launch`.
 * `name` (default `producer`) names the command and the tracing service.
 */
export const command = <const F extends Command.Command.Config = {}>(
  app: Application<F>,
  platform: Platform,
  options: { readonly name?: string; } = {}
) => {
  const name = options.name ?? "producer";
  return Command.make(name, { sdk: flags, app: (app.flags ?? {}) as F }, ({ sdk: settings, app: own }) => {
    const mine = own as Command.Command.Config.Infer<F>;
    const connection = Predicate.isFunction(app.connection) ? app.connection(mine) : app.connection;
    return launchWithRmq(
      Layer.mergeAll(
        settings.metrics
          ? HttpRouter.serve(MetricsRoute).pipe(Layer.provide(platform.httpServer(settings.metricsPort)))
          : Layer.empty,
        settings.telemetry ? TracingLive(name) : Layer.empty
      ).pipe(Layer.provideMerge(Rmq.layer({ ...connectionOf(settings, name), ...connection }))),
      app.main(mine)
    );
  });
};
