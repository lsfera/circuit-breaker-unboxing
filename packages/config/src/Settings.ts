import { Config, ConfigProvider, Effect, Option as O, Result, Schema } from "effect";
import { Flag } from "effect/unstable/cli";

/**
 * Settings for every process here: one declaration, decoded at boot, and a
 * value the process cannot use stops it before it opens a socket.
 *
 * See docs/decisions/008-configuration-is-a-boundary.md for what each mistyped
 * variable used to do instead.
 */

/**
 * A capacity: fleet sizes, concurrency ceilings, rates. Zero is excluded rather
 * than left to `Natural`, because zero capacity stalls as silently as a `NaN`.
 */
export const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));

/** `host:port`, both halves required — a bare host is rejected rather than given a default port. */
export const BrokerAddress = Schema.TemplateLiteralParser([
  Schema.NonEmptyString,
  ":",
  Config.Port,
]);

type BrokerAddress = { readonly host: string; readonly port: number };

export const brokerAddress = (name: string): Config.Config<BrokerAddress> =>
  Config.schema(BrokerAddress, name).pipe(
    Config.map(([host, , port]) => ({ host, port })),
  );

/** Decode, or say which variable was wrong. Separate from `load` so the rules are testable. */
export const parse = <A>(
  settings: Config.Config<A>,
  provider: ConfigProvider.ConfigProvider,
): Result.Result<A, string> =>
  Result.match(Effect.runSync(Effect.result(settings.parse(provider))), {
    onSuccess: (value) => Result.succeed(value),
    onFailure: (error) => Result.fail(error.message),
  });

const decodeBroker = Schema.decodeUnknownOption(BrokerAddress);

/**
 * The two settings every process that speaks to the broker takes, declared here
 * rather than once per `main.ts`.
 *
 * Both were copied between the daemon and the producer, which meant the
 * *definition* of a broker address existed twice on the flag side while
 * `brokerAddress` above was its single definition on the environment side.
 */
export const brokerFlag = (description: string) =>
  Flag.string("rmq").pipe(
    Flag.filterMap(
      (raw) => O.map(decodeBroker(raw), ([host, , port]) => ({ host, port })),
      (raw) => `expected host:port, got ${raw}`,
    ),
    Flag.withFallbackConfig(brokerAddress("RMQ")),
    Flag.withDefault({ host: "127.0.0.1", port: 5672 }),
    Flag.withDescription(description),
  );

export const metricsPortFlag = Flag.integer("metrics-port").pipe(
  Flag.withFallbackConfig(Config.port("METRICS_PORT")),
  Flag.withDefault(9464),
  Flag.withDescription("Port /metrics is served on"),
);

