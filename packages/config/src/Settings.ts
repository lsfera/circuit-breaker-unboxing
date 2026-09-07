import { Config, ConfigProvider, Effect, Result, Schema } from "effect";

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
const BrokerAddressFromString = Schema.TemplateLiteralParser([
  Schema.NonEmptyString,
  ":",
  Config.Port,
]);

export type BrokerAddress = { readonly host: string; readonly port: number };

export const brokerAddress = (name: string): Config.Config<BrokerAddress> =>
  Config.schema(BrokerAddressFromString, name).pipe(
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

/**
 * Read the settings, or exit 1. Eagerly, at module load: nothing should open a
 * broker connection and register consumers on configuration it could not read.
 */
export const load = <A>(
  processName: string,
  settings: Config.Config<A>,
  provider: ConfigProvider.ConfigProvider = ConfigProvider.fromEnv(),
): A =>
  Result.match(parse(settings, provider), {
    onSuccess: (value) => value,
    onFailure: (message) => {
      Effect.runSync(Effect.logFatal(`${processName}: ${message}`));
      return process.exit(1);
    },
  });
