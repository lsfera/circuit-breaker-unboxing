import { Config, ConfigProvider, Data, Effect, Result, Schema } from "effect";

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

type BrokerAddress = { readonly host: string; readonly port: number };

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

/** `message` is the field `Error` reads, so the failure prints as itself. */
export class SettingsUnreadable extends Data.TaggedError("SettingsUnreadable")<{
  readonly message: string;
}> {}

/**
 * Read the settings as an Effect that fails, rather than a value that exits.
 *
 * Still fail-fast — a layer that cannot be built builds nothing below it, so
 * nothing opens a socket on configuration it could not read — but the failure
 * now travels the way every other startup failure does, reported by
 * `NodeRuntime.runMain` with a non-zero exit. Reading it at module load meant a
 * library module owned the process's fate, and importing it was enough to end
 * one.
 */
export const read = <A>(
  processName: string,
  settings: Config.Config<A>,
  provider: ConfigProvider.ConfigProvider = ConfigProvider.fromEnv(),
): Effect.Effect<A, SettingsUnreadable> =>
  Effect.suspend(() =>
    Result.match(parse(settings, provider), {
      onSuccess: (value) => Effect.succeed(value),
      onFailure: (message) =>
        Effect.fail(new SettingsUnreadable({ message: `${processName}: ${message}` })),
    }),
  );
