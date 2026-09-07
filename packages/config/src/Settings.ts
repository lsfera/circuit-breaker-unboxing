import { Config, ConfigProvider, Effect, Result, Schema } from "effect";

/**
 * Configuration is a boundary, and it was the last one here still being read
 * by hand.
 *
 * Every process in this repo took its settings the same way:
 *
 *   const env = (name, fallback) => process.env[name] ?? fallback;
 *   fleetSize: Number(env("FLEET_SIZE", "5")),
 *
 * `Number("five")` is `NaN`, and nothing downstream is written to notice one.
 * Measured against the real pure code, a single mistyped variable does this:
 *
 *  - `FLEET_SIZE` — `activeIndices` computes `Array.from({ length: NaN })`,
 *    which is empty, so **every daemon idles forever** while the producer keeps
 *    filling the queue. Five containers up, healthy, consuming nothing. That is
 *    the "healthy-looking process can be a deaf one" failure this repo already
 *    has a finding about, reachable from one typo with no error anywhere.
 *  - `MAX_IN_FLIGHT` — the concurrency gate is `inFlight < maxInFlight`, false
 *    against `NaN`, so the first message waits on a permit that never comes.
 *  - `REDRIVE_MAX` — the bound is `moved >= maxPerPass`, false against `NaN`,
 *    so a redrive pass is unbounded. That bound exists because an unbounded
 *    pass was measured republishing the same two messages 17,703 times.
 *  - `RATE_PER_SECOND` — `Array.from({ length: NaN })` again: the producer
 *    publishes nothing, quietly.
 *
 * And the booleans were `env("REDRIVE_ON_CLOSE", "false") === "true"`, so
 * `1`, `yes`, `on` and `TRUE` all meant *off* without saying so.
 *
 * The rule is the one 007 applied to messages, applied to the environment: one
 * declaration per process, decoded through it, and a value the process cannot
 * use does not decode. `Config` reports the *name* of the offending variable,
 * which is the whole difference between this and a `NaN` — "Expected a value
 * greater than 0 at [\"FLEET_SIZE\"]" is a fix; an idle fleet is an
 * investigation.
 */

/**
 * A count that must be at least one: fleet sizes, concurrency ceilings, rates.
 *
 * Zero is excluded deliberately rather than left to `Natural`. Every one of
 * these is a *capacity*, and zero capacity is the same silent stall a `NaN`
 * produces — `MAX_IN_FLIGHT=0` deadlocks the gate exactly as `abc` does.
 */
export const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));

/**
 * `host:port`, declared rather than split — the same reason and the same shape
 * as the lease token's wire form in `Coordination.ts`.
 *
 * What it replaces got two things wrong quietly. `Number(port)` on
 * `RMQ=rabbitmq:abc` is `NaN`, which amqplib turns into a connection attempt
 * nobody can read the failure of; and `rmqHost ?? "127.0.0.1"` does not catch
 * `RMQ=:5672`, because an empty string is not nullish, so the fallback that
 * looks like it protects the host does not.
 *
 * Both halves are now required. A bare `RMQ=rabbitmq` used to mean "and
 * assume 5672", and assuming is what this pass is removing: every deployment
 * here already writes both, and the one that forgets should be told.
 */
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

/**
 * Decode one process's settings, or say which variable was wrong.
 *
 * Separate from `load` below so the rules are testable without a process to
 * exit — the failure is a string here, and the only thing `load` adds is what
 * to do with it.
 */
export const parse = <A>(
  settings: Config.Config<A>,
  provider: ConfigProvider.ConfigProvider,
): Result.Result<A, string> =>
  Result.match(Effect.runSync(Effect.result(settings.parse(provider))), {
    onSuccess: (value) => Result.succeed(value),
    onFailure: (error) => Result.fail(error.message),
  });

/**
 * Read the settings, or stop.
 *
 * Eagerly, at module load, before any layer is built: a process whose
 * configuration cannot be read has nothing useful to do, and the one thing it
 * must not do is open a broker connection, register consumers and then behave
 * oddly. Failing here means the container restart loop and the log line say
 * the same thing, which is what `restart: unless-stopped` needs to be
 * legible.
 */
export const load = <A>(
  processName: string,
  settings: Config.Config<A>,
  provider: ConfigProvider.ConfigProvider = ConfigProvider.fromEnv(),
): A =>
  Result.match(parse(settings, provider), {
    onSuccess: (value) => value,
    onFailure: (message) => {
      // Effect's logger rather than `console`, so this line looks like every
      // other line the process emits — it is the last thing it will say.
      Effect.runSync(Effect.logFatal(`${processName}: ${message}`));
      return process.exit(1);
    },
  });
