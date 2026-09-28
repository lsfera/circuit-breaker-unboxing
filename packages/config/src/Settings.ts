import { Config, Schema } from "effect";
import { Flag } from "effect/unstable/cli";
import manifest from "../../../package.json" with { type: "json" };

/**
 * The settings more than one process here takes, declared once and decoded at boot: a value the process cannot
 * use stops it before it opens a socket.
 */

/** What every entry point reports for `--version`: the manifest's, not five string literals to keep in step. */
export const VERSION: string = manifest.version;

/**
 * A capacity: fleet sizes, concurrency ceilings, rates. Zero is excluded rather
 * than left to `Natural`, because zero capacity stalls as silently as a `NaN`.
 */
export const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));

/**
 * A flag with the environment behind it, both decoded by `schema`, so a bad value is refused whichever side it came
 * from and the message names that side.
 */
export const setting = <I, A>(flag: Flag.Flag<I>, schema: Schema.Codec<A, I>, env: string): Flag.Flag<A> =>
  flag.pipe(Flag.withSchema(schema), Flag.withFallbackConfig(Config.schema(schema, env)));

/** A port as a *schema*, for the template parser below: `Config.Port` is the config reader, not the schema it reads with. */
const Port = Schema.Int.pipe(Schema.check(Schema.isBetween({ minimum: 1, maximum: 65535 })));

/** `host:port`, both halves required — a bare host is rejected rather than given a default port. */
const BrokerAddress = Schema.TemplateLiteralParser([Schema.NonEmptyString, ":", Port]);

const toAddress = ([host, , port]: typeof BrokerAddress.Type) => ({ host, port });

export const brokerFlag = (description: string) =>
  setting(Flag.String("rmq"), BrokerAddress, "RMQ").pipe(
    Flag.map(toAddress),
    Flag.withDefault({ host: "127.0.0.1", port: 5672 }),
    Flag.withDescription(description),
  );

export const metricsPortFlag = Flag.Int("metrics-port").pipe(
  Flag.withFallbackConfig(Config.Port("METRICS_PORT")),
  Flag.withDefault(9464),
  Flag.withDescription("Port /metrics is served on"),
);
