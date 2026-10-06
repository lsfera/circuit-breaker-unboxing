import { Config, Redacted, Schema } from "effect";
import { Flag } from "effect/cli";
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
    Flag.withDescription(description)
  );

/**
 * How to connect beyond where: the broker's vhost, credentials, and whether to use TLS, each a flag with an
 * environment fallback. RabbitMQ's own defaults (`/`, `guest`/`guest`, plain TCP) unless given; `guest` is refused
 * from anywhere but localhost unless the broker allows it, as the compose broker does.
 */
export const connectionFlags = {
  rmqVhost: Flag.String("rmq-vhost").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("RMQ_VHOST")),
    Flag.withDefault("/"),
    Flag.withDescription("The broker's virtual host")
  ),
  rmqUsername: Flag.String("rmq-username").pipe(
    Flag.withFallbackConfig(Config.NonEmptyString("RMQ_USERNAME")),
    Flag.withDefault("guest"),
    Flag.withDescription("The user to connect to the broker as")
  ),
  rmqPassword: Flag.Redacted("rmq-password").pipe(
    Flag.withFallbackConfig(Config.Redacted("RMQ_PASSWORD")),
    Flag.withDefault(Redacted.make("guest")),
    Flag.withDescription("That user's password")
  ),
  rmqTls: Flag.Boolean("rmq-tls").pipe(
    Flag.withFallbackConfig(Config.Boolean("RMQ_TLS")),
    Flag.withDefault(false),
    Flag.withDescription("Connect with TLS (amqps), trusting the runtime's CAs; RMQ should then name the TLS port")
  )
};

/** What `brokerFlag` and `connectionFlags` decode to, as connection options: a connection named `name`. */
export const connectionOf = (
  settings: {
    readonly broker: { readonly host: string; readonly port: number; };
    readonly rmqVhost: string;
    readonly rmqUsername: string;
    readonly rmqPassword: Redacted.Redacted;
    readonly rmqTls: boolean;
  },
  name: string
) => ({
  ...settings.broker,
  vhost: settings.rmqVhost,
  username: settings.rmqUsername,
  password: Redacted.value(settings.rmqPassword),
  tls: settings.rmqTls,
  name
});

export const metricsPortFlag = Flag.Int("metrics-port").pipe(
  Flag.withFallbackConfig(Config.Port("METRICS_PORT")),
  Flag.withDefault(9464),
  Flag.withDescription("Port /metrics is served on when metrics are enabled")
);

export const metricsFlag = Flag.Boolean("metrics").pipe(
  Flag.withFallbackConfig(Config.Boolean("EXPOSE_METRICS")),
  Flag.withFallbackConfig(Config.Boolean("METRICS")),
  Flag.withDefault(false),
  Flag.withDescription("Expose the /metrics endpoint")
);

export const telemetryFlag = Flag.Boolean("telemetry").pipe(
  Flag.withFallbackConfig(Config.Boolean("EXPOSE_TELEMETRY")),
  Flag.withFallbackConfig(Config.Boolean("TELEMETRY")),
  Flag.withDefault(false),
  Flag.withDescription("Enable OTLP tracing export")
);
