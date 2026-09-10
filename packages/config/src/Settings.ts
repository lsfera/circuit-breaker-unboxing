import { Config, Option as O, Schema } from "effect";
import { Flag } from "effect/unstable/cli";

/**
 * The settings more than one process here takes, declared once: one
 * declaration, decoded at boot, and a value the process cannot use stops it
 * before it opens a socket. See
 * docs/decisions/008-configuration-is-a-boundary.md.
 */

/**
 * A capacity: fleet sizes, concurrency ceilings, rates. Zero is excluded rather
 * than left to `Natural`, because zero capacity stalls as silently as a `NaN`.
 */
export const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));

/** `host:port`, both halves required — a bare host is rejected rather than given a default port. */
const BrokerAddress = Schema.TemplateLiteralParser([
  Schema.NonEmptyString,
  ":",
  Config.Port,
]);

type BrokerAddress = { readonly host: string; readonly port: number };

/** The environment side of the address. */
export const brokerAddress = (name: string): Config.Config<BrokerAddress> =>
  Config.schema(BrokerAddress, name).pipe(
    Config.map(([host, , port]) => ({ host, port })),
  );

const decodeBroker = Schema.decodeUnknownOption(BrokerAddress);

/**
 * The flag side of the same address, undecorated: the aggregator makes it
 * optional and everything else falls back to `RMQ`. Composing one declaration
 * is what keeps "what an address is" from being restated per entry point.
 */
export const rmqFlag = Flag.string("rmq").pipe(
  Flag.filterMap(
    (raw) => O.map(decodeBroker(raw), ([host, , port]) => ({ host, port })),
    (raw) => `expected host:port, got ${raw}`,
  ),
);

export const brokerFlag = (description: string) =>
  rmqFlag.pipe(
    Flag.withFallbackConfig(brokerAddress("RMQ")),
    Flag.withDefault({ host: "127.0.0.1", port: 5672 }),
    Flag.withDescription(description),
  );

export const metricsPortFlag = Flag.integer("metrics-port").pipe(
  Flag.withFallbackConfig(Config.port("METRICS_PORT")),
  Flag.withDefault(9464),
  Flag.withDescription("Port /metrics is served on"),
);
