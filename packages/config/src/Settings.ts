import { Config, Option as O, Schema } from "effect";
import { Flag } from "effect/cli";
import manifest from "../../../package.json" with { type: "json" };

/** Settings shared by several processes, decoded at boot (ADR 008). */

export const VERSION: string = manifest.version;

/**
 * A capacity: fleet sizes, concurrency ceilings, rates. Zero is excluded rather
 * than left to `Natural`, because zero capacity stalls as silently as a `NaN`.
 */
export const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));

/** `Config.Port` is a reader; this version exports no port schema. */
const Port = Schema.Int.pipe(Schema.check(Schema.isBetween({ minimum: 1, maximum: 65535 })));

/** `host:port`, both halves required — a bare host is rejected rather than given a default port. */
const BrokerAddress = Schema.TemplateLiteralParser([Schema.NonEmptyString, ":", Port]);

type BrokerAddress = { readonly host: string; readonly port: number };

/** The environment side of the address. */
export const brokerAddress = (name: string): Config.Config<BrokerAddress> =>
  Config.schema(BrokerAddress, name).pipe(
    Config.map(([host, , port]) => ({ host, port })),
  );

const decodeBroker = Schema.decodeUnknownOption(BrokerAddress);

export const rmqFlag = Flag.String("rmq").pipe(
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

export const metricsPortFlag = Flag.Int("metrics-port").pipe(
  Flag.withFallbackConfig(Config.Port("METRICS_PORT")),
  Flag.withDefault(9464),
  Flag.withDescription("Port /metrics is served on"),
);
