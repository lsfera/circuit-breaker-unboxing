import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, Result } from "effect";
import { Flag } from "effect/cli";
import assert from "node:assert/strict";
import { test } from "vitest";
import { brokerFlag, metricsFlag, metricsPortFlag, PositiveInt, setting, telemetryFlag } from "../src/Settings.ts";

/**
 * The settings every process takes, read the way `Command.run` reads them: the command line first, the environment
 * behind it. Each rejection is a value that used to be accepted and turned into `NaN` or a host of `""`.
 */

/** The parsed value, or the message a failure would print. */
const read = <A>(flag: Flag.Flag<A>, env: Record<string, string>, flags: Record<string, ReadonlyArray<string>> = {}) =>
  Effect.runPromise(
    Effect.result(flag.parse({ flags, arguments: [] })).pipe(
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
      Effect.provide(NodeServices.layer)
    )
  ).then(Result.match({ onSuccess: ([, value]) => value as A | string, onFailure: (error) => error.message }));

const broker = brokerFlag("test");
const rate = setting(Flag.Int("rate"), PositiveInt, "RATE_PER_SECOND");

test("an unset broker address takes its default", async () => {
  assert.deepEqual(await read(broker, {}), { host: "127.0.0.1", port: 5672 });
});

test("a broker address decodes into a host and a port, from the environment or the flag", async () => {
  assert.deepEqual(await read(broker, { RMQ: "rabbitmq:5672" }), { host: "rabbitmq", port: 5672 });
  assert.deepEqual(await read(broker, { RMQ: "rabbitmq:5672" }, { rmq: ["other:5673"] }), {
    host: "other",
    port: 5673
  });
});

/**
 * `Number(port)` gave `NaN`, and `host ?? fallback` never fired for an empty string: the fallback that looks like
 * it protects the host did not.
 */
test("an address missing either half is rejected rather than half-guessed, and the message names its source", async () => {
  for (const raw of ["rabbitmq:abc", ":5672", "rabbitmq", "rabbitmq:", "rabbitmq:70000"]) {
    assert.match(String(await read(broker, { RMQ: raw })), /RMQ/, raw);
    assert.match(String(await read(broker, {}, { rmq: [raw] })), /rmq/, raw);
  }
});

/** The finding, in one assertion: `Number("five")` was `NaN`, which silently stalls anything sized by it. */
test("a capacity that is not a number is rejected, and the message names its source", async () => {
  assert.match(String(await read(rate, { RATE_PER_SECOND: "five" })), /RATE_PER_SECOND/);
  assert.match(String(await read(rate, {}, { rate: ["five"] })), /rate/);
});

/** Zero is excluded on purpose: a capacity of zero is the same silent stall as a `NaN` one, and easier to type. */
test("a capacity of zero is rejected, not treated as a valid ceiling", async () => {
  assert.match(String(await read(rate, { RATE_PER_SECOND: "0" })), /RATE_PER_SECOND/);
  assert.match(String(await read(rate, {}, { rate: ["0"] })), /rate/);
  assert.equal(await read(rate, { RATE_PER_SECOND: "200" }), 200);
});

test("the metrics port defaults, reads the environment, and refuses what is not a port", async () => {
  assert.equal(await read(metricsPortFlag, {}), 9464);
  assert.equal(await read(metricsPortFlag, { METRICS_PORT: "9500" }), 9500);
  for (const raw of ["0", "70000", "abc"]) {
    assert.match(String(await read(metricsPortFlag, { METRICS_PORT: raw })), /METRICS_PORT/, raw);
  }
});

test("metrics are disabled by default and can be enabled by flag or env", async () => {
  assert.equal(await read(metricsFlag, {}), false);
  assert.equal(await read(metricsFlag, { EXPOSE_METRICS: "true" }), true);
  assert.equal(await read(metricsFlag, { METRICS: "true" }), true);
  assert.equal(await read(metricsFlag, {}, { metrics: ["true"] }), true);
});

test("telemetry is disabled by default and can be enabled by flag or env", async () => {
  assert.equal(await read(telemetryFlag, {}), false);
  assert.equal(await read(telemetryFlag, { EXPOSE_TELEMETRY: "true" }), true);
  assert.equal(await read(telemetryFlag, { TELEMETRY: "true" }), true);
  assert.equal(await read(telemetryFlag, {}, { telemetry: ["true"] }), true);
});
