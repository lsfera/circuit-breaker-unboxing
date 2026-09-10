import { test } from "node:test";
import assert from "node:assert/strict";
import { Config, ConfigProvider, Effect, Result, Schema } from "effect";
import { brokerAddress, PositiveInt } from "../src/Settings.ts";

/**
 * The rules a process's settings are read by, with no process.
 *
 * Every assertion here is a value that used to be accepted and turned into
 * `NaN`, a silent `false`, or a host of `""` — see the module doc for what
 * each one then did to a running fleet.
 */

const settings = Config.all({
  fleetSize: Config.schema(PositiveInt, "FLEET_SIZE").pipe(Config.withDefault(5)),
  index: Config.schema(Schema.Natural, "DAEMON_INDEX").pipe(Config.withDefault(0)),
  redriveOnClose: Config.boolean("REDRIVE_ON_CLOSE").pipe(Config.withDefault(false)),
  broker: brokerAddress("RMQ").pipe(Config.withDefault({ host: "127.0.0.1", port: 5672 })),
});

/** The parsed settings, or the message a failure would print. */
const read = (env: Record<string, string>) =>
  Result.match(
    Effect.runSync(Effect.result(settings.parse(ConfigProvider.fromEnvRecord(env)))),
    {
      onSuccess: (s) => s as Record<string, unknown> | string,
      onFailure: (error) => error.message,
    },
  );

test("an unset variable takes its default", () => {
  assert.deepEqual(read({}), {
    fleetSize: 5,
    index: 0,
    redriveOnClose: false,
    broker: { host: "127.0.0.1", port: 5672 },
  });
});

/**
 * The finding, in one assertion. `Number("five")` was `NaN`, and `NaN` reached
 * `activeIndices`, where `Array.from({ length: NaN })` is empty — so the whole
 * fleet idled and nothing said why.
 */
test("a count that is not a number is rejected, and the message names the variable", () => {
  const message = read({ FLEET_SIZE: "five" });
  assert.equal(typeof message, "string");
  assert.match(message as string, /FLEET_SIZE/);
});

/**
 * Zero is excluded on purpose: a capacity of zero is the same silent stall as
 * a `NaN` one, and it is far easier to type by accident.
 */
test("a capacity of zero is rejected, not treated as a valid ceiling", () => {
  assert.match(read({ FLEET_SIZE: "0" }) as string, /FLEET_SIZE/);
});

test("an index below zero is rejected", () => {
  assert.match(read({ DAEMON_INDEX: "-1" }) as string, /DAEMON_INDEX/);
});

/**
 * `=== "true"` meant an operator who wrote `REDRIVE_ON_CLOSE=1` got the
 * feature silently switched off, with the container reporting nothing.
 */
test("the boolean spellings an operator actually types all mean on", () => {
  for (const raw of ["true", "yes", "on", "1"]) {
    const s = read({ REDRIVE_ON_CLOSE: raw });
    assert.equal((s as { redriveOnClose: boolean }).redriveOnClose, true, raw);
  }
  for (const raw of ["false", "no", "off", "0"]) {
    const s = read({ REDRIVE_ON_CLOSE: raw });
    assert.equal((s as { redriveOnClose: boolean }).redriveOnClose, false, raw);
  }
});

test("a broker address decodes into a host and a port", () => {
  assert.deepEqual((read({ RMQ: "rabbitmq:5672" }) as { broker: unknown }).broker, {
    host: "rabbitmq",
    port: 5672,
  });
});

/**
 * `Number(port)` gave `NaN`, and `host ?? fallback` never fired for an empty
 * string — the fallback that looks like it protects the host did not.
 */
test("an address missing either half is rejected rather than half-guessed", () => {
  for (const raw of ["rabbitmq:abc", ":5672", "rabbitmq", "rabbitmq:", "rabbitmq:70000"]) {
    assert.match(read({ RMQ: raw }) as string, /RMQ/, raw);
  }
});
