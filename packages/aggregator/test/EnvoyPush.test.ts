import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { Cause, Effect, Exit, Layer } from "effect";
import { EnvoyPushFleetLayer } from "../src/EnvoyPushSource.ts";

test("a metrics port already taken fails startup with a typed error, not a defect", async () => {
  const taken = createServer();
  await new Promise<void>((resolve) => taken.listen(0, "0.0.0.0", resolve));
  const port = (taken.address() as AddressInfo).port;
  try {
    const exit = await Effect.runPromiseExit(Effect.scoped(Layer.build(EnvoyPushFleetLayer(port, []))));
    assert.ok(Exit.isFailure(exit));
    const failure = Cause.findErrorOption(exit.cause);
    assert.equal(failure._tag, "Some", `expected a failure, got ${Cause.pretty(exit.cause)}`);
    assert.equal((failure.value as { _tag: string })._tag, "MetricsSinkUnavailable");
  } finally {
    taken.close();
  }
});
