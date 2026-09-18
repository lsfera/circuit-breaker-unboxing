import { test } from "node:test";
import assert from "node:assert/strict";
import { decide } from "../src/consumer.ts";

test("an ok call is accepted", () => {
  assert.equal(decide("ok"), "accept");
});

test("a failed call is handed back to the broker, not retried here", () => {
  assert.equal(decide("failed"), "requeue");
});

test("a call rejected by this replica's own open breaker also requeues", () => {
  // Same settlement as "failed" — the difference is telemetry (no call was
  // attempted), not what happens to the message. See consumer.ts's comment
  // on CallOutcome for why that distinction lives upstream of this function.
  assert.equal(decide("open"), "requeue");
});
