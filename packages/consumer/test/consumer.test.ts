import { test } from "node:test";
import assert from "node:assert/strict";
import { decide } from "../src/consumer.ts";

test("an ok call is accepted", () => {
  assert.equal(decide("ok"), "accept");
});

test("a failed call is handed back to the broker, not retried here", () => {
  assert.equal(decide("failed"), "requeue");
});

test("a call rejected by this replica's own open breaker is released, not requeued", () => {
  // Uncounted: no call was ever attempted, so it shouldn't spend any of
  // x-delivery-limit's budget — see consumer.ts's comment on CallOutcome.
  assert.equal(decide("open"), "release");
});
