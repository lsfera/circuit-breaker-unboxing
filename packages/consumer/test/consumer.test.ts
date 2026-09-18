import { test } from "node:test";
import assert from "node:assert/strict";
import { decide } from "../src/consumer.ts";

test("an ok call is accepted", () => {
  assert.equal(decide("ok"), "accept");
});

test("a failed call is handed back to the broker, not retried here", () => {
  assert.equal(decide("failed"), "requeue");
});
