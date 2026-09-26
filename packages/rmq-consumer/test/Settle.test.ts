import { test } from "node:test";
import assert from "node:assert/strict";
import { Cause, Exit } from "effect";
import { Halted, Rejected } from "../src/Dependency.ts";
import type { Stop } from "../src/Dependency.ts";
import { decide, settle } from "../src/Settle.ts";

test("an ok answer is accepted", () => {
  assert.equal(decide("ok"), "accept");
});

test("a failed answer is handed back to the broker, not retried here", () => {
  assert.equal(decide("failed"), "requeue");
});

test("a throttled answer is released, not requeued, whatever the role or streak", () => {
  assert.equal(decide("throttled"), "release");
  assert.equal(decide("throttled", "probe"), "release");
  assert.equal(decide("throttled", "work", 5), "release");
});

test("a client error is parked at once, whatever the role or streak: a retry or a redrive gets the same answer", () => {
  assert.equal(decide("client_error"), "park");
  assert.equal(decide("client_error", "probe"), "park");
  assert.equal(decide("client_error", "work", 5), "park");
});

test("a failed probe is released, not charged to the message that happened to carry it", () => {
  assert.equal(decide("failed", "probe"), "release");
  assert.equal(decide("ok", "probe"), "accept");
  assert.equal(decide("failed", "work"), "requeue");
});

test("a failure that stands alone is charged; one that follows another is the dependency's, not the message's", () => {
  assert.equal(decide("failed", "work", 1), "requeue", "a poison message between successes still spends its budget");
  assert.equal(decide("failed", "work", 2), "release");
  assert.equal(decide("failed", "work", 5), "release");
  assert.equal(decide("ok", "work", 5), "accept");
});

const haltedAt = (stop: Stop, role: "work" | "probe" = "work", streak = 1) =>
  Exit.fail(new Halted({ dependency: "ledger", stop, reason: "why", role, streak }));

test("an action that completes is accepted", () => {
  assert.deepEqual(settle(Exit.succeed(42)), { disposition: "accept", reason: "ok" });
});

test("an action halted at a dependency settles by that dependency's answer, and parks with its name and reason", () => {
  assert.deepEqual(settle(haltedAt("client_error")), { disposition: "park", reason: "refused-ledger-why" });
  assert.equal(settle(haltedAt("failed", "work", 1)).disposition, "requeue");
  assert.equal(settle(haltedAt("failed", "work", 2)).disposition, "release");
  assert.equal(settle(haltedAt("failed", "probe", 1)).disposition, "release");
  assert.equal(settle(haltedAt("throttled")).disposition, "release");
  assert.equal(settle(haltedAt("ok")).disposition, "accept", "a failure the application classified as fine ends the message");
});

test("a halt that made no call — the breaker was open, or another replica held the probe permit — releases uncharged", () => {
  assert.deepEqual(settle(haltedAt("open")), { disposition: "release", reason: "open" });
  assert.deepEqual(settle(haltedAt("no-permit", "probe", 0)), { disposition: "release", reason: "no-permit" });
});

test("a message the action rejects is parked with the action's reason, whatever else", () => {
  assert.deepEqual(settle(Exit.fail(new Rejected({ reason: "keyless" }))), { disposition: "park", reason: "rejected-keyless" });
});

test("a failure outside any dependency is the application's bug: charged to the message, not parked", () => {
  assert.deepEqual(settle(Exit.fail(new Error("boom"))), { disposition: "requeue", reason: "unwrapped-error" });
  assert.deepEqual(settle(Exit.failCause(Cause.die("boom"))), { disposition: "requeue", reason: "defect" });
});
