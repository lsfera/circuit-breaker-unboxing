import { test } from "node:test";
import assert from "node:assert/strict";
import { classify, nextAttempt } from "../src/Attempts.ts";
import { WORK_DELIVERY_LIMIT } from "@egress/rmq/ControlPlane.ts";

/**
 * The daemon's retry decision, with no broker, no fetch and no clock in it —
 * outcome and attempts header in, what to do about the delivery out.
 */

test("a successful call is accepted, whatever the attempts header says", () => {
  assert.deepEqual(nextAttempt("ok", undefined), { _tag: "accept" });
  assert.deepEqual(nextAttempt("ok", "2"), { _tag: "accept" });
});

test("a shed call is released, uncounted, whatever the attempts header says", () => {
  assert.deepEqual(nextAttempt("shed", undefined), { _tag: "release" });
  assert.deepEqual(nextAttempt("shed", "2"), { _tag: "release" });
});

test("a first failure republishes to work with attempts=1", () => {
  assert.deepEqual(nextAttempt("failed", undefined), {
    _tag: "republish",
    destination: "work",
    attempts: 1,
  });
});

test("a second failure republishes to work with attempts=2", () => {
  assert.deepEqual(nextAttempt("failed", "1"), {
    _tag: "republish",
    destination: "work",
    attempts: 2,
  });
});

/**
 * The attempt that would exceed WORK_DELIVERY_LIMIT goes to the dead-letter
 * queue instead — three calls per outage, spent by republishing rather than
 * by broker redeliveries now, but the same budget.
 */
test("the failure that reaches WORK_DELIVERY_LIMIT dead-letters instead of retrying again", () => {
  assert.deepEqual(nextAttempt("failed", String(WORK_DELIVERY_LIMIT - 1)), {
    _tag: "republish",
    destination: "dead",
    attempts: WORK_DELIVERY_LIMIT,
  });
});

test("further failures past the limit keep dead-lettering", () => {
  assert.deepEqual(nextAttempt("failed", String(WORK_DELIVERY_LIMIT + 4)), {
    _tag: "republish",
    destination: "dead",
    attempts: WORK_DELIVERY_LIMIT + 5,
  });
});

/**
 * There is no way to publish anything but a digit string here — the header a
 * malformed one is treated as zero, the same stance Redrive.ts's
 * `nextRedrive` takes on its own count header, rather than trusted as a
 * signal to dead-letter immediately.
 */
test("a header that doesn't parse is treated as zero, not trusted", () => {
  assert.deepEqual(nextAttempt("failed", "not-a-number"), {
    _tag: "republish",
    destination: "work",
    attempts: 1,
  });
});

test("statuses are sorted: 2xx ok, 429 shed, other 4xx refused, 408 and 5xx and no response failed", () => {
  assert.equal(classify(200), "ok");
  assert.equal(classify(204), "ok");
  assert.equal(classify(429), "shed");
  assert.equal(classify(400), "refused");
  assert.equal(classify(404), "refused");
  assert.equal(classify(409), "refused");
  assert.equal(classify(408), "failed", "a timeout on the third party's side is worth another try");
  assert.equal(classify(500), "failed");
  assert.equal(classify(503), "failed");
  assert.equal(classify(504), "failed");
  assert.equal(classify("error"), "failed");
});

test("a refused call is parked, never retried, whatever the attempts header says", () => {
  assert.deepEqual(nextAttempt("refused", undefined), { _tag: "park" });
  assert.deepEqual(nextAttempt("refused", "2"), { _tag: "park" });
});
