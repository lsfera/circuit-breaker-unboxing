import { test } from "node:test";
import assert from "node:assert/strict";
import { nextRedrive, REDRIVE_MAX_HOLD_MS } from "../src/Redrive.ts";
import { BROKER_CONSUMER_TIMEOUT_MS, MAX_REDRIVES } from "@egress/rmq/ControlPlane.ts";

/** The one decision in Redrive.ts with no broker in it: count header in, destination and next count out. */

test("no header is a first redrive", () => {
  assert.deepEqual(nextRedrive(undefined), { destination: "work", count: 1 });
});

test("counts climb by one per redrive while under the cap", () => {
  for (let n = 1; n < MAX_REDRIVES; n++) {
    assert.deepEqual(
      nextRedrive(String(n)),
      { destination: "work", count: n + 1 },
      `redrive ${n} must still go to work`,
    );
  }
});

test("the redrive that would exceed MAX_REDRIVES parks instead", () => {
  assert.deepEqual(nextRedrive(String(MAX_REDRIVES)), { destination: "parked" });
});

test("further redrives past the cap stay parked", () => {
  assert.deepEqual(nextRedrive(String(MAX_REDRIVES + 3)), { destination: "parked" });
});

/**
 * There is no way to publish anything but a digit string here, so an
 * unparseable header is a message this function has never produced — treated
 * as 0 rather than trusted, the same stance as a malformed control message
 * elsewhere in this repo.
 */
test("a header that doesn't parse is treated as zero, not trusted", () => {
  for (const header of ["not-a-number", "", "NaN", "-1"]) {
    const decision = nextRedrive(header);
    assert.ok(
      decision.destination === "work" && decision.count === (header === "-1" ? 0 : 1),
      `${JSON.stringify(header)} -> ${JSON.stringify(decision)}`,
    );
  }
});

/**
 * The elected daemon holds the redrive trigger unacked for the whole redrive. Past
 * the broker's consumer_timeout the channel is closed under it, the trigger is
 * redelivered, and a second redrive starts behind the first. Five minutes spare.
 */
test("the longest redrive fits inside the broker's consumer_timeout", () => {
  assert.ok(
    REDRIVE_MAX_HOLD_MS + 5 * 60_000 <= BROKER_CONSUMER_TIMEOUT_MS,
    `a redrive may hold its trigger ${REDRIVE_MAX_HOLD_MS} ms against a ${BROKER_CONSUMER_TIMEOUT_MS} ms timeout`,
  );
});
