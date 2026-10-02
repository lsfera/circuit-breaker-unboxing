import assert from "node:assert/strict";
import { test } from "node:test";
import * as Redrive from "../src/Redrive.ts";

/**
 * `nextRedrive` is the one decision in Redrive.ts with no broker in it —
 * pure, so tested directly. Cases mirror master's own coverage for the same
 * function (`packages/rmq-consumer/src/Redrive.ts` at the vendored tag).
 */

test("no header is a first redrive", () => {
  assert.deepEqual(Redrive.nextRedrive(undefined), { destination: "work", count: 1 });
});

test("counts climb by one per redrive while under the cap", () => {
  assert.deepEqual(Redrive.nextRedrive("1"), { destination: "work", count: 2 });
  assert.deepEqual(Redrive.nextRedrive("4"), { destination: "work", count: 5 });
});

test("the redrive that would exceed MAX_REDRIVES parks instead", () => {
  assert.deepEqual(Redrive.nextRedrive("5"), { destination: "parked" });
});

test("further redrives past the cap stay parked", () => {
  assert.deepEqual(Redrive.nextRedrive("6"), { destination: "parked" });
  assert.deepEqual(Redrive.nextRedrive("100"), { destination: "parked" });
});

test("a header that doesn't parse is treated as zero, not trusted", () => {
  // Untrusted rather than merely defensive: there is no way for this header
  // to hold anything but a digit string this module itself published, so a
  // garbage value here means something is forging headers — treating it as
  // zero rather than, say, parked-forever is what keeps a forged header from
  // being a way to skip the entire redrive budget.
  assert.deepEqual(Redrive.nextRedrive("not-a-number"), { destination: "work", count: 1 });
});

test("a negative or fractional count is not a count: it buys no extra redrives", () => {
  assert.deepEqual(Redrive.nextRedrive("-100"), { destination: "work", count: 1 });
  assert.deepEqual(Redrive.nextRedrive("2.5"), { destination: "work", count: 1 });
});
