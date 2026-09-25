import { Option as O } from "effect";
import { test } from "node:test";
import assert from "node:assert/strict";
import { classifyEvent } from "../src/Model.ts";
import type { Applied, Lease } from "../src/Model.ts";

const at = (epoch: string, counter: number, sequence: number): Applied => ({
  lease: O.some<Lease>({ epoch, counter }),
  sequence,
});

test("nothing recorded yet is a first event, whatever its sequence", () => {
  assert.equal(classifyEvent(O.none(), at("a", 1, 97)), "first");
});

test("within one leader, the sequence decides", () => {
  assert.equal(classifyEvent(O.some(at("a", 1, 4)), at("a", 1, 5)), "next");
  assert.equal(classifyEvent(O.some(at("a", 1, 4)), at("a", 1, 4)), "duplicate");
  assert.equal(classifyEvent(O.some(at("a", 1, 4)), at("a", 1, 7)), "gap");
});

test("a new epoch restarts the sequences, so a low one is not a duplicate", () => {
  assert.equal(classifyEvent(O.some(at("a", 3, 40)), at("b", 1, 1)), "new-epoch");
});

test("an older leader in the same epoch is stale, even with a higher sequence", () => {
  assert.equal(classifyEvent(O.some(at("a", 2, 10)), at("a", 1, 30)), "stale");
});

test("a newer leader continues the epoch's sequence", () => {
  assert.equal(classifyEvent(O.some(at("a", 1, 10)), at("a", 2, 11)), "next");
});

test("no lease on either side is the one epoch of a publisher that predates leases", () => {
  assert.equal(classifyEvent(O.some({ lease: O.none(), sequence: 3 }), { lease: O.none(), sequence: 4 }), "next");
});
