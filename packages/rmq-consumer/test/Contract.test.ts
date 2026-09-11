import { test } from "node:test";
import assert from "node:assert/strict";
import { Option as O } from "effect";
import { SEQUENCED_EVENT, SNAPSHOT_EVENT } from "@egress/domain/Model.ts";
import type { EventType } from "@egress/domain/Model.ts";
import { initialContract, observe } from "../src/Contract.ts";
import type { ContractState } from "../src/Contract.ts";

const fold = (
  events: ReadonlyArray<readonly [type: EventType, sequence: number]>,
): ContractState => events.reduce((s, [type, seq]) => observe(s, type, seq), initialContract);

const changes = (...sequences: ReadonlyArray<number>) =>
  sequences.map((n) => [SEQUENCED_EVENT, n] as const);

test("a consecutive run is neither gapped nor duplicated", () => {
  const c = fold(changes(1, 2, 3, 4));
  assert.equal(c.gaps, 0);
  assert.equal(c.duplicates, 0);
  assert.deepEqual(c.lastSequence, O.some(4));
});

test("joining mid-incident is not a gap", () => {
  // A daemon started during an outage sees its first event at some arbitrary
  // sequence. Counting that as a gap would make the metric lie on every
  // restart, which is worse than not measuring it at all.
  const c = fold(changes(97, 98));
  assert.equal(c.gaps, 0);
  assert.deepEqual(c.lastSequence, O.some(98));
});

test("a skipped sequence is a gap", () => {
  const c = fold(changes(1, 2, 5));
  assert.equal(c.gaps, 1);
  assert.equal(c.duplicates, 0);
  assert.deepEqual(c.lastSequence, O.some(5));
});

test("a repeated sequence is a duplicate", () => {
  const c = fold(changes(1, 2, 2));
  assert.equal(c.duplicates, 1);
  assert.equal(c.gaps, 0);
});

test("a sequence that goes backwards is a duplicate, and does not rewind the high-water mark", () => {
  const c = fold(changes(10, 11, 12, 8, 9));
  assert.equal(c.duplicates, 2, "8 and 9 both reuse a sequence already seen");
  assert.equal(c.gaps, 0, "going backwards is not a gap");
  assert.deepEqual(c.lastSequence, O.some(12), "a stale event must not move the mark back");
});

test("snapshots repeat the current sequence and are exempt", () => {
  const c = fold([
    [SEQUENCED_EVENT, 1],
    [SNAPSHOT_EVENT, 1],
    [SNAPSHOT_EVENT, 1],
    [SEQUENCED_EVENT, 2],
  ]);
  assert.equal(c.duplicates, 0, "a snapshot repeating the sequence is the contract, not a breach");
  assert.equal(c.gaps, 0);
  assert.deepEqual(c.lastSequence, O.some(2));
});

test("a gap is counted once, not once per missing number", () => {
  const c = fold(changes(1, 9));
  assert.equal(c.gaps, 1);
});
