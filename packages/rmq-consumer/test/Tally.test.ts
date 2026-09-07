import { test } from "node:test";
import assert from "node:assert/strict";
import * as Tally from "../src/Tally.ts";
import { initialContract } from "../src/Contract.ts";
import type { ContractState } from "../src/Contract.ts";

/** The arithmetic behind `egress_daemon_*`, with no registry and no broker. */

const contract = (over: Partial<ContractState> = {}): ContractState => ({
  ...initialContract,
  ...over,
});

test("a delta is what happened since the last publication", () => {
  const counts = Tally.zero();
  counts.ok = 12;
  counts.failed = 3;
  Tally.observed(counts, "egress.circuit.snapshot");

  const first = Tally.snapshot(counts, contract({ gaps: 1 }));
  const delta = Tally.since(Tally.nothing, first);

  assert.equal(delta.ok, 12);
  assert.equal(delta.failed, 3);
  assert.equal(delta.gaps, 1);
  assert.deepEqual(delta.byType, [["egress.circuit.snapshot", 1]]);
});

test("publishing twice with nothing in between asks the registry for nothing", () => {
  const counts = Tally.zero();
  counts.ok = 5;
  Tally.observed(counts, "egress.circuit.state_changed");

  const published = Tally.snapshot(counts, contract());
  const delta = Tally.since(published, Tally.snapshot(counts, contract()));

  assert.equal(delta.ok, 0);
  assert.deepEqual(delta.byType, [], "a counter is never asked to go up by zero");
});

/**
 * Publishing suspends. Anything counted during it must land in the next delta
 * rather than in a mark that moved without it — which is what re-reading the
 * counters after publishing would do.
 */
test("what is counted after a snapshot is taken survives to the next delta", () => {
  const counts = Tally.zero();
  Tally.observed(counts, "egress.circuit.state_changed");

  // The flush takes its one reading and advances the mark from it...
  const published = Tally.snapshot(counts, contract());
  assert.deepEqual(Tally.since(Tally.nothing, published).byType, [
    ["egress.circuit.state_changed", 1],
  ]);

  // ...and an event arrives while it is still publishing.
  Tally.observed(counts, "egress.circuit.state_changed");
  counts.ok = 4;

  const next = Tally.since(published, Tally.snapshot(counts, contract()));
  assert.deepEqual(
    next.byType,
    [["egress.circuit.state_changed", 1]],
    "the mid-flush arrival is owed to the registry, not lost to a mark that moved without it",
  );
  assert.equal(next.ok, 4);
});

test("a type first seen after the mark is published in full", () => {
  const counts = Tally.zero();
  Tally.observed(counts, "egress.circuit.snapshot");
  const published = Tally.snapshot(counts, contract());

  Tally.observed(counts, "egress.circuit.state_changed");
  Tally.observed(counts, "egress.circuit.state_changed");

  const delta = Tally.since(published, Tally.snapshot(counts, contract()));
  assert.deepEqual(delta.byType, [["egress.circuit.state_changed", 2]]);
});

/**
 * `gaps` and `duplicates` are not incremented by the daemon — `observe`
 * derives them from the sequence stream — so folding them into the same
 * snapshot is what keeps one reading covering everything a flush publishes.
 */
test("the delivery-contract counters come along in the same reading", () => {
  const counts = Tally.zero();
  const published = Tally.snapshot(counts, contract({ gaps: 2, duplicates: 1 }));
  const delta = Tally.since(published, Tally.snapshot(counts, contract({ gaps: 5, duplicates: 1 })));

  assert.equal(delta.gaps, 3);
  assert.equal(delta.duplicates, 0);
});
