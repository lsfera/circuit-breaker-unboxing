import { test } from "node:test";
import assert from "node:assert/strict";
import * as Verdict from "../src/Verdict.ts";

test("an empty registry has zero open fraction and a closed verdict", () => {
  const registry: Verdict.ApiRegistry = new Map();
  assert.equal(Verdict.openFraction(registry), 0);
  assert.equal(Verdict.verdictFor(Verdict.openFraction(registry), 0.5), "closed");
});

test("half_open counts toward the open fraction, same as open", () => {
  const registry: Verdict.ApiRegistry = new Map([
    ["a", { state: "half_open", at: 0 }],
    ["b", { state: "closed", at: 0 }],
  ]);
  assert.equal(Verdict.openFraction(registry), 0.5);
});

test("verdictFor opens at the threshold, not only past it", () => {
  assert.equal(Verdict.verdictFor(0.5, 0.5), "open", "exactly at threshold should open");
  assert.equal(Verdict.verdictFor(0.49, 0.5), "closed");
});

test("five replicas, three open: 60% crosses the default 0.5 threshold", () => {
  const registry: Verdict.ApiRegistry = new Map([
    ["a", { state: "open", at: 0 }],
    ["b", { state: "open", at: 0 }],
    ["c", { state: "open", at: 0 }],
    ["d", { state: "closed", at: 0 }],
    ["e", { state: "closed", at: 0 }],
  ]);
  const fraction = Verdict.openFraction(registry);
  assert.equal(fraction, 0.6);
  assert.equal(Verdict.verdictFor(fraction, 0.5), "open");
});

test("prune drops an instance that's gone stale, and keeps a fresh one", () => {
  const registry: Verdict.ApiRegistry = new Map([
    ["stale", { state: "open", at: 0 }],
    ["fresh", { state: "open", at: 59_000 }],
  ]);
  const pruned = Verdict.prune(registry, 60_001, 60_000);
  assert.equal(pruned.has("stale"), false, "60001 - 0 = 60001, past the 60000ms window");
  assert.equal(pruned.has("fresh"), true, "60001 - 59000 = 1001, still inside the window");
});

test("pruning a dead replica's last open vote lets the fraction recover", () => {
  const registry: Verdict.ApiRegistry = new Map([
    ["dead", { state: "open", at: 0 }],
    ["alive", { state: "closed", at: 100_000 }],
  ]);
  // Without pruning, one dead replica's stale "open" would permanently hold
  // the fraction at 50% no matter how long the live replica stays healthy.
  const pruned = Verdict.prune(registry, 100_000, 60_000);
  assert.equal(Verdict.openFraction(pruned), 0, "the dead replica's vote must stop counting");
});

test("isReplicaState accepts exactly the four known states, nothing else", () => {
  assert.equal(Verdict.isReplicaState("closed"), true);
  assert.equal(Verdict.isReplicaState("open"), true);
  assert.equal(Verdict.isReplicaState("half_open"), true);
  assert.equal(Verdict.isReplicaState("isolated"), true);
  assert.equal(Verdict.isReplicaState("degraded"), false, "not one of this fleet's real states");
  assert.equal(Verdict.isReplicaState(""), false);
});

test("shouldAccept takes the first event for an instance unconditionally", () => {
  assert.equal(Verdict.shouldAccept(undefined, { at: 0 }), true);
});

test("shouldAccept takes an event no older than what's already known", () => {
  assert.equal(Verdict.shouldAccept({ at: 100 }, { at: 100 }), true, "equal at is accepted, not just newer");
  assert.equal(Verdict.shouldAccept({ at: 100 }, { at: 200 }), true);
});

test("shouldAccept rejects a redelivered or delayed event older than what's already known", () => {
  assert.equal(Verdict.shouldAccept({ at: 200 }, { at: 100 }), false);
});
