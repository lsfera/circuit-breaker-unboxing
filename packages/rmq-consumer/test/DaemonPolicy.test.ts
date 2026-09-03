import { test } from "node:test";
import assert from "node:assert/strict";
import * as DaemonPolicy from "../src/DaemonPolicy.ts";
import { State } from "@egress/domain/Model.ts";

const FLEET = 5;

test("CLOSED at full strength stays at full strength", () => {
  const state = DaemonPolicy.step(DaemonPolicy.initial(FLEET), State.CLOSED, FLEET);
  assert.equal(state.targetActive, FLEET);
});

test("DEGRADED halves the active fleet, rounding up, minimum 1", () => {
  assert.equal(DaemonPolicy.step(DaemonPolicy.initial(5), State.DEGRADED, 5).targetActive, 3);
  assert.equal(DaemonPolicy.step(DaemonPolicy.initial(1), State.DEGRADED, 1).targetActive, 1);
  assert.equal(DaemonPolicy.step(DaemonPolicy.initial(4), State.DEGRADED, 4).targetActive, 2);
});

test("OPEN stops every daemon", () => {
  const state = DaemonPolicy.step(DaemonPolicy.initial(FLEET), State.OPEN, FLEET);
  assert.equal(state.targetActive, 0);
});

test("HALF_OPEN is always exactly one, regardless of fleet size", () => {
  assert.equal(DaemonPolicy.step({ targetActive: 0 }, State.HALF_OPEN, FLEET).targetActive, 1);
  assert.equal(DaemonPolicy.step({ targetActive: 5 }, State.HALF_OPEN, FLEET).targetActive, 1);
});

test("recovery ramps up one rung per tick, never snaps to full", () => {
  let s = DaemonPolicy.step({ targetActive: 0 }, State.CLOSED, 20);
  assert.equal(s.targetActive, 1, "first rung");
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 4, "second rung");
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 16, "third rung");
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 20, "capped at fleet size, not the next rung (would be unbounded)");
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 20, "stays at full strength once there");
});

test("ramp is capped by fleet size even mid-rung", () => {
  // A fleet smaller than the next rung should jump straight to fleet size,
  // not overshoot past how many daemons actually exist.
  const s = DaemonPolicy.step({ targetActive: 1 }, State.CLOSED, 3);
  assert.equal(s.targetActive, 3);
});

test("a relapse from CLOSED back through OPEN restarts the ramp from zero", () => {
  let s = DaemonPolicy.step({ targetActive: 0 }, State.CLOSED, 20);
  assert.equal(s.targetActive, 1);
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 4);
  // Upstream degrades again before the ramp finished.
  s = DaemonPolicy.step(s, State.OPEN, 20);
  assert.equal(s.targetActive, 0);
  s = DaemonPolicy.step(s, State.CLOSED, 20);
  assert.equal(s.targetActive, 1, "ramp restarts from the bottom rung, not from where it left off");
});

test("activeIndices picks a stable, deterministic prefix of daemon indices", () => {
  assert.deepEqual(DaemonPolicy.activeIndices(3, 5), new Set([0, 1, 2]));
  assert.deepEqual(DaemonPolicy.activeIndices(0, 5), new Set());
  assert.deepEqual(DaemonPolicy.activeIndices(5, 5), new Set([0, 1, 2, 3, 4]));
});

test("activeIndices never exceeds the actual fleet size", () => {
  assert.deepEqual(DaemonPolicy.activeIndices(999, 3), new Set([0, 1, 2]));
});
