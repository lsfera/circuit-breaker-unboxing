import { test } from "node:test";
import assert from "node:assert/strict";
import * as DaemonPolicy from "../src/DaemonPolicy.ts";
import { State } from "@egress/domain/Model.ts";

const FLEET = 5;
const T0 = 1_700_000_000_000;
const DWELL = DaemonPolicy.RAMP_DWELL_MS;

/**
 * `step` takes the time rather than reading a clock, so these stay plain
 * assertions with no runtime, no TestClock and no mocking — the whole reason
 * the policy is a pure function of its inputs.
 */
const at = (offsetMs: number) => T0 + offsetMs;

test("CLOSED at full strength stays at full strength", () => {
  const state = DaemonPolicy.step(
    DaemonPolicy.initial(FLEET, at(0)),
    State.CLOSED,
    FLEET,
    at(0),
  );
  assert.equal(state.targetActive, FLEET);
});

test("DEGRADED halves the active fleet, rounding up, minimum 1", () => {
  const d = (fleet: number) =>
    DaemonPolicy.step(DaemonPolicy.initial(fleet, at(0)), State.DEGRADED, fleet, at(0))
      .targetActive;
  assert.equal(d(5), 3);
  assert.equal(d(1), 1);
  assert.equal(d(4), 2);
});

test("OPEN stops every daemon", () => {
  const state = DaemonPolicy.step(
    DaemonPolicy.initial(FLEET, at(0)),
    State.OPEN,
    FLEET,
    at(0),
  );
  assert.equal(state.targetActive, 0);
});

test("HALF_OPEN is always exactly one, regardless of fleet size", () => {
  const half = (targetActive: number) =>
    DaemonPolicy.step({ targetActive, rungSince: at(0) }, State.HALF_OPEN, FLEET, at(0))
      .targetActive;
  assert.equal(half(0), 1);
  assert.equal(half(5), 1);
});

test("recovery ramps one rung at a time, never snapping to full", () => {
  let s = DaemonPolicy.step({ targetActive: 0, rungSince: at(0) }, State.CLOSED, 20, at(0));
  assert.equal(s.targetActive, 1, "first rung, immediately: a closed circuit should do work now");
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL));
  assert.equal(s.targetActive, 4, "second rung");
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL * 2));
  assert.equal(s.targetActive, 16, "third rung");
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL * 3));
  assert.equal(s.targetActive, 20, "capped at fleet size, not the next rung (would be unbounded)");
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL * 4));
  assert.equal(s.targetActive, 20, "stays at full strength once there");
});

/**
 * The point of the whole phase: a rung is earned by being held, not by a
 * message arriving.
 *
 * Before this, a rung advanced on any `circuit.control` message — so the pace
 * of the ramp was set by how chatty the aggregator happened to be, and a
 * recovering third party got more load because a snapshot was due rather than
 * because the current rung was working.
 */
test("a rung is held until it has been held long enough, however many events arrive", () => {
  let s = DaemonPolicy.step({ targetActive: 0, rungSince: at(0) }, State.CLOSED, 20, at(0));
  assert.equal(s.targetActive, 1);

  // Ten events in the first second of the rung: the old policy would have
  // ramped to full strength on these alone.
  for (let i = 0; i < 10; i++) {
    s = DaemonPolicy.step(s, State.CLOSED, 20, at(100 * i));
  }
  assert.equal(s.targetActive, 1, "still the first rung, because no time has passed");

  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL - 1));
  assert.equal(s.targetActive, 1, "and still, one millisecond short");

  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL));
  assert.equal(s.targetActive, 4, "the rung is earned exactly when it has been held long enough");
});

/**
 * Five daemons converge on the same number with no coordination at all, and
 * this is the property that makes that possible: the same inputs give the same
 * answer, so the fleet does not need to agree on anything except what it has
 * already seen. It is also why the gate is a clock and not a count of
 * successful calls — a success count is per daemon, so the busy ones would
 * ramp while the idle ones held, and the fleet would disagree about its size.
 */
test("the same inputs give the same target, which is what lets the fleet converge", () => {
  const busy = { targetActive: 1, rungSince: at(0) };
  const idle = { targetActive: 1, rungSince: at(0) };
  assert.deepEqual(
    DaemonPolicy.step(busy, State.CLOSED, FLEET, at(DWELL)),
    DaemonPolicy.step(idle, State.CLOSED, FLEET, at(DWELL)),
  );
});

test("ramp is capped by fleet size even mid-rung", () => {
  // A fleet smaller than the next rung should jump straight to fleet size,
  // not overshoot past how many daemons actually exist.
  const s = DaemonPolicy.step({ targetActive: 1, rungSince: at(0) }, State.CLOSED, 3, at(DWELL));
  assert.equal(s.targetActive, 3);
});

test("a relapse from CLOSED back through OPEN restarts the ramp from zero", () => {
  let s = DaemonPolicy.step({ targetActive: 0, rungSince: at(0) }, State.CLOSED, 20, at(0));
  assert.equal(s.targetActive, 1);
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL));
  assert.equal(s.targetActive, 4);
  // Upstream degrades again before the ramp finished.
  s = DaemonPolicy.step(s, State.OPEN, 20, at(DWELL + 100));
  assert.equal(s.targetActive, 0);
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL + 200));
  assert.equal(s.targetActive, 1, "ramp restarts from the bottom rung, not from where it left off");
  s = DaemonPolicy.step(s, State.CLOSED, 20, at(DWELL + 300));
  assert.equal(
    s.targetActive,
    1,
    "and the restarted ramp waits its dwell out like any other — a flapping circuit does not fast-track",
  );
});

test("activeIndices picks a stable, deterministic prefix of daemon indices", () => {
  assert.deepEqual(DaemonPolicy.activeIndices(3, 5), new Set([0, 1, 2]));
  assert.deepEqual(DaemonPolicy.activeIndices(0, 5), new Set());
  assert.deepEqual(DaemonPolicy.activeIndices(5, 5), new Set([0, 1, 2, 3, 4]));
});

test("activeIndices never exceeds the actual fleet size", () => {
  assert.deepEqual(DaemonPolicy.activeIndices(999, 3), new Set([0, 1, 2]));
});
