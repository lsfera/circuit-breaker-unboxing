import { test } from "node:test";
import assert from "node:assert/strict";
import * as DaemonPolicy from "../src/DaemonPolicy.ts";
import { State } from "@egress/domain/Model.ts";

const T0 = 1_700_000_000_000;
const DWELL = DaemonPolicy.RAMP_DWELL_MS;

/**
 * `step` takes the time rather than reading a clock, so these stay plain
 * assertions with no runtime, no TestClock and no mocking — the whole reason
 * the policy is a pure function of its inputs.
 */
const at = (offsetMs: number) => T0 + offsetMs;

/** The state a daemon is in while running normally. */
const ramping = (fraction: number) => ({ fraction, floor: true, rungSince: at(0) });
/** The state a daemon is in while stopped — no fraction, and no floor either. */
const stopped = { fraction: 0, floor: false, rungSince: at(0) };

test("CLOSED at full strength stays at full strength", () => {
  const state = DaemonPolicy.step(ramping(1), State.CLOSED, at(0));
  assert.equal(state.fraction, 1);
  assert.equal(state.floor, true);
});

test("a daemon starts idle, floor included, and its first CLOSED starts the ramp", () => {
  assert.deepEqual(DaemonPolicy.initial(at(0)), stopped, "nobody works on a circuit nobody has heard");
  const first = DaemonPolicy.step(DaemonPolicy.initial(at(0)), State.CLOSED, at(0));
  assert.deepEqual(first, { fraction: 0, floor: true, rungSince: at(0) }, "the floor alone, as after an outage");
});

test("a silent control plane leaves a quarter of the fleet working, with no floor to elect", () => {
  assert.deepEqual(DaemonPolicy.silent(at(0)), { fraction: DaemonPolicy.SILENT_FRACTION, floor: false, rungSince: at(0) });
  assert.equal(DaemonPolicy.SILENT_FRACTION, 0.25);
});

test("DEGRADED halves the fleet, and keeps the floor so it can never be none of it", () => {
  const state = DaemonPolicy.step(DaemonPolicy.initial(at(0)), State.DEGRADED, at(0));
  assert.equal(state.fraction, 0.5);
  assert.equal(state.floor, true, "a DEGRADED fleet that stops is indistinguishable from OPEN");
});

test("OPEN stops every daemon, floor included", () => {
  const state = DaemonPolicy.step(DaemonPolicy.initial(at(0)), State.OPEN, at(0));
  assert.equal(state.fraction, 0);
  assert.equal(state.floor, false, "OPEN means nobody, not one");
});

test("HALF_OPEN asks for nobody: the one call it permits is the prober's", () => {
  const half = (prior: DaemonPolicy.DaemonPolicyState) =>
    DaemonPolicy.step(prior, State.HALF_OPEN, at(0));
  assert.deepEqual(half(stopped), { fraction: 0, floor: false, rungSince: at(0) });
  assert.deepEqual(half(ramping(1)), { fraction: 0, floor: false, rungSince: at(0) });
});

test("recovery ramps one rung at a time, never snapping to full", () => {
  let s = DaemonPolicy.step(stopped, State.CLOSED, at(0));
  assert.deepEqual(
    [s.fraction, s.floor],
    [0, true],
    "first rung, immediately: one daemon, the one the broker elected",
  );
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL));
  assert.equal(s.fraction, 0.25, "second rung");
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL * 2));
  assert.equal(s.fraction, 0.5, "third rung");
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL * 3));
  assert.equal(s.fraction, 1, "all of it");
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL * 4));
  assert.equal(s.fraction, 1, "stays at full strength once there");
});

/** A rung is earned by being held, not by a control message arriving. */
test("a rung is held until it has been held long enough, however many events arrive", () => {
  let s = DaemonPolicy.step(stopped, State.CLOSED, at(0));
  assert.equal(s.fraction, 0);

  for (let i = 0; i < 10; i++) s = DaemonPolicy.step(s, State.CLOSED, at(100 * i));
  assert.equal(s.fraction, 0, "still the first rung, because no time has passed");

  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL - 1));
  assert.equal(s.fraction, 0, "and still, one millisecond short");

  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL));
  assert.equal(s.fraction, 0.25, "the rung is earned exactly when it has been held long enough");
});

/**
 * What lets a fleet converge with no coordination: same inputs, same answer.
 * Also why the ramp is gated on a clock and not on successful calls, which are
 * per daemon and would have the busy ones ramping while the idle ones held.
 */
test("the same inputs give the same target, which is what lets the fleet converge", () => {
  assert.deepEqual(
    DaemonPolicy.step(ramping(0.25), State.CLOSED, at(DWELL)),
    DaemonPolicy.step(ramping(0.25), State.CLOSED, at(DWELL)),
  );
});

test("recovering from DEGRADED continues the ramp rather than dropping back to one", () => {
  const degraded = DaemonPolicy.step(DaemonPolicy.initial(at(0)), State.DEGRADED, at(0));
  const closed = DaemonPolicy.step(degraded, State.CLOSED, at(DWELL));
  assert.equal(closed.fraction, 1, "half a fleet that recovers goes up, not back to a single daemon");
});

test("a relapse from CLOSED back through OPEN restarts the ramp from one daemon", () => {
  let s = DaemonPolicy.step(stopped, State.CLOSED, at(0));
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL));
  assert.equal(s.fraction, 0.25);
  // Upstream degrades again before the ramp finished.
  s = DaemonPolicy.step(s, State.OPEN, at(DWELL + 100));
  assert.deepEqual([s.fraction, s.floor], [0, false]);
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL + 200));
  assert.deepEqual(
    [s.fraction, s.floor],
    [0, true],
    "ramp restarts at one daemon, not from where it left off",
  );
  s = DaemonPolicy.step(s, State.CLOSED, at(DWELL + 300));
  assert.equal(
    s.fraction,
    0,
    "and the restarted ramp waits its dwell out — a flapping circuit does not fast-track",
  );
});

// ---------------------------------------------------------------------------
// Selecting a share of the fleet without knowing how big it is — ADR 013.
// ---------------------------------------------------------------------------

test("a position is stable for an identity and spread across the space", () => {
  assert.equal(DaemonPolicy.position("daemon-0"), DaemonPolicy.position("daemon-0"));
  const ps = Array.from({ length: 200 }, (_, i) => DaemonPolicy.position(`daemon-${i}`));
  assert.ok(
    ps.every((p) => p >= 0 && p < 1),
    "every position is in [0, 1), which is what makes a fraction a threshold",
  );
  const below = ps.filter((p) => p < 0.5).length;
  assert.ok(below > 70 && below < 130, `half the space should hold about half of 200, got ${below}`);
});

test("a falling fraction idles a subset of who was running, rather than reshuffling", () => {
  const fleet = Array.from({ length: 40 }, (_, i) => `daemon-${i}`);
  const running = (fraction: number) =>
    new Set(
      fleet.filter((id) =>
        DaemonPolicy.runsWork(ramping(fraction), {
          position: DaemonPolicy.position(id),
          isFloor: false,
        }),
      ),
    );
  const all = running(1);
  const half = running(0.5);
  const quarter = running(0.25);
  assert.ok(quarter.size < half.size && half.size < all.size, "fewer each time");
  for (const id of quarter) assert.ok(half.has(id), `${id} kept working as the fraction fell`);
  for (const id of half) assert.ok(all.has(id), `${id} was already working at full strength`);
});

test("the floor runs the elected daemon even when the fraction selects nobody", () => {
  const elected = { position: 0.99, isFloor: true };
  const other = { position: 0.99, isFloor: false };
  assert.equal(DaemonPolicy.runsWork(ramping(0), elected), true);
  assert.equal(DaemonPolicy.runsWork(ramping(0), other), false);
});

test("the floor does not override OPEN", () => {
  const elected = { position: 0.01, isFloor: true };
  assert.equal(
    DaemonPolicy.runsWork({ fraction: 0, floor: false, rungSince: at(0) }, elected),
    false,
    "OPEN clears the floor, so the elected daemon stops with everyone else",
  );
});

test("a fleet of any size runs at least one daemon whenever the target is non-zero", () => {
  // The property the floor exists for. Without it, 3.2% of five-daemon fleets
  // select nobody at 0.5 — measured in scripts/sim-fractional-target.mjs.
  for (const size of [1, 3, 5, 10]) {
    for (const seed of [0, 1, 2, 3, 4, 5, 6, 7]) {
      const fleet = Array.from({ length: size }, (_, i) => `fleet${seed}-daemon-${i}`);
      // The broker elects exactly one; which one is not this module's business.
      const electedId = fleet[seed % fleet.length]!;
      const running = fleet.filter((id) =>
        DaemonPolicy.runsWork(ramping(0.5), {
          position: DaemonPolicy.position(id),
          isFloor: id === electedId,
        }),
      );
      assert.ok(running.length >= 1, `fleet of ${size}, seed ${seed}, ran nobody`);
    }
  }
});
