import { Option as O } from "effect";
import { test } from "node:test";
import assert from "node:assert/strict";
import * as Breaker from "../src/Breaker.ts";
import { defaultConfig, Reason, State } from "../src/Model.ts";
import type { AggregatorConfig, ReplicaReport } from "../src/Model.ts";

/**
 * The state machine is pure, so these tests need no Effect runtime, no clock
 * and no mocking — just values in, values out.
 */

const CFG: AggregatorConfig = {
  ...defaultConfig,
  dwellMs: 500,
  minStateMs: 500,
  openMs: 1000,
};

const reports = (t: number, healthy: number[], total = 4): ReplicaReport[] =>
  healthy.map((h, i) => ({
    replicaId: `envoy-${i}`,
    apiId: "api-a",
    healthy: h,
    total,
    ejectionsActive: total - h,
    overflowTotal: 0,
    observedAt: t,
  }));

/** A published transition, plus the tick it published on — the base
 * `Transition` has no clock of its own, since `Breaker` is pure. */
type TimedTransition = Breaker.Transition & { readonly t: number };

/** Drive the machine over a window with a fixed per-replica view. */
const drive = (
  from: Breaker.BreakerState,
  start: number,
  ms: number,
  healthy: number[],
  step = 100,
): [Breaker.BreakerState, TimedTransition[]] => driveWith(CFG, from, start, ms, () => healthy, step);

/**
 * Same as `drive`, but for a caller that needs its own config (the flap
 * replay wants real timings, not the scaled-down `CFG` above) or a healthy
 * count that changes over the window (a flap, not a fixed level).
 */
const driveWith = (
  cfg: AggregatorConfig,
  from: Breaker.BreakerState,
  start: number,
  ms: number,
  healthyAt: (t: number) => number[],
  step = 100,
): [Breaker.BreakerState, TimedTransition[]] => {
  let s = from;
  const moves: TimedTransition[] = [];
  for (let t = start; t < start + ms; t += step) {
    for (const r of reports(t, healthyAt(t))) s = Breaker.ingest(s, r);
    const [next, move] = Breaker.step(s, t, cfg);
    s = next;
    if (O.isSome(move)) moves.push({ t, ...move.value });
  }
  return [s, moves];
};

const fresh = (cfg: AggregatorConfig = CFG) => Breaker.initial("api-a", cfg);

test("stays closed while every replica is healthy", () => {
  const [s, moves] = drive(fresh(), 1000, 3000, [4, 4, 4, 4, 4]);
  assert.equal(s.state, State.CLOSED);
  assert.deepEqual(moves, []);
});

test("a minority of unhealthy replicas does not trip the fleet", () => {
  const [s] = drive(fresh(), 1000, 3000, [0, 0, 4, 4, 4]);
  assert.equal(s.state, State.CLOSED);
});

test("partial ejection across a quorum degrades but does not open", () => {
  const [s] = drive(fresh(), 1000, 3000, [2, 3, 2, 3, 4]);
  assert.equal(s.state, State.DEGRADED);
});

test("quorate but not unanimous loss opens with the weaker reason", () => {
  const [s, moves] = drive(fresh(), 1000, 3000, [0, 0, 0, 0, 4]);
  assert.equal(s.state, State.OPEN);
  assert.equal(moves[0]?.reason, Reason.OUTLIER_EJECTION);
});

test("unanimous loss is reported as ALL_ENDPOINTS_EJECTED", () => {
  const [, moves] = drive(fresh(), 1000, 1000, [0, 0, 0, 0, 0]);
  assert.equal(moves[0]?.reason, Reason.ALL_ENDPOINTS_EJECTED);
});

test("dwell time suppresses a transient blip", () => {
  let [s] = drive(fresh(), 1000, 2000, [4, 4, 4, 4, 4]);
  [s] = drive(s, 3000, 300, [0, 0, 0, 0, 0]); // shorter than the 500ms dwell
  [s] = drive(s, 3300, 2000, [4, 4, 4, 4, 4]);
  assert.equal(s.state, State.CLOSED);
});

test("open -> half-open -> closed on recovery, gapless sequence", () => {
  let [s, a] = drive(fresh(), 1000, 1000, [0, 0, 0, 0, 0]);
  assert.equal(s.state, State.OPEN);

  const [s2, b] = drive(s, 2000, 4000, [4, 4, 4, 4, 4]);
  assert.equal(s2.state, State.CLOSED);

  assert.deepEqual(
    [...a, ...b].map((m) => m.to),
    [State.OPEN, State.HALF_OPEN, State.CLOSED],
  );
  assert.equal(s2.sequence, 3, "sequence numbers must be gapless");
});

test("a failed probe reopens and doubles the backoff", () => {
  const [s, moves] = drive(fresh(), 1000, 6000, [0, 0, 0, 0, 0]);
  assert.deepEqual(
    moves.slice(0, 3).map((m) => m.to),
    [State.OPEN, State.HALF_OPEN, State.OPEN],
  );
  assert.ok(moves.some((m) => m.reason === Reason.PROBE_FAILED));
  assert.ok(
    s.openBackoffMs > CFG.openMs,
    "backoff must grow so probes get rarer, not hammer a dead upstream",
  );
});

/**
 * The probe landing on a half-healthy fleet used to fall off the end of the
 * HALF_OPEN branch and mean "stay put", so a partially recovered upstream held
 * the circuit in HALF_OPEN indefinitely — and HALF_OPEN tells the daemon fleet
 * to run exactly one prober, while DEGRADED, the state that exists for this,
 * tells it to run half of them. Measured before the fix: 600 ticks, no
 * transition, no event published for anything downstream to act on.
 */
test("a probe that finds the fleet half-healthy settles into DEGRADED, not HALF_OPEN forever", () => {
  // All hosts gone: long enough to open, not long enough to time out into a probe.
  const [open, toOpen] = drive(fresh(), 1000, 1200, [0, 0, 0, 0, 0]);
  assert.equal(open.state, State.OPEN);
  assert.deepEqual(toOpen.map((m) => m.to), [State.OPEN]);

  // The upstream comes back at half strength and stays there for a minute.
  const [settled, moves] = drive(open, 2200, 60_000, [2, 2, 2, 2, 2]);
  assert.deepEqual(
    moves.map((m) => m.to),
    [State.HALF_OPEN, State.DEGRADED],
    "the probe must resolve: a half-healthy fleet is a verdict, not an inconclusive result",
  );
  assert.equal(settled.state, State.DEGRADED);
  assert.equal(settled.sequence, 3, "sequence numbers must stay gapless");

  // The backoff it earned is kept, so a relapse does not probe optimistically
  // the way a full recovery is allowed to.
  assert.ok(
    settled.openBackoffMs >= open.openBackoffMs,
    "a partial recovery must not reset the backoff",
  );

  // And a full recovery still closes it from there.
  const [closed] = drive(settled, 63_000, 4000, [4, 4, 4, 4, 4]);
  assert.equal(closed.state, State.CLOSED);
});

test("backoff is capped at maxOpenMs", () => {
  const [s] = drive(fresh(), 1000, 120_000, [0, 0, 0, 0, 0], 250);
  assert.ok(s.openBackoffMs <= CFG.maxOpenMs);
});

test("a stale replica stops counting toward quorum", () => {
  let s = fresh();
  // Three replicas report DOWN, then go silent.
  for (const r of reports(1000, [0, 0, 0])) s = Breaker.ingest(s, r);
  [s] = Breaker.step(s, 1000, CFG);
  // Two healthy replicas keep reporting well past replicaTimeoutMs.
  for (let t = 8000; t < 12000; t += 100) {
    for (const r of reports(t, [4, 4]).map((r, i) => ({
      ...r,
      replicaId: `envoy-${i + 3}`,
    })))
      s = Breaker.ingest(s, r);
    [s] = Breaker.step(s, t, CFG);
  }
  assert.equal(s.state, State.CLOSED);
  assert.equal(s.replicas.size, 2, "stale replicas are evicted");
});

test("snapshot exposes per-replica votes for the console", () => {
  const [s] = drive(fresh(), 1000, 1000, [4, 2, 0, 4, 4]);
  const snap = Breaker.snapshot(s);
  assert.equal(snap.replicas.length, 5);
  assert.equal(snap.votes.OK + snap.votes.DEGRADED + snap.votes.DOWN, 5);
  assert.deepEqual(
    snap.replicas.map((r) => r.replicaId),
    ["envoy-0", "envoy-1", "envoy-2", "envoy-3", "envoy-4"],
  );
});

test("ingest is immutable — the input state is never mutated", () => {
  const a = fresh();
  const b = Breaker.ingest(a, reports(1000, [0])[0]!);
  assert.equal(a.replicas.size, 0);
  assert.equal(b.replicas.size, 1);
});

/**
 * DEGRADED's earned hold. A relapse into DEGRADED soon after a close means the
 * fleet must then look healthy without a break for `closeHoldMs` (at least
 * `relapseHoldMs`, doubling, capped) before closing again.
 */
const RELAPSE_CFG: AggregatorConfig = {
  ...defaultConfig,
  dwellMs: 200,
  minStateMs: 300,
  openMs: 1000,
  relapseWindowMs: 1000,
  relapseHoldMs: 800,
  maxCloseHoldMs: 2400,
};

const DEGRADED_VIEW = [2, 3, 2, 3, 4];
const HEALTHY_VIEW = [4, 4, 4, 4, 4];

/**
 * One degrade-then-heal cycle starting at `t`: 400ms degraded, then healthy
 * long enough to clear whatever hold DEGRADED has earned. Returns where the
 * next cycle starts, back to back, so each re-entry is inside the relapse window.
 */
const flapCycle = (
  s: Breaker.BreakerState,
  t: number,
): [Breaker.BreakerState, Breaker.Transition[], number] => {
  const [degraded, a] = driveWith(RELAPSE_CFG, s, t, 400, () => DEGRADED_VIEW);
  const healMs = degraded.closeHoldMs + 400;
  const [healed, b] = driveWith(RELAPSE_CFG, degraded, t + 400, healMs, () => HEALTHY_VIEW);
  return [healed, [...a, ...b], t + 400 + healMs];
};

/** `n` back-to-back cycles from t=1000. */
const flapCycles = (n: number): [Breaker.BreakerState, Breaker.Transition[]] => {
  let s = fresh(RELAPSE_CFG);
  let t = 1000;
  const moves: Breaker.Transition[] = [];
  for (let i = 0; i < n; i++) {
    const [next, cycleMoves, end] = flapCycle(s, t);
    s = next;
    t = end;
    moves.push(...cycleMoves);
  }
  return [s, moves];
};

const edges = (moves: ReadonlyArray<Breaker.Transition>) => moves.map((m) => `${m.from}->${m.to}`);

test("a first incident closes as fast as before: closeHoldMs is dwellMs", () => {
  const [s, toDegraded] = drive(fresh(), 1000, 2000, [2, 3, 2, 3, 4]);
  assert.deepEqual(edges(toDegraded), ["CLOSED->DEGRADED"]);
  assert.equal(s.closeHoldMs, CFG.dwellMs);

  const [closed, toClosed] = drive(s, 3000, 1000, [4, 4, 4, 4, 4]);
  assert.deepEqual(toClosed.map((m) => [m.t, m.to]), [[3500, State.CLOSED]]);
  assert.equal(closed.state, State.CLOSED);
});

test("a relapse raises closeHoldMs to relapseHoldMs, then it doubles, capped at maxCloseHoldMs", () => {
  assert.equal(flapCycles(1)[0].closeHoldMs, 200, "first incident: base");
  assert.equal(flapCycles(2)[0].closeHoldMs, 800, "first relapse: relapseHoldMs, not 2 x 200");
  assert.equal(flapCycles(3)[0].closeHoldMs, 1600);
  const [four, moves] = flapCycles(4);
  assert.equal(four.closeHoldMs, 2400, "capped, not 3200");
  assert.equal(four.state, State.CLOSED);
  assert.deepEqual(edges(moves), Array.from({ length: 4 }, () => ["CLOSED->DEGRADED", "DEGRADED->CLOSED"]).flat());
});

test("after a relapse, a healthy window shorter than the hold does not close; a break restarts the clock", () => {
  const [once] = flapCycles(1);
  const t = once.changedAt + 50;
  const [relapsed] = driveWith(RELAPSE_CFG, once, t, 400, () => DEGRADED_VIEW);
  assert.equal(relapsed.state, State.DEGRADED);
  assert.equal(relapsed.closeHoldMs, 800);

  // 600ms healthy, 200ms degraded, 600ms healthy: 1,200ms mostly healthy, never 800ms unbroken.
  const view = (at: number) => (at - (t + 400) >= 600 && at - (t + 400) < 800 ? DEGRADED_VIEW : HEALTHY_VIEW);
  const [held, moves] = driveWith(RELAPSE_CFG, relapsed, t + 400, 1400, view);
  assert.deepEqual(moves, []);
  assert.equal(held.state, State.DEGRADED);

  const [closed, closing] = driveWith(RELAPSE_CFG, held, t + 1800, 600, () => HEALTHY_VIEW);
  assert.deepEqual(edges(closing), ["DEGRADED->CLOSED"]);
  assert.equal(closed.state, State.CLOSED);
});

test("closeHoldMs resets to dwellMs once CLOSED has held past the relapse window", () => {
  const [s] = flapCycles(3);
  assert.equal(s.state, State.CLOSED);
  assert.ok(s.closeHoldMs > RELAPSE_CFG.dwellMs);
  const [stable] = driveWith(RELAPSE_CFG, s, s.changedAt, 4000, () => HEALTHY_VIEW);
  assert.equal(stable.state, State.CLOSED);
  assert.equal(stable.closeHoldMs, RELAPSE_CFG.dwellMs);
});

/**
 * The chaos fault `net-upstream-flap`, at real timings: the upstream is
 * unreachable 10s, reachable 6s, repeatedly. Before the relapse hold, every 6s
 * window re-closed the breaker onto a link still flapping. Now the first
 * incident closes once and every later healthy window is shorter than the hold.
 */
test("flap replay: after the first close, the breaker stays shut out until the link is stable", () => {
  const FLAP_START = 10_000;
  const FLAP_END = FLAP_START + 48_000;
  const flapping = (t: number): number[] =>
    t < FLAP_START || t >= FLAP_END || (t - FLAP_START) % 16_000 >= 10_000 ? HEALTHY_VIEW : DEGRADED_VIEW;

  const [settled] = driveWith(defaultConfig, Breaker.initial("api-a", defaultConfig, 0), 0, FLAP_START, () => HEALTHY_VIEW, 250);
  const [afterFault, faultMoves] = driveWith(defaultConfig, settled, FLAP_START, FLAP_END - FLAP_START, flapping, 250);

  assert.deepEqual(
    edges(faultMoves),
    ["CLOSED->DEGRADED", "DEGRADED->CLOSED", "CLOSED->DEGRADED"],
    "one close, after the first incident; none after the relapse",
  );
  assert.equal(afterFault.state, State.DEGRADED);
  assert.equal(afterFault.closeHoldMs, defaultConfig.relapseHoldMs);

  const [stable, recovery] = driveWith(defaultConfig, afterFault, FLAP_END, 15_000, flapping, 250);
  // The last outage ended at 52,000; the close lands one relapse hold after it.
  const lastBreakEnd = FLAP_START + 32_000 + 10_000;
  assert.deepEqual(recovery.map((m) => [m.t, m.to]), [[lastBreakEnd + defaultConfig.relapseHoldMs, State.CLOSED]]);
  assert.equal(stable.state, State.CLOSED);
});
