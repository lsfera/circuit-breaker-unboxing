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

/** Drive the machine over a window with a fixed per-replica view. */
const drive = (
  from: Breaker.BreakerState,
  start: number,
  ms: number,
  healthy: number[],
  step = 100,
): [Breaker.BreakerState, Breaker.Transition[]] => {
  let s = from;
  const moves: Breaker.Transition[] = [];
  for (let t = start; t < start + ms; t += step) {
    for (const r of reports(t, healthy)) s = Breaker.ingest(s, r);
    const [next, move] = Breaker.step(s, t, CFG);
    s = next;
    if (O.isSome(move)) moves.push(move.value);
  }
  return [s, moves];
};

const fresh = () => Breaker.initial("api-a", CFG);

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
