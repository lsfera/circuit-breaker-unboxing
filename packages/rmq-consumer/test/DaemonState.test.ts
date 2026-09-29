import { test } from "node:test";
import assert from "node:assert/strict";
import { Option as O } from "effect";
import { SEQUENCED_EVENT, SNAPSHOT_EVENT, State } from "@egress/domain/Model.ts";
import type { Lease } from "@egress/domain/Model.ts";
import * as DaemonState from "../src/DaemonState.ts";
import { RAMP_DWELL_MS } from "../src/DaemonPolicy.ts";

/** The daemon's decisions, with no broker, no connections and no runtime. */

const T0 = 1_700_000_000_000;
const start = DaemonState.initialState(T0);

const reduce = (
  state: DaemonState.DaemonState,
  command: DaemonState.Command,
  redriveOnClose = true,
) => DaemonState.reduce(state, command, redriveOnClose);

test("entering HALF_OPEN asks for a probe trigger to be published", () => {
  const { next, actions } = reduce(start, {
    _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
    state: State.HALF_OPEN,
    sequence: 7,
    at: T0,
  });
  assert.equal(next.circuit, State.HALF_OPEN);
  assert.equal(next.policy.fraction, 0, "HALF_OPEN asks for nobody; the prober is elected");
  assert.equal(next.policy.floor, false);
  assert.deepEqual(actions, [{ _tag: "PublishProbeTrigger", sequence: 7 }]);
});

/**
 * The distinction a snapshot would otherwise destroy: the aggregator republishes
 * the current state every `snapshotMs`, and a redrive per snapshot would replay
 * the dead-letter queue every fifteen seconds forever.
 */
test("only the transition into CLOSED asks for a redrive, not every event that says CLOSED", () => {
  const open = reduce(start, {
    _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
    state: State.OPEN,
    sequence: 1,
    at: T0,
  }).next;

  const recovered = reduce(open, {
    _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
    state: State.CLOSED,
    sequence: 2,
    at: T0 + 1000,
  });
  assert.deepEqual(recovered.actions, [{ _tag: "PublishRedriveTrigger", sequence: 2 }]);

  const snapshot = reduce(recovered.next, {
    _tag: "CircuitChanged",
    type: SNAPSHOT_EVENT,
    lease: O.none(),
    state: State.CLOSED,
    sequence: 2,
    at: T0 + 16_000,
  });
  assert.deepEqual(snapshot.actions, [], "a snapshot repeating CLOSED must not replay anything");
});

test("REDRIVE_ON_CLOSE off means no redrive trigger, and nothing else changes", () => {
  const open = reduce(start, { _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(), state: State.OPEN, sequence: 1, at: T0 })
    .next;
  const { next, actions } = reduce(
    open,
    { _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(), state: State.CLOSED, sequence: 2, at: T0 + 1000 },
    false,
  );
  assert.deepEqual(actions, []);
  assert.equal(next.circuit, State.CLOSED);
});

/**
 * Every daemon publishes a probe trigger so one still arrives when some are
 * down, and SAC delivers all of them to the one elected consumer. Dropping the
 * repeats is what turns "several triggers" back into "one probe per
 * transition" — and it is one atomic step now, not a read followed by a write
 * from a concurrent AMQP callback.
 */
test("a probe trigger fires once per sequence, however many copies arrive", () => {
  const first = reduce(start, { _tag: "ProbeTriggered", sequence: 4 });
  assert.deepEqual(first.actions, [{ _tag: "Probe" }]);
  assert.equal(first.next.probedSequence, 4);

  for (const sequence of [4, 3, 4]) {
    const repeat = reduce(first.next, { _tag: "ProbeTriggered", sequence });
    assert.deepEqual(repeat.actions, [], `sequence ${sequence} must not probe again`);
    assert.equal(repeat.next, first.next, "and must not touch the state at all");
  }

  assert.deepEqual(
    reduce(first.next, { _tag: "ProbeTriggered", sequence: 5 }).actions,
    [{ _tag: "Probe" }],
    "a later transition is a different probe",
  );
});

test("a redrive trigger dedupes the same way — one replay per recovery", () => {
  const first = reduce(start, { _tag: "RedriveTriggered", sequence: 9 });
  assert.deepEqual(first.actions, [{ _tag: "Redrive" }]);
  assert.deepEqual(
    reduce(first.next, { _tag: "RedriveTriggered", sequence: 9 }).actions,
    [],
  );
});

/**
 * The periodic sweep: a message can dead-letter while the circuit never
 * leaves CLOSED — a broker restart, an Envoy 503, a rolling redeploy — and
 * nothing else would ever replay it. `SweepTick` has no sequence to dedupe
 * on, so it is a `Redrive` exactly when the floor is CLOSED and asks, and a
 * no-op every other time.
 */
test("a sweep asks for a redrive only from the floor, only while CLOSED", () => {
  assert.deepEqual(
    reduce(start, { _tag: "SweepTick", isFloor: true }).actions,
    [{ _tag: "Redrive" }],
    "CLOSED and the floor: redrive",
  );
  assert.deepEqual(
    reduce(start, { _tag: "SweepTick", isFloor: false }).actions,
    [],
    "CLOSED but not the floor: nothing",
  );
});

test("a sweep does nothing outside CLOSED, whoever holds the floor", () => {
  for (const circuitState of [State.OPEN, State.DEGRADED, State.HALF_OPEN]) {
    const notClosed = reduce(start, {
      _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
      state: circuitState,
      sequence: 1,
      at: T0,
    }).next;
    assert.deepEqual(
      reduce(notClosed, { _tag: "SweepTick", isFloor: true }).actions,
      [],
      `${circuitState} must not redrive even from the floor`,
    );
  }
});

test("REDRIVE_ON_CLOSE off means a sweep never redrives either", () => {
  assert.deepEqual(
    reduce(start, { _tag: "SweepTick", isFloor: true }, false).actions,
    [],
  );
});

test("a sweep never touches state — no sequence to dedupe on, and none is spent", () => {
  const redriven = reduce(start, { _tag: "RedriveTriggered", sequence: 3 }).next;
  const { next, actions } = reduce(redriven, { _tag: "SweepTick", isFloor: true });
  assert.deepEqual(actions, [{ _tag: "Redrive" }], "a sweep still asks for a redrive");
  assert.equal(next, redriven, "and the state, redrivenSequence included, is untouched");
});

test("the ramp advances only while CLOSED, and only when the dwell has elapsed", () => {
  const open = reduce(start, { _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(), state: State.OPEN, sequence: 1, at: T0 })
    .next;
  assert.equal(
    reduce(open, { _tag: "RampTick", at: T0 + 60_000 }).next.policy.fraction,
    0,
    "OPEN is a level, not a ramp",
  );

  const closed = reduce(open, {
    _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
    state: State.CLOSED,
    sequence: 2,
    at: T0 + 1000,
  }).next;
  assert.equal(closed.policy.fraction, 0, "first rung on recovery is the elected daemon alone");
  assert.equal(closed.policy.floor, true);

  const tooSoon = reduce(closed, { _tag: "RampTick", at: T0 + 1500 });
  assert.equal(tooSoon.next.policy.fraction, 0, "a rung is earned by being held");

  const earned = reduce(closed, { _tag: "RampTick", at: T0 + 1000 + RAMP_DWELL_MS });
  assert.equal(earned.next.policy.fraction, 0.25);
});

// ---------------------------------------------------------------------------
// Which connections should exist, and what to do about the ones that do.
// ---------------------------------------------------------------------------

test("HALF_OPEN never lets a daemon take work, wherever it sits in the space", () => {
  const half = reduce(start, {
    _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(),
    state: State.HALF_OPEN,
    sequence: 3,
    at: T0,
  }).next;
  // A daemon low enough in the hash space, or holding the floor, would
  // otherwise self-activate and race the one the broker elected — two calls in
  // the one state whose contract is "exactly one".
  for (const self of [
    { position: 0, isFloor: false },
    { position: 0, isFloor: true },
    { position: 0.99, isFloor: true },
  ]) {
    assert.equal(DaemonState.desired(half, self).work, false);
  }
  assert.equal(
    DaemonState.desired(half, { position: 0, isFloor: false }).probe,
    true,
    "but a probe may exist",
  );
});

test("work follows the fraction, and the floor runs whoever the broker elected", () => {
  const closed = { ...start, policy: { fraction: 0.5, floor: true, rungSince: T0 } };
  const works = (position: number, isFloor = false) =>
    DaemonState.desired(closed, { position, isFloor }).work;
  assert.deepEqual(
    [works(0.1), works(0.49), works(0.5), works(0.9)],
    [true, true, false, false],
    "the fraction is a threshold on the daemon's own position",
  );
  assert.equal(works(0.9, true), true, "except for the one holding the floor");
});

test("a probe belongs to HALF_OPEN and a redrive to CLOSED, and to nothing else", () => {
  const inState = (state: State) =>
    DaemonState.desired(
      reduce(start, { _tag: "CircuitChanged",
    type: SEQUENCED_EVENT,
    lease: O.none(), state, sequence: 1, at: T0 }).next,
      { position: 0, isFloor: false },
    );
  assert.deepEqual(
    { probe: inState(State.OPEN).probe, redrive: inState(State.OPEN).redrive },
    { probe: false, redrive: false },
  );
  assert.equal(inState(State.HALF_OPEN).probe, true);
  assert.equal(inState(State.HALF_OPEN).redrive, false);
  assert.equal(inState(State.CLOSED).redrive, true);
});

/**
 * The asymmetry is the point: reconcile opens work connections and only ever
 * *retires* probe and redrive ones. Those are opened by whichever daemon the
 * broker elected, in response to a trigger — if this ever plans to start one,
 * two daemons will be probing.
 */
test("planning starts work but never starts a probe or a redrive", () => {
  const want = { work: true, probe: true, redrive: true };
  const have = { work: false, probe: false, redrive: false };
  assert.deepEqual(DaemonState.plan(want, have), {
    startWork: true,
    stopWork: false,
    stopProbe: false,
    stopRedrive: false,
  });
});

test("planning retires every connection whose state is gone", () => {
  const want = { work: false, probe: false, redrive: false };
  const have = { work: true, probe: true, redrive: true };
  assert.deepEqual(DaemonState.plan(want, have), {
    startWork: false,
    stopWork: true,
    stopProbe: true,
    stopRedrive: true,
  });
});

test("planning does nothing when the world already matches", () => {
  const both = { work: true, probe: false, redrive: true };
  assert.deepEqual(DaemonState.plan(both, both), {
    startWork: false,
    stopWork: false,
    stopProbe: false,
    stopRedrive: false,
  });
});

const lease = (counter: number, epoch = "e1") => O.some({ epoch, counter });
const changed = (
  state: State,
  sequence: number,
  leaseOf: O.Option<Lease>,
  type: typeof SEQUENCED_EVENT | typeof SNAPSHOT_EVENT = SEQUENCED_EVENT,
): DaemonState.Command => ({ _tag: "CircuitChanged", type, lease: leaseOf, state, sequence, at: T0 });

/**
 * A leader paused past its lease resumes and publishes the event it was about
 * to, reusing a sequence its successor has already published with another state.
 * The fence on the checkpoint stops it a moment later; this is what stops the
 * fleet obeying it in between.
 */
test("a paused leader's event, out-ranked by its successor's lease, is ignored", () => {
  const successor = reduce(start, changed(State.CLOSED, 42, lease(8))).next;
  const stale = reduce(successor, changed(State.OPEN, 42, lease(7)));
  assert.equal(stale.next, successor, "nothing changes");
  assert.deepEqual(stale.actions, []);
  assert.equal(stale.ignored, true, "and the reducer says so, rather than the caller comparing objects");

  const staleAhead = reduce(successor, changed(State.OPEN, 43, lease(7)));
  assert.equal(staleAhead.next.circuit, State.CLOSED, "an older lease loses even with a higher sequence");
});

test("a successor's event wins over its predecessor's, even at the same sequence", () => {
  const predecessor = reduce(start, changed(State.OPEN, 42, lease(7))).next;
  const successor = reduce(predecessor, changed(State.CLOSED, 42, lease(8))).next;
  assert.equal(successor.circuit, State.CLOSED);
});

test("within one leader, a transition must move forward and a snapshot must not move back", () => {
  const applied = reduce(start, changed(State.OPEN, 10, lease(3))).next;
  assert.equal(reduce(applied, changed(State.CLOSED, 10, lease(3))).next, applied, "a repeated transition");
  assert.equal(reduce(applied, changed(State.CLOSED, 9, lease(3))).next, applied, "an out-of-order one");
  assert.equal(
    reduce(applied, changed(State.CLOSED, 9, lease(3), SNAPSHOT_EVENT)).next,
    applied,
    "a snapshot from behind",
  );
  assert.equal(
    reduce(applied, changed(State.OPEN, 10, lease(3), SNAPSHOT_EVENT)).next.circuit,
    State.OPEN,
    "a snapshot of the current sequence still applies",
  );
});

/** Without this, a coordinator that lost its state would restart sequences at 0 and be ignored forever. */
test("a new epoch is accepted, even with a lower counter and sequence", () => {
  const old = reduce(start, changed(State.OPEN, 500, lease(9, "e1"))).next;
  const fresh = reduce(old, changed(State.CLOSED, 0, lease(1, "e2"), SNAPSHOT_EVENT)).next;
  assert.equal(fresh.circuit, State.CLOSED);
});

/**
 * A trigger is marked handled before its action runs, so a failed action has to
 * un-mark it: otherwise the requeued trigger, and every other daemon's copy of
 * it, is dropped as a duplicate and that transition never gets its probe.
 */
test("a failed probe un-marks its sequence, so the retried trigger probes", () => {
  const probed = reduce(start, { _tag: "ProbeTriggered", sequence: 9 }).next;
  const failed = reduce(probed, { _tag: "TriggerFailed", election: "probe", sequence: 9 }).next;
  assert.deepEqual(reduce(failed, { _tag: "ProbeTriggered", sequence: 9 }).actions, [{ _tag: "Probe" }]);
});

test("a failed redrive un-marks only its own election", () => {
  const both = reduce(reduce(start, { _tag: "ProbeTriggered", sequence: 4 }).next, {
    _tag: "RedriveTriggered",
    sequence: 4,
  }).next;
  const failed = reduce(both, { _tag: "TriggerFailed", election: "redrive", sequence: 4 }).next;
  assert.equal(failed.redrivenSequence, 3);
  assert.equal(failed.probedSequence, 4, "the probe election is untouched");
});

test("a late failure does not un-mark a newer transition", () => {
  const newer = reduce(start, { _tag: "ProbeTriggered", sequence: 12 }).next;
  const staleFailure = reduce(newer, { _tag: "TriggerFailed", election: "probe", sequence: 9 }).next;
  assert.equal(staleFailure.probedSequence, 12);
});

const appliedBy = (state: DaemonState.DaemonState, type: typeof SEQUENCED_EVENT | typeof SNAPSHOT_EVENT, circuit: State, sequence: number, lease: O.Option<Lease> = O.none()) =>
  reduce(state, { _tag: "CircuitChanged", type, lease, state: circuit, sequence, at: T0 }).next;

test("an event stays current through a snapshot that repeats it", () => {
  const halfOpen = appliedBy(start, SEQUENCED_EVENT, State.HALF_OPEN, 4);
  const repeated = appliedBy(halfOpen, SNAPSHOT_EVENT, State.HALF_OPEN, 4);
  assert.ok(DaemonState.isCurrent(repeated, halfOpen.applied));
});

test("an event is no longer current once a newer one is applied", () => {
  const halfOpen = appliedBy(start, SEQUENCED_EVENT, State.HALF_OPEN, 4);
  assert.ok(!DaemonState.isCurrent(appliedBy(halfOpen, SEQUENCED_EVENT, State.OPEN, 5), halfOpen.applied));
  const newLeader = appliedBy(halfOpen, SNAPSHOT_EVENT, State.HALF_OPEN, 4, O.some({ epoch: "e2", counter: 1 }));
  assert.ok(!DaemonState.isCurrent(newLeader, halfOpen.applied), "same sequence under another lease is another event");
});
