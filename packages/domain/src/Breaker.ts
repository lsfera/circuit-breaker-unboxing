import { Array as Arr, Option as O, Result } from "effect";
import { Reason, State, Vote } from "./Model.ts";
import type { AggregatorConfig, ApiSnapshot, ReplicaReport } from "./Model.ts";

/**
 * Authoritative breaker state for one API, aggregated across the whole fleet.
 *
 * This module is deliberately pure — no Effect, no clock, no I/O. `step` is a
 * total function of (state, reports, now) and every timing rule is expressed as
 * arithmetic on timestamps passed in. Everything that makes this hard to reason
 * about (concurrency, scheduling, delivery) lives in the Effect layer above it,
 * so the logic that actually decides what subscribers are told can be tested
 * exhaustively without a runtime.
 */

type ReplicaSlot = {
  readonly report: ReplicaReport;
  readonly lastOverflowTotal: number;
  readonly overflowSinceTick: number;
  readonly vote: Vote;
};

export type BreakerState = {
  readonly apiId: string;
  readonly state: State;
  readonly reason: Reason;
  readonly sequence: number;
  readonly changedAt: number;
  readonly candidate: State;
  readonly candidateSince: number;
  readonly probeStreak: number;
  readonly openBackoffMs: number;
  readonly replicas: ReadonlyMap<string, ReplicaSlot>;
  readonly lastVotes: Record<Vote, number>;
  readonly lastHealthy: number;
  readonly lastTotal: number;
};

export type Transition = {
  readonly from: State;
  readonly to: State;
  readonly reason: Reason;
};

/**
 * `now` seeds changedAt/candidateSince. Passing a real timestamp matters: these
 * are published as `observedSince`, and a zero here emits 1970 on the wire.
 */
export const initial = (
  apiId: string,
  cfg: AggregatorConfig,
  now = 0,
): BreakerState => ({
  apiId,
  state: State.CLOSED,
  reason: Reason.HEALTHY,
  sequence: 0,
  changedAt: now,
  candidate: State.CLOSED,
  candidateSince: now,
  probeStreak: 0,
  openBackoffMs: cfg.openMs,
  replicas: new Map(),
  lastVotes: { OK: 0, DEGRADED: 0, DOWN: 0 },
  lastHealthy: 0,
  lastTotal: 0,
});

export const ingest = (
  self: BreakerState,
  report: ReplicaReport,
): BreakerState => {
  const prev = self.replicas.get(report.replicaId);
  // Counters are monotonic; a decrease means the replica restarted, so rebase.
  const last = prev ? prev.lastOverflowTotal : report.overflowTotal;
  const delta = Math.max(0, report.overflowTotal - last);
  const replicas = new Map(self.replicas);
  replicas.set(report.replicaId, {
    report,
    lastOverflowTotal: report.overflowTotal,
    overflowSinceTick: (prev?.overflowSinceTick ?? 0) + delta,
    vote: prev?.vote ?? Vote.OK,
  });
  return { ...self, replicas };
};

const voteOf = ({ report: { healthy, total }, overflowSinceTick }: ReplicaSlot): Vote =>
  total > 0 && healthy === 0
    ? Vote.DOWN
    : // Either some hosts are ejected, or none are and the replica is still
      // shedding on its own thresholds. Both are the same vote.
      healthy < total || overflowSinceTick > 0
      ? Vote.DEGRADED
      : Vote.OK;

const transition = (
  self: BreakerState,
  now: number,
  to: State,
  reason: Reason,
): [BreakerState, O.Option<Transition>] => [
  {
    ...self,
    state: to,
    reason,
    sequence: self.sequence + 1,
    changedAt: now,
    probeStreak: to === State.HALF_OPEN ? self.probeStreak : 0,
  },
  O.some({ from: self.state, to, reason }),
];

/**
 * The fleet's verdict for this tick. Never `HALF_OPEN`: that is a state this
 * machine puts itself into to probe, not something replicas can report.
 */
type Candidate = Exclude<State, typeof State.HALF_OPEN>;

/** Everything a resolution needs beyond the state itself, derived once per tick. */
type Tick = {
  readonly cfg: AggregatorConfig;
  /** Has the candidate held long enough to be believed? */
  readonly dwelled: boolean;
  /** Has the current state been in place long enough to leave? */
  readonly settled: boolean;
  /** Every reporting replica sees zero hosts, not merely a quorum of them. */
  readonly allGone: boolean;
  /** The impairment came from replicas shedding on their own thresholds. */
  readonly overflowDrove: boolean;
};

/**
 * Why a verdict is what it is, keyed by the verdict itself rather than by the
 * state being left: a DEGRADED fleet means THRESHOLD_OVERFLOW or
 * OUTLIER_EJECTION whether it is reached from CLOSED or out of a probe, and
 * writing that rule once is the difference between the two agreeing and the
 * two being kept in step by hand.
 */
const REASON: Record<Candidate, (tick: Tick) => Reason> = {
  OPEN: ({ allGone }) => (allGone ? Reason.ALL_ENDPOINTS_EJECTED : Reason.OUTLIER_EJECTION),
  DEGRADED: ({ overflowDrove }) =>
    overflowDrove ? Reason.THRESHOLD_OVERFLOW : Reason.OUTLIER_EJECTION,
  CLOSED: () => Reason.HEALTHY,
};

/** One cell of the table below: what this (state, candidate) pair does. */
type Resolve = (
  self: BreakerState,
  now: number,
  tick: Tick,
) => [BreakerState, O.Option<Transition>];

const hold: Resolve = (self) => [self, O.none()];

/** OPEN ignores the fleet entirely until its backoff elapses, then probes. */
const waitOutBackoff: Resolve = (self, now) =>
  now - self.changedAt >= self.openBackoffMs
    ? transition(self, now, State.HALF_OPEN, Reason.OPEN_TIMEOUT_ELAPSED)
    : [self, O.none()];

/** The probe found the fleet still down: back to OPEN, waiting twice as long. */
const probeFailed: Resolve = (self, now, { dwelled, cfg }) => {
  const reset = { ...self, probeStreak: 0 };
  return dwelled
    ? transition(
        { ...reset, openBackoffMs: Math.min(self.openBackoffMs * 2, cfg.maxOpenMs) },
        now,
        State.OPEN,
        Reason.PROBE_FAILED,
      )
    : [reset, O.none()];
};

/** The probe found the fleet healthy — `probeSuccesses` times before believing it. */
const probeSucceeded: Resolve = (self, now, { cfg }) => {
  const probeStreak = self.probeStreak + 1;
  return probeStreak >= cfg.probeSuccesses
    ? transition(
        { ...self, probeStreak, openBackoffMs: cfg.openMs },
        now,
        State.CLOSED,
        Reason.PROBE_SUCCEEDED,
      )
    : [{ ...self, probeStreak }, O.none()];
};

/**
 * The probe found the fleet partly healthy, which is a verdict and not an
 * inconclusive result: DEGRADED is the state that says "keep pulling, at
 * reduced rate", and the daemon fleet acts on it (half the daemons, against
 * exactly one prober for HALF_OPEN).
 *
 * `openBackoffMs` is deliberately not reset the way PROBE_SUCCEEDED resets it.
 * The upstream is still impaired, so if this relapses to OPEN it should wait
 * out the backoff it had already earned rather than start over optimistically.
 */
const probeDegraded: Resolve = (self, now, tick) => {
  const reset = { ...self, probeStreak: 0 };
  return tick.dwelled
    ? transition(reset, now, State.DEGRADED, REASON.DEGRADED(tick))
    : [reset, O.none()];
};

/** A settled state moving to a different settled state, once both timers allow. */
const settleInto =
  (to: Candidate): Resolve =>
  (self, now, tick) =>
    tick.dwelled && tick.settled
      ? transition(self, now, to, REASON[to](tick))
      : [self, O.none()];

/**
 * The whole graph, as a table rather than a chain of `if`s: every state this
 * machine can be in, against every verdict the fleet can return.
 *
 * `Record<State, Record<Candidate, Resolve>>` is what makes it total. The
 * previous shape let a pair fall off the end of the HALF_OPEN branch and mean
 * "stay put", which is how HALF_OPEN x DEGRADED became a state the machine
 * could enter and never leave — a half-healthy upstream held the fleet at one
 * prober indefinitely while DEGRADED, the state that exists for exactly that,
 * sat unreachable. Falling through is now spelt `hold`, and a missing pair is
 * a compile error.
 */
const RESOLVE: Record<State, Record<Candidate, Resolve>> = {
  OPEN: { CLOSED: waitOutBackoff, DEGRADED: waitOutBackoff, OPEN: waitOutBackoff },
  HALF_OPEN: { CLOSED: probeSucceeded, DEGRADED: probeDegraded, OPEN: probeFailed },
  CLOSED: { CLOSED: hold, DEGRADED: settleInto(State.DEGRADED), OPEN: settleInto(State.OPEN) },
  DEGRADED: { CLOSED: settleInto(State.CLOSED), DEGRADED: hold, OPEN: settleInto(State.OPEN) },
};

/**
 * Advance the machine one tick. Returns the next state and, if the fleet's
 * verdict changed, the transition that should be published.
 */
export const step = (
  self: BreakerState,
  now: number,
  cfg: AggregatorConfig,
): [BreakerState, O.Option<Transition>] => {
  // Each surviving replica keeps its vote for this tick, and its accumulated
  // overflow is consumed by being counted here.
  const voted = Arr.filterMap([...self.replicas], ([id, slot]) =>
    now - slot.report.observedAt > cfg.replicaTimeoutMs
      ? Result.fail(id)
      : Result.succeed([id, { ...slot, vote: voteOf(slot), overflowSinceTick: 0 }] as const),
  );

  return Arr.match(voted, {
    // Nothing is reporting: hold the last verdict rather than invent one from
    // an empty fleet.
    onEmpty: () => [{ ...self, replicas: new Map() }, O.none()],
    onNonEmpty: (live) => {
      const { votes, healthy, total, overflowDrove } = Arr.reduce(
        live,
        {
          votes: { OK: 0, DEGRADED: 0, DOWN: 0 } as Record<Vote, number>,
          healthy: 0,
          total: 0,
          overflowDrove: false,
        },
        (acc, [, slot]) => ({
          votes: { ...acc.votes, [slot.vote]: acc.votes[slot.vote] + 1 },
          healthy: acc.healthy + slot.report.healthy,
          total: acc.total + slot.report.total,
          // No hosts ejected, yet the replica still votes DEGRADED: the
          // impairment came from its own shedding thresholds.
          overflowDrove:
            acc.overflowDrove ||
            (slot.vote === Vote.DEGRADED && slot.report.healthy === slot.report.total),
        }),
      );

      const reporting = live.length;
      const candidate: Candidate =
        votes.DOWN / reporting >= cfg.quorum
          ? State.OPEN
          : (votes.DOWN + votes.DEGRADED) / reporting >= cfg.quorum
            ? State.DEGRADED
            : State.CLOSED;

      const next: BreakerState = {
        ...self,
        replicas: new Map(live),
        lastVotes: votes,
        lastHealthy: Math.round(healthy / reporting),
        lastTotal: Math.round(total / reporting),
        candidate,
        candidateSince: candidate !== self.candidate ? now : self.candidateSince,
      };

      return RESOLVE[next.state][candidate](next, now, {
        cfg,
        dwelled: now - next.candidateSince >= cfg.dwellMs,
        settled: now - next.changedAt >= cfg.minStateMs,
        allGone: votes.DOWN === reporting,
        overflowDrove,
      });
    },
  });
};

export const snapshot = (self: BreakerState): ApiSnapshot => ({
  apiId: self.apiId,
  state: self.state,
  reason: self.reason,
  sequence: self.sequence,
  healthyEndpoints: self.lastHealthy,
  totalEndpoints: self.lastTotal,
  reportingReplicas: self.replicas.size,
  votes: { ...self.lastVotes },
  observedSince: self.candidateSince,
  changedAt: self.changedAt,
  replicas: [...self.replicas.values()]
    .map((s) => ({
      replicaId: s.report.replicaId,
      vote: s.vote,
      healthy: s.report.healthy,
      total: s.report.total,
      ejectionsActive: s.report.ejectionsActive,
    }))
    .sort((a, b) => a.replicaId.localeCompare(b.replicaId)),
});
