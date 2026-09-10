import { Option as O } from "effect";
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

const voteOf = (slot: ReplicaSlot): Vote => {
  const { healthy, total } = slot.report;
  if (total > 0 && healthy === 0) return Vote.DOWN;
  if (healthy < total) return Vote.DEGRADED;
  // No hosts ejected, but the replica is shedding on its own thresholds.
  if (slot.overflowSinceTick > 0) return Vote.DEGRADED;
  return Vote.OK;
};

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
 * Advance the machine one tick. Returns the next state and, if the fleet's
 * verdict changed, the transition that should be published.
 */
export const step = (
  self: BreakerState,
  now: number,
  cfg: AggregatorConfig,
): [BreakerState, O.Option<Transition>] => {
  const replicas = new Map<string, ReplicaSlot>();
  const live: ReplicaSlot[] = [];
  for (const [id, slot] of self.replicas) {
    if (now - slot.report.observedAt > cfg.replicaTimeoutMs) continue;
    live.push(slot);
    replicas.set(id, slot);
  }
  if (live.length === 0) return [{ ...self, replicas }, O.none()];

  const votes: Record<Vote, number> = { OK: 0, DEGRADED: 0, DOWN: 0 };
  let healthy = 0;
  let total = 0;
  let overflowDrove = false;

  for (const [id, slot] of replicas) {
    const vote = voteOf(slot);
    votes[vote] += 1;
    if (vote === Vote.DEGRADED && slot.report.healthy === slot.report.total) {
      overflowDrove = true;
    }
    healthy += slot.report.healthy;
    total += slot.report.total;
    replicas.set(id, { ...slot, vote, overflowSinceTick: 0 }); // consumed
  }

  const downFrac = votes.DOWN / live.length;
  const impairedFrac = (votes.DOWN + votes.DEGRADED) / live.length;
  const candidate: State =
    downFrac >= cfg.quorum
      ? State.OPEN
      : impairedFrac >= cfg.quorum
        ? State.DEGRADED
        : State.CLOSED;

  let next: BreakerState = {
    ...self,
    replicas,
    lastVotes: votes,
    lastHealthy: Math.round(healthy / live.length),
    lastTotal: Math.round(total / live.length),
    candidate,
    candidateSince:
      candidate !== self.candidate ? now : self.candidateSince,
  };

  const dwelled = now - next.candidateSince >= cfg.dwellMs;
  const settled = now - next.changedAt >= cfg.minStateMs;

  // --- OPEN: wait out the backoff, then probe. ---------------------------
  if (next.state === State.OPEN) {
    return now - next.changedAt >= next.openBackoffMs
      ? transition(next, now, State.HALF_OPEN, Reason.OPEN_TIMEOUT_ELAPSED)
      : [next, O.none()];
  }

  // --- HALF_OPEN: exactly one owner probes. ------------------------------
  if (next.state === State.HALF_OPEN) {
    if (candidate === State.OPEN && dwelled) {
      next = {
        ...next,
        probeStreak: 0,
        openBackoffMs: Math.min(next.openBackoffMs * 2, cfg.maxOpenMs),
      };
      return transition(next, now, State.OPEN, Reason.PROBE_FAILED);
    }
    if (candidate === State.CLOSED) {
      const probeStreak = next.probeStreak + 1;
      next = { ...next, probeStreak };
      if (probeStreak >= cfg.probeSuccesses) {
        next = { ...next, openBackoffMs: cfg.openMs };
        return transition(next, now, State.CLOSED, Reason.PROBE_SUCCEEDED);
      }
      return [next, O.none()];
    }
    return [{ ...next, probeStreak: 0 }, O.none()];
  }

  // --- CLOSED / DEGRADED -------------------------------------------------
  if (candidate === next.state || !dwelled || !settled) return [next, O.none()];

  if (candidate === State.OPEN) {
    // Unanimous, not merely quorate: every reporting replica sees zero hosts.
    const allGone = votes.DOWN === live.length;
    return transition(
      next,
      now,
      State.OPEN,
      allGone ? Reason.ALL_ENDPOINTS_EJECTED : Reason.OUTLIER_EJECTION,
    );
  }
  if (candidate === State.DEGRADED) {
    return transition(
      next,
      now,
      State.DEGRADED,
      overflowDrove ? Reason.THRESHOLD_OVERFLOW : Reason.OUTLIER_EJECTION,
    );
  }
  return transition(next, now, State.CLOSED, Reason.HEALTHY);
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
