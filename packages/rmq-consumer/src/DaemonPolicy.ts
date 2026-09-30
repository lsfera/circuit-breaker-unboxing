import { Match } from "effect";
import { createHash } from "node:crypto";
import { State } from "@egress/domain/Model.ts";

/**
 * Pure fleet policy: a total function of (prior, circuit state, now). The target
 * is a proportion each daemon applies to itself, so nothing needs a fleet size or
 * an index (ADR 013); `floor` stops "approximately half" from meaning none.
 */

/** What proportion of the fleet should be pulling from the work queue right now. */
export type DaemonPolicyState = {
  /** 0 to 1. A daemon runs when its own position in the hash space falls below this. */
  readonly fraction: number;
  /** One daemon, elected by the broker, runs regardless: hashing alone ran zero of five 3.2% of the time. */
  readonly floor: boolean;
  /** Epoch millis the rung was entered; passed in so `step` stays pure. */
  readonly rungSince: number;
};

/**
 * Before the first event: nobody works, the floor included. A daemon that starts
 * mid-outage must not spend attempt budgets against a failing upstream while it
 * waits for a snapshot (ADR 019). The first CLOSED starts the ramp from the floor.
 */
export const initial = (now: number): DaemonPolicyState => ({
  fraction: 0,
  floor: false,
  rungSince: now,
});

/**
 * With the control plane silent, a quarter of the fleet by position works. No
 * floor: its election rides on the events that stopped. Envoy still ejects and
 * sheds locally, so this is load a failing upstream can refuse, and zero would
 * be a fleet stalled for as long as the silence lasts (ADR 019).
 */
export const SILENT_FRACTION = 0.25;

export const silent = (now: number): DaemonPolicyState => ({
  fraction: SILENT_FRACTION,
  floor: false,
  rungSince: now,
});

/**
 * One daemon, then a quarter, a half, all — never straight back to the whole fleet.
 * "One" is fraction 0 with the floor on: only the elected daemon runs.
 */
const RAMP_SCHEDULE: ReadonlyArray<number> = [0, 0.25, 0.5, 1];

/** Elapsed time, not successes: a clock is the only input every daemon shares. */
export const RAMP_DWELL_MS = 5000;

/** Called on every control event and on a timer, since a quiet recovery publishes nothing to advance the ramp. */
export const step = (
  prior: DaemonPolicyState,
  circuitState: State,
  now: number,
): DaemonPolicyState => {
  return Match.value(circuitState).pipe(
    // From OPEN or a probe (floor off), start at one daemon; from DEGRADED, continue.
    Match.when(State.CLOSED, () => {
      if (!prior.floor) return { fraction: RAMP_SCHEDULE[0] ?? 0, floor: true, rungSince: now };
      if (prior.fraction >= 1) return { fraction: 1, floor: true, rungSince: prior.rungSince };
      if (now - prior.rungSince < RAMP_DWELL_MS) return prior;
      const next = RAMP_SCHEDULE.find((rung) => rung > prior.fraction) ?? 1;
      return { fraction: next, floor: true, rungSince: now };
    }),
    Match.when(State.DEGRADED, () => ({ fraction: 0.5, floor: true, rungSince: now })),
    Match.when(State.OPEN, () => ({ fraction: 0, floor: false, rungSince: now })),
    // Only the elected prober calls.
    Match.when(State.HALF_OPEN, () => ({ fraction: 0, floor: false, rungSince: now })),
    Match.exhaustive,
  );
};

/**
 * A daemon's fixed place in [0, 1), so lowering the fraction idles a subset of
 * those running rather than reshuffling them.
 */
export const position = (instanceId: string): number => {
  // SHA-256: FNV-1a put daemon-0…daemon-5 on a line covering 2% of the space.
  const digest = createHash("sha256").update(instanceId).digest();
  return digest.readUInt32BE(0) / 0x100000000;
};

/**
 * Whether this daemon runs, given the fleet's target and whether the broker has
 * elected it as the floor.
 */
export const runsWork = (
  policy: DaemonPolicyState,
  self: { readonly position: number; readonly isFloor: boolean },
): boolean => self.position < policy.fraction || (policy.floor && self.isFloor);

