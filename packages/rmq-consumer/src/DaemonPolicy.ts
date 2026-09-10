import { State } from "@egress/domain/Model.ts";

/**
 * Pure decision logic for the fleet: a total function of (prior, circuitState,
 * fleetSize, now), with the AMQP lifetimes and the SAC election left to
 * daemon.ts above it.
 *
 * DEGRADED scales whole daemons rather than each daemon's prefetch. Prefetch is
 * fixed when a consumer is created, so changing it live means cancelling and
 * re-registering — the same operation as retiring a daemon, at more risk — and
 * a daemon count is something every daemon can agree on without coordination.
 */

/** How many daemons should be pulling from the work queue right now. */
export type DaemonPolicyState = {
  readonly targetActive: number;
  /**
   * When the current rung was entered, in epoch millis.
   *
   * Carried in the state rather than read from a clock inside `step`, because
   * this function staying total and pure is what makes the fleet's behaviour
   * exhaustively testable — and because the daemon that calls it already has
   * a clock.
   */
  readonly rungSince: number;
};

export const initial = (fleetSize: number, now: number): DaemonPolicyState => ({
  targetActive: fleetSize,
  rungSince: now,
});

/**
 * Recovery ramp: 1 -> 4 -> 16 -> fleetSize. Snapping straight back to the full
 * fleet the instant OPEN clears is the thundering herd this design exists to
 * prevent — the ramp is the actual fix, not the cutoff on OPEN.
 */
const RAMP_SCHEDULE: ReadonlyArray<number> = [1, 4, 16];

/**
 * How long a rung is held before the next is allowed.
 *
 * Elapsed time, not "N successful calls at this rung": every daemon must derive
 * the same target from the same inputs, and a success count is per daemon — the
 * busy ones would ramp while the idle ones held. A clock is the only input they
 * all share.
 */
export const RAMP_DWELL_MS = 5000;

/**
 * Advance the policy.
 *
 * Called on every `circuit.control` message the fleet observes for its apiId,
 * and also on the daemon's own timer — because a ramp gated on elapsed time
 * only advances if something asks it to, and a quiet recovery produces no
 * events for `snapshotMs` at a stretch.
 */
export const step = (
  prior: DaemonPolicyState,
  circuitState: State,
  fleetSize: number,
  now: number,
): DaemonPolicyState => {
  switch (circuitState) {
    case State.CLOSED: {
      if (prior.targetActive >= fleetSize) return { targetActive: fleetSize, rungSince: prior.rungSince };
      // The first rung is immediate: a circuit that just closed should start
      // doing work now, at one daemon. It is every rung *after* that which
      // has to be earned by holding the current one without relapsing.
      if (prior.targetActive > 0 && now - prior.rungSince < RAMP_DWELL_MS) return prior;
      const next = RAMP_SCHEDULE.find((rung) => rung > prior.targetActive) ?? fleetSize;
      return { targetActive: Math.min(next, fleetSize), rungSince: now };
    }
    case State.DEGRADED:
      return { targetActive: Math.max(1, Math.ceil(fleetSize / 2)), rungSince: now };
    case State.OPEN:
      return { targetActive: 0, rungSince: now };
    case State.HALF_OPEN:
      // Exactly one prober — which physical daemon that is comes from SAC
      // election in daemon.ts, not from this count. Any daemon whose own
      // index happens to fall under this target must still defer to SAC
      // rather than self-activate; see daemon.ts's HALF_OPEN branch.
      return { targetActive: 1, rungSince: now };
  }
};

/**
 * Which daemon indices (0-based, out of fleetSize) should be active for a
 * given target count — deterministic and stable so the *same* daemons stay
 * active as the target shrinks and grows, rather than reshuffling which
 * ones are idle on every tick.
 */
export const activeIndices = (targetActive: number, fleetSize: number): ReadonlySet<number> =>
  new Set(Array.from({ length: Math.min(targetActive, fleetSize) }, (_, i) => i));
