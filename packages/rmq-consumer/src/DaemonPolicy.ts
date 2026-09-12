import { createHash } from "node:crypto";
import { State } from "@egress/domain/Model.ts";

/**
 * Pure decision logic for the fleet: a total function of (prior, circuitState,
 * now), with the AMQP lifetimes and the SAC election left to daemon.ts above it.
 *
 * DEGRADED scales whole daemons rather than each daemon's prefetch. Prefetch is
 * fixed when a consumer is created, so changing it live means cancelling and
 * re-registering — the same operation as retiring a daemon, at more risk.
 *
 * The target is a *proportion* of the fleet, not a count of it, and that is the
 * whole reason this module has no `fleetSize` parameter. A count has to be
 * turned into a set, which needs each daemon to hold a unique index and to
 * agree on how many there are — two pieces of static configuration that cannot
 * change without restarting every replica. A proportion each daemon can apply
 * to itself needs neither. See ADR 013 for what that costs: the proportion is
 * only approximately realised, and `floor` is what stops "approximately" from
 * ever meaning zero.
 */

/** What proportion of the fleet should be pulling from the work queue right now. */
export type DaemonPolicyState = {
  /** 0 to 1. A daemon runs when its own position in the hash space falls below this. */
  readonly fraction: number;
  /**
   * Whether one daemon must run regardless of the fraction. Hash selection is
   * independent per daemon, so a small fleet can land on nobody — measured at
   * 3.2% for five daemons at 0.5 — and a DEGRADED fleet that stops entirely is
   * indistinguishable from an OPEN one. The broker elects which daemon; this
   * says only that there has to be one.
   */
  readonly floor: boolean;
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

export const initial = (now: number): DaemonPolicyState => ({
  fraction: 1,
  floor: true,
  rungSince: now,
});

/**
 * Recovery ramp: one daemon, then a quarter, a half, all of them. Snapping
 * straight back to the full fleet the instant OPEN clears is the thundering
 * herd this design exists to prevent — the ramp is the actual fix, not the
 * cutoff on OPEN.
 *
 * The first rung is `0` with the floor on, which is how "exactly one" is said
 * without knowing how many there are: no daemon passes a fraction of zero, so
 * the only one running is the one the broker elected. The old schedule was
 * 1 -> 4 -> 16 -> fleetSize, which is the same curve wherever the fleet is
 * bigger than sixteen and a shorter one below that.
 */
const RAMP_SCHEDULE: ReadonlyArray<number> = [0, 0.25, 0.5, 1];

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
  now: number,
): DaemonPolicyState => {
  switch (circuitState) {
    case State.CLOSED: {
      // Arriving from a state that was not ramping — OPEN, or a probe — starts
      // at once, at one daemon. `floor` is what distinguishes "the first rung"
      // from "not running", since both have a fraction of zero. Arriving from
      // DEGRADED continues the ramp from where it already is rather than
      // dropping back to one.
      if (!prior.floor) return { fraction: RAMP_SCHEDULE[0] ?? 0, floor: true, rungSince: now };
      if (prior.fraction >= 1) return { fraction: 1, floor: true, rungSince: prior.rungSince };
      if (now - prior.rungSince < RAMP_DWELL_MS) return prior;
      const next = RAMP_SCHEDULE.find((rung) => rung > prior.fraction) ?? 1;
      return { fraction: next, floor: true, rungSince: now };
    }
    case State.DEGRADED:
      // Half the fleet, and never none of it: the floor is the reason this can
      // be a fraction at all.
      return { fraction: 0.5, floor: true, rungSince: now };
    case State.OPEN:
      return { fraction: 0, floor: false, rungSince: now };
    case State.HALF_OPEN:
      // No daemon pulls work here. The one call this state permits is the
      // prober's, elected by the broker, and `desired` refuses the work
      // consumer outright — so there is nothing for a fraction to say.
      return { fraction: 0, floor: false, rungSince: now };
  }
};

/**
 * Where a daemon sits in the hash space, from its instance id. Stable for the
 * life of that identity, independent of every other daemon, and uniform enough
 * that a fraction selects about that proportion of any fleet.
 *
 * This is the index's replacement, and it keeps the property the index was
 * bought for: the ordering is fixed, so a fraction moving from 0.5 to 0.25
 * idles a subset of the daemons already running rather than reshuffling which
 * ones work.
 */
export const position = (instanceId: string): number => {
  // SHA-256, and the reason is measured rather than cautious. FNV-1a was tried
  // first — cheap, no imports — and it does not mix ids that differ in one
  // character, which is exactly what fleet instance ids are: `daemon-0` through
  // `daemon-5` came out as 0.9426, 0.9465, 0.9504, 0.9543, 0.9582, 0.9621, a
  // straight line covering 2% of the space. A fraction applied to that selects
  // the wrong proportion *systematically*, which is worse than selecting it
  // noisily, and silently.
  //
  // This runs once per process, at startup, so nothing is being paid for the
  // stronger hash — and it is the same function the simulation behind ADR 013
  // measured, so those numbers describe this code.
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

