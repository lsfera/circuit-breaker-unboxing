import { State } from "@egress/domain/Model.ts";

/**
 * Pure decision logic for the daemon fleet — no Effect, no RabbitMQ, no
 * clock, same philosophy as @egress/domain's Breaker.step: a total function
 * of (prior, circuitState, fleetSize), testable exhaustively with plain
 * assert. Everything hard to reason about (the actual AMQP connections,
 * which physical daemon is active, SAC election) lives in daemon.ts, above
 * this.
 *
 * The state -> action mapping is built entirely on two RabbitMQ primitives
 * verified live against a real broker (see docs/rmq-control-plane.md):
 * opening/closing a consumer, and x-single-active-consumer election. There
 * is deliberately no per-consumer credit/prefetch lever here — the pinned
 * client (rabbitmq-amqp-js-client) does not expose one, and AMQP 1.0 has no
 * "prefetch" to fall back to; see the doc's terminology note. DEGRADED is
 * therefore a fleet-size decision (how many daemons are active), not a
 * per-daemon one.
 */

/** How many daemons should be pulling from the work queue right now. */
export type DaemonPolicyState = {
  readonly targetActive: number;
};

export const initial = (fleetSize: number): DaemonPolicyState => ({
  targetActive: fleetSize,
});

/**
 * Recovery ramp: 1 -> 4 -> 16 -> fleetSize, one rung per policy tick spent
 * in CLOSED. Snapping straight back to the full fleet the instant OPEN
 * clears is the thundering herd this design exists to prevent — the ramp is
 * the actual fix, not the cutoff on OPEN.
 */
export const RAMP_SCHEDULE: ReadonlyArray<number> = [1, 4, 16];

/**
 * Advance one policy tick. A "tick" is any circuit.control message the
 * daemon fleet observes for its apiId — a state_changed transition, or one
 * of the aggregator's periodic snapshots. Snapshots repeating the current
 * state is what lets the ramp advance during a quiet recovery period even
 * without a fresh transition; that is not incidental, it is the reason
 * `snapshotMs` matters to this side of the system too.
 */
export const step = (
  prior: DaemonPolicyState,
  circuitState: State,
  fleetSize: number,
): DaemonPolicyState => {
  switch (circuitState) {
    case State.CLOSED: {
      if (prior.targetActive >= fleetSize) return { targetActive: fleetSize };
      const next = RAMP_SCHEDULE.find((rung) => rung > prior.targetActive) ?? fleetSize;
      return { targetActive: Math.min(next, fleetSize) };
    }
    case State.DEGRADED:
      return { targetActive: Math.max(1, Math.ceil(fleetSize / 2)) };
    case State.OPEN:
      return { targetActive: 0 };
    case State.HALF_OPEN:
      // Exactly one prober — which physical daemon that is comes from SAC
      // election in daemon.ts, not from this count. Any daemon whose own
      // index happens to fall under this target must still defer to SAC
      // rather than self-activate; see daemon.ts's HALF_OPEN branch.
      return { targetActive: 1 };
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
