import {
  circuitBreaker,
  CircuitState,
  ConsecutiveBreaker,
  ExponentialBackoff,
  handleAll,
  isBrokenCircuitError,
} from "cockatiel";
import type { CircuitBreakerPolicy } from "cockatiel";

/**
 * One breaker per process, shared across every message it handles — never
 * recreated per call, per cockatiel's own warning that a breaker only works
 * when the same instance sees every execution. Five `docker compose`
 * replicas means five of these, each with entirely private state: this
 * module has no way to know what any other replica thinks, and doesn't try
 * to. That absence is deliberate — see README.md.
 *
 * `ConsecutiveBreaker` trips after N calls fail in a row, which is the direct
 * realization of the definition this article series opened with: "a
 * threshold on recent failures, never a single failure." A percentage/window
 * breaker (cockatiel's `SamplingBreaker`) is closer to what a fleet-wide
 * verdict needs once there is a fleet-wide verdict to compute — not yet.
 *
 * `ExponentialBackoff` is what gives `halfOpenAfter` its growth: cockatiel's
 * own defaults (128ms initial, exponent 2, 30s max, decorrelated-jitter
 * generator) already are "exponential backoff and jitter" — this only
 * overrides the two bounds to values sized for this scenario.
 */

export type BreakerConfig = {
  /** Consecutive failures before the breaker opens. */
  readonly consecutiveFailures: number;
  /** First half-open probe, in milliseconds. */
  readonly initialDelayMs: number;
  /** Ceiling the backoff grows to. */
  readonly maxDelayMs: number;
};

export const make = (cfg: BreakerConfig): CircuitBreakerPolicy =>
  circuitBreaker(handleAll, {
    breaker: new ConsecutiveBreaker(cfg.consecutiveFailures),
    halfOpenAfter: new ExponentialBackoff({
      initialDelay: cfg.initialDelayMs,
      maxDelay: cfg.maxDelayMs,
    }),
  });

/** `CircuitState.Isolated` (manual hold) never happens here — nothing calls `.isolate()`. */
export const STATE_CODE: Record<CircuitState, number> = {
  [CircuitState.Closed]: 0,
  [CircuitState.Open]: 1,
  [CircuitState.HalfOpen]: 2,
  [CircuitState.Isolated]: 3,
};

/** What every breaker starts as, before its first `onStateChange` fires. */
export const INITIAL_STATE_CODE: number = STATE_CODE[CircuitState.Closed];

export { isBrokenCircuitError };
