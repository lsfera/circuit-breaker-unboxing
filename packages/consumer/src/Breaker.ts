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
 * One breaker per process, shared across every message: a breaker only works when the same instance sees
 * every execution, and a fresh one per call would never accumulate a failure count. Five replicas means five
 * of these with private state; nothing here knows what another replica thinks.
 *
 * `ConsecutiveBreaker` trips after N calls fail in a row ("a threshold on recent failures, never a single
 * failure"). `ExponentialBackoff` grows `halfOpenAfter`; cockatiel's defaults (128ms initial, exponent 2,
 * decorrelated jitter) already are exponential backoff with jitter, so only the two bounds are overridden.
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
