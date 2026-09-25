import {
  circuitBreaker,
  CircuitState,
  ConsecutiveBreaker,
  ExponentialBackoff,
  handleAll,
  isBrokenCircuitError,
} from "cockatiel";
import type { CircuitBreakerPolicy } from "cockatiel";
import { isCallStatus } from "./Upstream.ts";
import type { CallStatus } from "./Upstream.ts";

/**
 * One breaker per process, shared across every message: a breaker only works when the same instance sees
 * every execution, and a fresh one per call would never accumulate a failure count. Five replicas means five
 * of these with private state; nothing here knows what another replica thinks.
 *
 * `ConsecutiveBreaker` trips after N calls fail in a row ("a threshold on recent failures, never a single
 * failure"). `ExponentialBackoff` grows `halfOpenAfter`; cockatiel's defaults (128ms initial, exponent 2,
 * decorrelated jitter) already are exponential backoff with jitter, so only the two bounds are overridden.
 *
 * The policy is also where a third party's answer is judged (`classify`): a call fails the breaker by
 * throwing or by coming back `failed`, and a `client_error` is a success, because the third party answered.
 */

/**
 * `ok` is a 2xx. `client_error` is a 4xx other than 408 and 429: the third party is up and refused this
 * request, and repeating it gets the same answer. `failed` is everything else, the third party or the way to
 * it not working: a 5xx, 408, 429, no answer in time, a dropped connection, a 1xx or 3xx nobody expects.
 */
export type CallOutcome = "ok" | "client_error" | "failed";

/** The two 4xx that mean "try again": the third party's failure, not the request's. */
const TRY_AGAIN = new Set([408, 429]);

export const classify = (status: CallStatus): CallOutcome =>
  typeof status === "string"
    ? "failed"
    : status >= 200 && status < 300
      ? "ok"
      : status >= 400 && status < 500 && !TRY_AGAIN.has(status)
        ? "client_error"
        : "failed";

export type BreakerConfig = {
  /** Consecutive failures before the breaker opens. */
  readonly consecutiveFailures: number;
  /** First half-open probe, in milliseconds. */
  readonly initialDelayMs: number;
  /** Ceiling the backoff grows to. */
  readonly maxDelayMs: number;
};

const failedCall = (result: unknown): boolean => isCallStatus(result) && classify(result) === "failed";

export const make = (cfg: BreakerConfig): CircuitBreakerPolicy =>
  circuitBreaker(handleAll.orWhenResult(failedCall), {
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
