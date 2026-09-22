import {
  circuitBreaker,
  CircuitState,
  ConsecutiveBreaker,
  ExponentialBackoff,
  handleAll,
  isBrokenCircuitError,
} from "cockatiel";
import type { CircuitBreakerPolicy } from "cockatiel";
import { Effect, Match, Option as O, Predicate } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import { isCallStatus } from "./Upstream.ts";
import type { CallStatus } from "./Upstream.ts";

/**
 * One breaker per process, shared across every message — recreating it per call would never accumulate a
 * failure count. Each replica's state is entirely private; the fleet-wide verdict is a separate channel
 * (`circuit.control`, see `consumer.ts`), not this breaker's own decision.
 */

export type BreakerConfig = {
  /** Consecutive failures before the breaker opens. */
  readonly consecutiveFailures: number;
  /** First half-open probe, in milliseconds. */
  readonly initialDelayMs: number;
  /** Ceiling the backoff grows to. */
  readonly maxDelayMs: number;
};

/**
 * `ok` is a 2xx. `client_error` is a 4xx other than 408 and 429: the third party is up and refused this
 * request, and repeating it gets the same answer. `failed` is everything else, the third party or the way to
 * it not working: a 5xx, 408, 429, no answer in time, a dropped connection, a 1xx or 3xx nobody expects.
 */
export type CallOutcome = "ok" | "client_error" | "failed";

/** The two 4xx that mean "try again": the third party's failure, not the request's. */
const TRY_AGAIN = new Set([408, 429]);

export const classify = (status: CallStatus): CallOutcome =>
  Match.value(status).pipe(
    Match.when(Predicate.isString, (): CallOutcome => "failed"),
    Match.when(
      (n) => n >= 200 && n < 300,
      (): CallOutcome => "ok",
    ),
    Match.when(
      (n) => n >= 400 && n < 500 && !TRY_AGAIN.has(n),
      (): CallOutcome => "client_error",
    ),
    Match.orElse((): CallOutcome => "failed"),
  );

/** cockatiel's `resultFilter`: true means "count this returned value as a failure." Only `failed` does. */
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

/**
 * The wire form of a state for the `circuit.control` event `onStateChange` publishes — named, not
 * `STATE_CODE`'s number, since JSON with a named state reads without cross-referencing this file.
 */
export const STATE_NAME: Record<CircuitState, string> = {
  [CircuitState.Closed]: "closed",
  [CircuitState.Open]: "open",
  [CircuitState.HalfOpen]: "half_open",
  [CircuitState.Isolated]: "isolated",
};

export { isBrokenCircuitError };

/**
 * Fleet-wide cap on concurrent half-open probes: exactly one token, ever (`x-max-length: 1`,
 * `x-overflow: reject-publish`). Whichever replica's backoff clock elapses first wins it and is the only one
 * whose probe reaches the network; everyone else's `get` is an empty miss, indistinguishable from a failed
 * probe to their own breaker. Stops the recovery burst; does not make the five breakers agree — see README.md.
 */
export const permitQueueFor = (apiId: string): string => `${apiId}.probe-permit`;

const PERMIT_QUEUE_ARGS = { "x-max-length": 1, "x-overflow": "reject-publish" } as const;

/**
 * Publish the one token every replica competes for, once per replica at startup. A losing publish comes back
 * *nacked* on the confirm, which `send` surfaces as a fatal `RmqError` — swallowed here deliberately, since
 * four of five replicas losing this race is the expected outcome, not a failure to crash-loop on.
 */
export const seedPermit = Effect.fn(function* (apiId: string) {
  const rmq = yield* Rmq;
  const queue = permitQueueFor(apiId);
  yield* rmq.declareQueue(queue, { args: PERMIT_QUEUE_ARGS });
  const pub = yield* rmq.publisherToQueue(queue);
  yield* rmq.send(pub, "permit").pipe(Effect.ignore);
});

/** Thrown when this replica loses the race for the probe permit — cockatiel treats it exactly like a failed probe (see `consumer.ts`'s `call`). */
export class NoPermit extends Error {}

/**
 * Runs `attempt` only if this replica holds the fleet-wide probe permit; otherwise fails with `NoPermit`
 * without calling it. The permit is always handed back (`nack`, requeuing the token — never ack-and-republish,
 * so there's no window with zero tokens if this process dies mid-probe) regardless of outcome.
 */
export const withPermit = Effect.fn(function* <A>(apiId: string, attempt: () => Promise<A>) {
  const rmq = yield* Rmq;
  const got = yield* rmq.get(permitQueueFor(apiId));
  if (O.isNone(got)) return yield* Effect.fail(new NoPermit());
  return yield* Effect.tryPromise({ try: attempt, catch: (cause) => cause }).pipe(
    Effect.ensuring(got.value.nack),
  );
});
