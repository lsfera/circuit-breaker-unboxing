import {
  circuitBreaker,
  CircuitState,
  ConsecutiveBreaker,
  ExponentialBackoff,
  handleAll,
  isBrokenCircuitError,
} from "cockatiel";
import type { CircuitBreakerPolicy } from "cockatiel";
import { Effect, Option as O } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";

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

/**
 * The wire form of a state, for the `circuit.control` event `onStateChange`
 * publishes (see `consumer.ts`) — a string rather than `STATE_CODE`'s number,
 * since this crosses a process boundary and JSON with a named state reads
 * without cross-referencing this file.
 */
export const STATE_NAME: Record<CircuitState, string> = {
  [CircuitState.Closed]: "closed",
  [CircuitState.Open]: "open",
  [CircuitState.HalfOpen]: "half_open",
  [CircuitState.Isolated]: "isolated",
};

export { isBrokenCircuitError };

/**
 * Five replicas' half-open windows aren't coordinated (see the module doc
 * above), and cockatiel's own half-open concurrency limit is per-process:
 * `maxInFlight` messages already pulled off the work queue all wait on one
 * replica's own trial and fire together the instant it succeeds. Fixing
 * that within one process still leaves up to `maxInFlight × replica count`
 * concurrent requests at a third party that's been back up for
 * milliseconds, if several replicas' backoffs land close together — which
 * they do, since all five trip from the same outage.
 *
 * This queue is the fleet-wide fix, without an aggregator: exactly one
 * token, ever (`x-max-length: 1`, `x-overflow: reject-publish` — RabbitMQ
 * keeps the first publish and rejects the rest with a nack on the
 * publisher's own confirm, which `seedPermit` below swallows deliberately;
 * still no election code of our own, just a broker guarantee this module
 * has to know how to read rather than one it can stay ignorant of). Whichever
 * replica's own backoff clock
 * elapses first and wins the token is the only one whose half-open probe
 * reaches the network; everyone else's probe attempt is a fast local miss,
 * indistinguishable from a failed probe to their own breaker, which just
 * means they try again on their own next backoff step. This does not make
 * the five breakers agree — see README.md — it only stops the burst.
 */
export const permitQueueFor = (apiId: string): string => `${apiId}.probe-permit`;

const PERMIT_QUEUE_ARGS = { "x-max-length": 1, "x-overflow": "reject-publish" } as const;

/**
 * Publish the one token every replica competes for. Called once per replica
 * at startup.
 *
 * `x-overflow: reject-publish` doesn't silently drop a losing publish the
 * way a comment here first assumed — measured against a real broker, it
 * comes back *nacked* on the publisher's confirm, which `@egress/rmq`'s
 * `send` surfaces as a fatal `RmqError` by design (a real nack usually means
 * something is wrong). Here it doesn't: four of five replicas losing this
 * race is the expected, successful outcome, so the failure is swallowed
 * rather than left to crash-loop the daemon on every restart.
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
 * Runs `attempt` only if this replica currently holds the fleet-wide probe
 * permit; otherwise fails with `NoPermit` without calling `attempt` at all.
 * The permit is always handed back (`nack`, requeuing the same token —
 * never ack-and-republish, so there's no window where the queue holds zero
 * tokens if this process dies mid-probe) regardless of whether `attempt`
 * succeeded, so the next replica whose own backoff elapses can compete for
 * it next.
 */
export const withPermit = Effect.fn(function* <A>(apiId: string, attempt: () => Promise<A>) {
  const rmq = yield* Rmq;
  const got = yield* rmq.get(permitQueueFor(apiId));
  if (O.isNone(got)) return yield* Effect.fail(new NoPermit());
  return yield* Effect.tryPromise({ try: attempt, catch: (cause) => cause }).pipe(
    Effect.ensuring(got.value.nack),
  );
});
