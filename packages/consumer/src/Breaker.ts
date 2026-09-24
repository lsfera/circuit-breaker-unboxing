import { Deferred, Effect, Match, Option as O, Predicate } from "effect";
import type { Consumer, RmqError } from "@egress/rmq/Client.ts";
import type { CallStatus } from "./Upstream.ts";

/**
 * A circuit breaker whose open state is a message. Nothing is timed in this process: tripping cancels the work
 * consumer, so no delivery reaches this replica and the queue simply holds the work. The way back is a token sent
 * through the delay chain (`@egress/rmq/DelayedDelivery`), addressed to this replica; its arrival is the half-open
 * timer, and it carries how many probes have failed so far, which grows the hold. The only memory a replica keeps
 * is that one message in flight and the counter that decides to trip.
 *
 *   closed     work consumer at full prefetch; N calls fail in a row -> trip
 *   open       no consumer; a token is in the chain for `holdSeconds`
 *   half-open  the token is back: a consumer with prefetch 1, so exactly one message is the probe.
 *              It succeeds -> closed, fails -> open again with a longer hold. Its call needs the fleet's
 *              one probe permit (Permit.ts); without it no call is made -> open again, same hold.
 *
 * Every consumer-shaped thing is passed in, so the machine is exercised without a broker. This is also where a
 * third party's answer is judged (`classify`): a `client_error` counts as a success — the third party answered —
 * so it trips nothing and closes a probe; only `failed` counts toward tripping.
 */

export type BreakerConfig = {
  /** Consecutive failures before the breaker opens. */
  readonly consecutiveFailures: number;
  /** First hold, in seconds — the chain's resolution. */
  readonly initialDelaySeconds: number;
  /** Ceiling the hold grows to. A day is 86,400; the chain counts to 131,071. */
  readonly maxDelaySeconds: number;
};

/**
 * `ok` is a 2xx. `client_error` is a 4xx other than 408 and 429: the third party is up and refused this
 * request, and repeating it gets the same answer. `failed` is everything else, the third party or the way to
 * it not working: a 5xx, 408, 429, no answer in time, a dropped connection, a 1xx or 3xx nobody expects.
 * `throttled` is a 429 while `throttling` — this replica adapting its concurrency to them (Limiter.ts): the
 * third party is full, not broken, so it is not a breaker failure. Otherwise a 429 is `failed`, like a 408.
 */
export type CallOutcome = "ok" | "client_error" | "throttled" | "failed";

/** The two 4xx that mean "try again": the third party's failure, not the request's. */
const TRY_AGAIN = new Set([408, 429]);

export const classify = (status: CallStatus, throttling = false): CallOutcome =>
  Match.value(status).pipe(
    Match.when(Predicate.isString, (): CallOutcome => "failed"),
    Match.when(
      (n) => throttling && n === 429,
      (): CallOutcome => "throttled",
    ),
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

export type Phase = "closed" | "open" | "half-open";

export const PHASE_CODE: Record<Phase, number> = { closed: 0, open: 1, "half-open": 2 };

/**
 * Doubles per failed probe up to the ceiling, then lands in its upper half: replicas that trip together must not
 * all come back together, and at least half the doubling keeps the growth honest.
 */
export const holdSeconds = (cfg: BreakerConfig, attempt: number, random: () => number = Math.random): number => {
  const base = Math.min(cfg.maxDelaySeconds, cfg.initialDelaySeconds * 2 ** attempt);
  return Math.max(1, Math.ceil(base / 2 + (random() * base) / 2));
};

/**
 * What the machine needs from the world. `report` is told whether every call the consumer it started makes
 * counts as a breaker success (`classify(status) !== "failed"` — a `client_error` counts, same as `ok`), and
 * answers with how many calls in a row have now failed, that one included: the consumer settles a failure
 * differently once it is part of a run (see `decide` in consumer.ts).
 */
export type Report = (ok: boolean) => number;

/** `no-permit`: another replica was probing, so no call was made and nothing was learned about the third party. */
export type ProbeVerdict = "ok" | "failed" | "no-permit";

export type Io = {
  /** The work consumer, at full prefetch. */
  readonly subscribe: (report: Report) => Effect.Effect<Consumer, RmqError>;
  /** The probe consumer, prefetch 1: `verdict` is told how its one message went. */
  readonly probe: (verdict: (v: ProbeVerdict) => void) => Effect.Effect<Consumer, RmqError>;
  /** Stop a consumer, let what it holds settle, and close it. */
  readonly retire: (consumer: Consumer) => Effect.Effect<void>;
  /** Send the token and return once it comes back, with the attempt it carries. */
  readonly hold: (seconds: number, attempt: number) => Effect.Effect<number, RmqError>;
  readonly onPhase: (phase: Phase) => Effect.Effect<void>;
};

export const supervise = (cfg: BreakerConfig, io: Io): Effect.Effect<never, RmqError> => {
  const closed = Effect.gen(function* () {
    yield* io.onPhase("closed");
    const tripped = Deferred.makeUnsafe<void>();
    let failures = 0;
    const consumer = yield* io.subscribe((ok) => {
      failures = ok ? 0 : failures + 1;
      O.map(
        O.liftPredicate(failures, (n) => n >= cfg.consecutiveFailures),
        () => Deferred.doneUnsafe(tripped, Effect.void),
      );
      return failures;
    });
    yield* Deferred.await(tripped);
    yield* io.retire(consumer);
  });

  const probe = Effect.gen(function* () {
    yield* io.onPhase("half-open");
    const verdict = Deferred.makeUnsafe<ProbeVerdict>();
    const consumer = yield* io.probe((v) => Deferred.doneUnsafe(verdict, Effect.succeed(v)));
    const v = yield* Deferred.await(verdict);
    yield* io.retire(consumer);
    return v;
  });

  const open = (attempt: number): Effect.Effect<void, RmqError> =>
    Effect.gen(function* () {
      yield* io.onPhase("open");
      const woken = yield* io.hold(holdSeconds(cfg, attempt), attempt);
      yield* Match.value(yield* probe).pipe(
        Match.when("ok", () => Effect.void),
        Match.when("failed", () => open(woken + 1)),
        Match.when("no-permit", () => open(woken)),
        Match.exhaustive,
      );
    });

  return Effect.forever(closed.pipe(Effect.andThen(open(0))));
};
