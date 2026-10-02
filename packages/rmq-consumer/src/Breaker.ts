import type { Consumer, RmqError } from "@egress/rmq/Client.ts";
import { Deferred, Effect, Match, Random, Ref } from "effect";

/**
 * A circuit breaker whose open state is a message. Tripping withdraws its registration, so its consumers get no
 * deliveries and the queue holds the work; a token sent to this replica through the delay chain is the half-open
 * timer, and carries the failed-probe count that grows the hold.
 *
 *   closed     registered at full prefetch; N failures in a row -> open
 *   open       unregistered; the token waits `holdSeconds` in the chain
 *   half-open  registered at prefetch 1, so one message is the probe: ok -> closed, failed -> open with a longer
 *              hold, no fleet permit (Permit.ts) -> open with the same hold
 *
 * The world is passed in (`Io`), so the machine runs without a broker.
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
 * How a dependency answered, as the breaker reads it. `ok` and `client_error` (it answered, and refused this
 * message) are successes; only `failed` counts toward tripping. `throttled` (it is full, not broken) is neither:
 * the concurrency limit reacts to it instead (Limiter.ts).
 */
export type Outcome = "ok" | "client_error" | "throttled" | "failed";

export type Phase = "closed" | "open" | "half-open";

export const PHASE_CODE: Record<Phase, number> = { closed: 0, open: 1, "half-open": 2 };

/**
 * Doubles per failed probe up to the ceiling, then lands in its upper half: replicas that trip together must not
 * all come back together, and at least half the doubling keeps the growth honest. `draw` is uniform in [0, 1).
 */
export const holdSeconds = (cfg: BreakerConfig, attempt: number, draw: number): number => {
  const base = Math.min(cfg.maxDelaySeconds, cfg.initialDelaySeconds * 2 ** attempt);
  return Math.max(1, Math.ceil(base / 2 + (draw * base) / 2));
};

/**
 * What the machine needs from the world. `report` is told whether every call the consumer it started makes
 * counts as a breaker success (anything but `failed` — a `client_error` counts, same as `ok`), and
 * answers with how many calls in a row have now failed, that one included: the consumer settles a failure
 * differently once it is part of a run (see `decide` in Settle.ts).
 */
export type Report = (ok: boolean) => Effect.Effect<number>;

/** `no-permit`: another replica was probing, so no call was made and nothing was learned about the third party. */
export type ProbeVerdict = "ok" | "failed" | "no-permit";

export type Io<Handle = Consumer> = {
  /** Consume at full prefetch. */
  readonly subscribe: (report: Report) => Effect.Effect<Handle, RmqError>;
  /** Consume one message at a time: `verdict` is told how the probe went. */
  readonly probe: (verdict: (v: ProbeVerdict) => Effect.Effect<void>) => Effect.Effect<Handle, RmqError>;
  /** Stop consuming for this handle, and return once what it holds has settled. */
  readonly retire: (handle: Handle) => Effect.Effect<void>;
  /** Send the token and return once it comes back, with the attempt it carries. */
  readonly hold: (seconds: number, attempt: number) => Effect.Effect<number, RmqError>;
  readonly onPhase: (phase: Phase) => Effect.Effect<void>;
};

export const supervise = <Handle>(cfg: BreakerConfig, io: Io<Handle>): Effect.Effect<never, RmqError> => {
  const closed = Effect.gen(function*() {
    yield* io.onPhase("closed");
    const tripped = yield* Deferred.make<void>();
    const failures = yield* Ref.make(0);
    const consumer = yield* io.subscribe((ok) =>
      Ref.updateAndGet(failures, (n) => (ok ? 0 : n + 1)).pipe(
        Effect.tap((n) =>
          Effect.when(Deferred.succeed(tripped, undefined), Effect.succeed(n >= cfg.consecutiveFailures))
        )
      )
    );
    yield* Deferred.await(tripped);
    yield* io.retire(consumer);
  });

  const probe = Effect.gen(function*() {
    yield* io.onPhase("half-open");
    const verdict = yield* Deferred.make<ProbeVerdict>();
    const consumer = yield* io.probe((v) => Effect.asVoid(Deferred.succeed(verdict, v)));
    const v = yield* Deferred.await(verdict);
    yield* io.retire(consumer);
    return v;
  });

  const open = Effect.fnUntraced(function*(attempt: number): Effect.fn.Return<void, RmqError> {
    yield* io.onPhase("open");
    const woken = yield* io.hold(holdSeconds(cfg, attempt, yield* Random.next), attempt);
    yield* Match.value(yield* probe).pipe(
      Match.when("ok", () => Effect.void),
      Match.when("failed", () => open(woken + 1)),
      Match.when("no-permit", () => open(woken)),
      Match.exhaustive
    );
  });

  return Effect.forever(closed.pipe(Effect.andThen(open(0))));
};
