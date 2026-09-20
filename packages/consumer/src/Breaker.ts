import { Deferred, Effect, Option as O } from "effect";
import type { Consumer, RmqError } from "@egress/rmq/Client.ts";

/**
 * A circuit breaker whose open state is a message.
 *
 * Nothing is timed in this process. Tripping cancels the work consumer, which
 * is what "open" means to a broker: no delivery reaches this replica, so no
 * call is made and nothing spins — the queue simply holds the work. The way
 * back is a token sent through the delay chain (`@egress/rmq/DelayedDelivery`)
 * addressed to this replica; the token's arrival is the half-open timer, and it
 * carries how many probes have failed so far, which is what grows the hold.
 * The only memory a replica keeps of an outage is that one message in flight —
 * and the counter that decides to trip, which is just the calls it is making.
 *
 *   closed     work consumer at full prefetch; N calls fail in a row -> trip
 *   open       no consumer; a token is in the chain for `holdSeconds`
 *   half-open  the token is back: a consumer with prefetch 1, so exactly one
 *              message is the probe. It succeeds -> closed, fails -> open again
 *              with a longer hold.
 *
 * Every consumer-shaped thing is passed in, so the machine is exercised without
 * a broker.
 */

export type BreakerConfig = {
  /** Consecutive failures before the breaker opens. */
  readonly consecutiveFailures: number;
  /** First hold, in seconds — the chain's resolution. */
  readonly initialDelaySeconds: number;
  /** Ceiling the hold grows to. A day is 86,400; the chain counts to 131,071. */
  readonly maxDelaySeconds: number;
};

export type Phase = "closed" | "open" | "half-open";

export const PHASE_CODE: Record<Phase, number> = { closed: 0, open: 1, "half-open": 2 };

/**
 * Doubles per failed probe up to the ceiling, then lands somewhere in its upper
 * half: replicas that trip together must not all come back together, and a
 * hold of at least half the doubling keeps the growth honest.
 */
export const holdSeconds = (cfg: BreakerConfig, attempt: number, random: () => number = Math.random): number => {
  const base = Math.min(cfg.maxDelaySeconds, cfg.initialDelaySeconds * 2 ** attempt);
  return Math.max(1, Math.ceil(base / 2 + (random() * base) / 2));
};

/**
 * What the machine needs from the world. `report` is told the outcome of every
 * call the consumer it started makes, and answers with how many calls in a row
 * have now failed, that one included: the consumer settles a failure differently
 * once it is part of a run (see `decide` in consumer.ts).
 */
export type Report = (ok: boolean) => number;
export type Io = {
  readonly subscribe: (role: "work" | "probe", report: Report) => Effect.Effect<Consumer, RmqError>;
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
    const consumer = yield* io.subscribe("work", (ok) => {
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
    const verdict = Deferred.makeUnsafe<boolean>();
    const consumer = yield* io.subscribe("probe", (ok) => {
      Deferred.doneUnsafe(verdict, Effect.succeed(ok));
      return ok ? 0 : 1;
    });
    const ok = yield* Deferred.await(verdict);
    yield* io.retire(consumer);
    return ok;
  });

  const open = (attempt: number): Effect.Effect<void, RmqError> =>
    Effect.gen(function* () {
      yield* io.onPhase("open");
      const woken = yield* io.hold(holdSeconds(cfg, attempt), attempt);
      const ok = yield* probe;
      yield* ok ? Effect.void : open(woken + 1);
    });

  return Effect.forever(closed.pipe(Effect.andThen(open(0))));
};
