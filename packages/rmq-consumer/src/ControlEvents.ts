import { Duration, Effect, Option as O, Schedule } from "effect";
import type { Semaphore } from "effect/Semaphore";
import type { Action } from "./DaemonState.ts";

/** Between attempts at one step of a control event. */
export const APPLY_RETRY = {
  schedule: Schedule.exponential(Duration.millis(100)),
  times: 4,
} as const;

/** All attempts at one step, backoff included: a publish waits on a confirm a broker alarm can withhold indefinitely. */
export const APPLY_BUDGET = Duration.seconds(5);

/** One step of a control event: retried in place, and given up on within `APPLY_BUDGET`. */
export const persist = <A, E, R>(step: Effect.Effect<A, E, R>) =>
  step.pipe(Effect.retry(APPLY_RETRY), Effect.timeout(APPLY_BUDGET));

type Dispatched<S> = {
  readonly next: S;
  readonly ignored: boolean;
  readonly actions: ReadonlyArray<Action>;
};

/**
 * Control events move the state one at a time: transition, then `settle`
 * (reconcile), under the daemon's one permit (`lock`), so the channels always
 * follow the newest event.
 *
 * The triggers they owe are published after the permit, so an OPEN never waits
 * on a publish: each only if `isCurrent` still says its event is the one applied.
 * A newer event landing between that check and the send can still let one through;
 * the probe it would start is retired by the next reconcile.
 *
 * Nothing is undone on failure. Every daemon publishes the same triggers, and the
 * next snapshot re-applies the state. Succeeds with whether the event was applied.
 */
export const makeApplier = <A, S, E, R, E2, R2>(lock: Semaphore, options: {
  readonly dispatch: (event: A) => Effect.Effect<Dispatched<S>>;
  readonly settle: (event: A) => Effect.Effect<void, E, R>;
  readonly isCurrent: (next: S) => Effect.Effect<boolean>;
  readonly publish: (action: Action) => Effect.Effect<void, E2, R2>;
  readonly superseded?: (action: Action) => Effect.Effect<void>;
}) =>
  (event: A) =>
    lock.withPermit(
      Effect.flatMap(options.dispatch(event), (dispatched) =>
        dispatched.ignored
          ? Effect.succeed(O.none<Dispatched<S>>())
          : Effect.as(options.settle(event), O.some(dispatched)),
      ),
    ).pipe(
      Effect.flatMap(
        O.match({
          onNone: () => Effect.succeed(false),
          onSome: ({ next, actions }) =>
            Effect.forEach(
              actions,
              (action) =>
                Effect.flatMap(options.isCurrent(next), (current) =>
                  current ? options.publish(action) : (options.superseded?.(action) ?? Effect.void),
                ),
              { discard: true },
            ).pipe(Effect.as(true)),
        }),
      ),
    );
