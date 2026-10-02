import type { RmqError } from "@egress/rmq/Client.ts";
import { Array as Arr, Effect, Option as O, Ref, Semaphore } from "effect";
import type { Registration } from "./Dependency.ts";

/**
 * One consumer's RabbitMQ subscription, derived from the breakers of the dependencies it lists: any of them open
 * (unregistered) → no subscription; else any half-open → prefetch 1, so exactly one message is the probe; else full
 * prefetch. A breaker never touches a subscription itself: it registers, and every Gate that lists it reconciles.
 */

type Mode = "none" | "probe" | "full";

export const modeOf = (registrations: ReadonlyArray<O.Option<Registration>>): Mode =>
  Arr.some(registrations, O.isNone)
    ? "none"
    : Arr.some(registrations, O.exists((r) => r.phase === "half-open"))
    ? "probe"
    : "full";

type GateIo<Subscription> = {
  readonly subscribe: (mode: "probe" | "full") => Effect.Effect<Subscription, RmqError>;
  /** Stop the subscription and return once what it holds has settled. */
  readonly retire: (subscription: Subscription) => Effect.Effect<void>;
};

export type Gate = {
  /** Bring the subscription in line with the registrations as they are now. Serialised: one change at a time. */
  readonly reconcile: Effect.Effect<void, RmqError>;
  readonly mode: Effect.Effect<Mode>;
};

export const make = Effect.fnUntraced(function*<Subscription>(
  registrations: Effect.Effect<ReadonlyArray<O.Option<Registration>>>,
  io: GateIo<Subscription>
) {
  const lock = yield* Semaphore.make(1);
  const current = yield* Ref.make<{ readonly mode: Mode; readonly subscription: O.Option<Subscription>; }>({
    mode: "none",
    subscription: O.none()
  });

  const change = Effect.fnUntraced(function*(next: Mode) {
    const { subscription } = yield* Ref.get(current);
    yield* O.match(subscription, { onNone: () => Effect.void, onSome: io.retire });
    yield* Ref.set(current, { mode: "none", subscription: O.none() });
    const started = next === "none" ? O.none<Subscription>() : O.some(yield* io.subscribe(next));
    yield* Ref.set(current, { mode: next, subscription: started });
  });

  const reconcile = Semaphore.withPermit(
    lock,
    Effect.gen(function*() {
      const next = modeOf(yield* registrations);
      const { mode } = yield* Ref.get(current);
      yield* next === mode ? Effect.void : change(next);
    })
  );

  return { reconcile, mode: Effect.map(Ref.get(current), (c) => c.mode) } satisfies Gate;
});
