import { Cause, Context, Data, Duration, Effect, Exit, Metric, Option as O, Result, Schema } from "effect";
import { PositiveInt } from "@egress/config/Settings.ts";
import { MAX_DELAY_SECONDS } from "@egress/rmq/DelayedDelivery.ts";
import type { BreakerConfig, Outcome, ProbeVerdict, Report } from "./Breaker.ts";
import * as Telemetry from "./Telemetry.ts";

/**
 * A dependency an action calls — a third party, a database — with its own breaker, probe permit and timeout.
 * Wrapping an effect in it runs the effect under the timeout, judges its Exit with the application's `classify`,
 * tells this dependency's breaker, and halts the action on anything but `ok`, so later dependencies are not called.
 */

/** A dependency's answer as the breaker reads it; `reason` labels metrics and a parked message. */
export type Verdict = { readonly outcome: Outcome; readonly reason: string };

/** What the breaker has registered, read at the moment of a call. */
export type Registration =
  | { readonly phase: "closed"; readonly report: Report }
  | { readonly phase: "half-open"; readonly verdict: (v: ProbeVerdict) => Effect.Effect<void> };

/** The live side of one dependency, provided by the application runner. */
export type Guard = {
  /** `None` while the breaker is open. */
  readonly registration: Effect.Effect<O.Option<Registration>>;
  /** The fleet's probe permit for this dependency, as the effect that hands it back; `None` if another replica holds it. */
  readonly takePermit: Effect.Effect<O.Option<Effect.Effect<void>>>;
};

declare const GatedTypeId: unique symbol;
/** The service a wrapped call needs: the breaker of dependency `Name`, provided only if the consumer lists it. */
export interface Gated<Name extends string> {
  readonly [GatedTypeId]: Name;
}

const guardFor = <Name extends string>(name: Name) =>
  Context.Service<Gated<Name>, Guard>(`@egress/rmq-consumer/Dependency/${name}`);

export type Role = "work" | "probe";

/**
 * Why the action stopped at a dependency. `open` and `no-permit` made no call. `ok` is a failed effect the
 * application classified as fine (say, a duplicate insert): the message is done, and the rest of the action is not run.
 */
export type Stop = Outcome | "open" | "no-permit";

export class Halted extends Data.TaggedError("Halted")<{
  readonly dependency: string;
  readonly stop: Stop;
  readonly reason: string;
  readonly role: Role;
  /** Failures in a row at this dependency, this one included. */
  readonly streak: number;
}> {}

/** The action refused the message before touching a dependency: parked, never retried, no breaker hears of it. */
export class Rejected extends Data.TaggedError("Rejected")<{ readonly reason: string }> {}

/** The consumer a wrapped call runs for: its name, and its concurrency limit's view of the answer. */
export type Caller = {
  readonly consumer: string;
  /** Off: a `throttled` answer is a plain `failed` one. */
  readonly throttling: boolean;
  readonly epoch: () => number;
  /** Feeds the concurrency limit; on `throttled` it also holds the call's slot for the backoff. */
  readonly observe: (verdict: Verdict, startedIn: number) => Effect.Effect<void>;
};

export const CurrentCaller = Context.Reference<Caller>("@egress/rmq-consumer/Dependency/CurrentCaller", {
  defaultValue: () => ({ consumer: "none", throttling: false, epoch: () => 0, observe: () => Effect.void }),
});

const DEFAULT_TIMEOUT = Duration.seconds(2);

/** What a dependency may set of its own breaker; the rest comes from the application's `BREAKER_*` defaults. */
const BreakerOverrides = Schema.Struct({
  consecutiveFailures: Schema.optionalKey(PositiveInt),
  initialDelaySeconds: Schema.optionalKey(PositiveInt),
  maxDelaySeconds: Schema.optionalKey(PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS))),
});
export type BreakerOverrides = typeof BreakerOverrides.Type;
const decodeOverrides = Schema.decodeUnknownResult(BreakerOverrides);

/** This dependency's breaker: its own settings where it has them, the application's defaults elsewhere. */
export const breakerFor = (defaults: BreakerConfig, dependency: AnyDependency): BreakerConfig => ({
  ...defaults,
  ...dependency.breaker,
});

export interface Dependency<Name extends string, A, E> {
  <R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, Halted, R | Gated<Name>>;
  readonly dependencyName: Name;
  readonly guard: Context.Service<Gated<Name>, Guard>;
  readonly breaker: BreakerOverrides;
}

export type AnyDependency = Dependency<any, any, any>;

export const make = <const Name extends string, A, E>(
  name: Name,
  options: {
    readonly classify: (exit: Exit.Exit<A, E>) => Verdict;
    readonly timeout?: Duration.Input;
    /** This dependency's own breaker settings; anything left out follows the application's `BREAKER_*`. */
    readonly breaker?: BreakerOverrides;
  },
): Dependency<Name, A, E> => {
  const guard = guardFor(name);
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  // A declaration, not input: an impossible setting stops the application as it loads, naming the dependency.
  const breaker = Result.getOrThrowWith(
    decodeOverrides(options.breaker ?? {}),
    (error) => new Error(`dependency ${name}: invalid breaker settings: ${error.message}`),
  );

  // The SDK's own failures are judged before the application's `classify` sees anything.
  const judge = (exit: Exit.Exit<A, E | Cause.TimeoutError>): Verdict =>
    Exit.match(exit, {
      onSuccess: () => options.classify(exit as Exit.Exit<A, E>),
      onFailure: (cause) =>
        O.match(Cause.findErrorOption(cause), {
          onNone: (): Verdict => ({ outcome: "failed", reason: "defect" }),
          onSome: (error) =>
            Cause.isTimeoutError(error)
              ? { outcome: "failed", reason: "timeout" }
              : options.classify(exit as Exit.Exit<A, E>),
        }),
    });

  const halt = (stop: Stop, reason: string, role: Role, streak = 0) =>
    Effect.fail(new Halted({ dependency: name, stop, reason, role, streak }));

  const attempt = Effect.fnUntraced(function* <R>(
    effect: Effect.Effect<A, E, R>,
    report: Report,
    role: Role,
    caller: Caller,
  ) {
    const startedIn = caller.epoch();
    const exit = yield* Effect.exit(Effect.timeout(effect, timeout));
    const judged = judge(exit);
    const verdict: Verdict =
      judged.outcome === "throttled" && !caller.throttling ? { ...judged, outcome: "failed" } : judged;
    const streak = yield* report(verdict.outcome !== "failed");
    yield* Metric.update(
      Metric.withAttributes(Telemetry.calls, {
        consumer: caller.consumer,
        dependency: name,
        outcome: verdict.outcome,
        status: verdict.reason,
      }),
      1,
    );
    yield* caller.observe(verdict, startedIn);
    return yield* verdict.outcome === "ok" && Exit.isSuccess(exit)
      ? Effect.succeed(exit.value)
      : halt(verdict.outcome, verdict.reason, role, streak);
  });

  // The permit is held for the call alone, not while the probe consumer waits for a message.
  const probe = <R>(
    effect: Effect.Effect<A, E, R>,
    verdict: (v: ProbeVerdict) => Effect.Effect<void>,
    g: Guard,
    caller: Caller,
  ) =>
    Effect.flatMap(g.takePermit, (permit) =>
      O.match(permit, {
        onNone: () =>
          verdict("no-permit").pipe(
            Effect.andThen(Metric.update(Metric.withAttributes(Telemetry.permitLost, { dependency: name }), 1)),
            Effect.andThen(halt("no-permit", "no-permit", "probe")),
          ),
        onSome: (giveBack) =>
          attempt(effect, (ok) => Effect.as(verdict(ok ? "ok" : "failed"), ok ? 0 : 1), "probe", caller).pipe(
            Effect.ensuring(giveBack),
          ),
      }),
    );

  const call = Effect.fnUntraced(function* <R>(effect: Effect.Effect<A, E, R>) {
    const g = yield* guard;
    const caller = yield* CurrentCaller;
    return yield* O.match(yield* g.registration, {
      // Tripped while this message was in flight: the call would only add load to what is already failing.
      onNone: () => halt("open", "open", "work"),
      onSome: (registration) =>
        registration.phase === "closed"
          ? attempt(effect, registration.report, "work", caller)
          : probe(effect, registration.verdict, g, caller),
    });
  });

  return Object.assign(call, { dependencyName: name, guard, breaker });
};
