import { PositiveInt } from "@egress/config/Settings.ts";
import { MAX_DELAY_SECONDS } from "@egress/rmq/DelayedDelivery.ts";
import {
  Cause,
  Context,
  Data,
  Deferred,
  Duration,
  Effect,
  Exit,
  Metric,
  Option as O,
  Ref,
  Result,
  Schema
} from "effect";
import type { BreakerConfig, BreakerPolicy, Outcome, ProbeVerdict, Report } from "./Breaker.ts";
import * as Telemetry from "./Telemetry.ts";

/**
 * A dependency an action calls — a third party, a database — with its own breaker, probe permit and timeout.
 * Wrapping an effect in it runs the effect under the timeout, judges what it returned with the application's `classify`,
 * tells this dependency's breaker, and halts the action on anything but `ok`, so later dependencies are not called.
 */

/** A dependency's answer as the breaker reads it; `reason` labels metrics and a parked message. */
export type Verdict = { readonly outcome: Outcome; readonly reason: string; };

/** What the breaker has registered, read at the moment of a call. */
export type Registration =
  | { readonly phase: "closed"; readonly report: Report; }
  | { readonly phase: "half-open"; readonly verdict: (v: ProbeVerdict) => Effect.Effect<void>; };

/** What asking for a dependency's probe permit found. */
export type PermitAnswer = Data.TaggedEnum<{
  Taken: { readonly giveBack: Effect.Effect<void>; };
  /** Another probe in this process holds it; `settled` waits for that probe's call, which is the breaker's verdict. */
  HeldHere: { readonly settled: Effect.Effect<void>; };
  HeldElsewhere: {};
}>;
export const PermitAnswer = Data.taggedEnum<PermitAnswer>();

/** The live side of one dependency, provided by the application runner. */
export type Guard = {
  /** `None` while the breaker is open. */
  readonly registration: Effect.Effect<O.Option<Registration>>;
  readonly takePermit: Effect.Effect<PermitAnswer>;
};

/**
 * The fleet's permit (`take`), taken by one probe of this process at a time. A sibling probe waits for the holder's
 * call rather than reporting `no-permit`, which would reach the breaker first every time and stop the hold growing.
 */
export const localPermit = Effect.fnUntraced(function*(take: Effect.Effect<O.Option<Effect.Effect<void>>>) {
  const holder = yield* Ref.make(O.none<Deferred.Deferred<void>>());
  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function*() {
      const mine = yield* Deferred.make<void>();
      const theirs = yield* Ref.modify(holder, (current) => [current, O.orElse(current, () => O.some(mine))] as const);
      const free = Ref.set(holder, O.none()).pipe(Effect.andThen(Deferred.succeed(mine, undefined)));
      return yield* O.match(theirs, {
        onSome: (held) => Effect.succeed(PermitAnswer.HeldHere({ settled: Deferred.await(held) })),
        onNone: () =>
          restore(take).pipe(
            Effect.onInterrupt(() => free),
            Effect.flatMap(
              (token): Effect.Effect<PermitAnswer> =>
                O.match(token, {
                  onNone: () => Effect.as(free, PermitAnswer.HeldElsewhere()),
                  onSome: (giveBack) =>
                    Effect.succeed(PermitAnswer.Taken({ giveBack: Effect.ensuring(giveBack, free) }))
                })
            )
          )
      });
    })
  );
});

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
export class Rejected extends Data.TaggedError("Rejected")<{ readonly reason: string; }> {}

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
  defaultValue: () => ({ consumer: "none", throttling: false, epoch: () => 0, observe: () => Effect.void })
});

const DEFAULT_TIMEOUT = Duration.seconds(2);

/** A breaker's hold ceiling: at most what the delay chain can count. */
export const MaxDelaySeconds = PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DELAY_SECONDS));

/** What a dependency may set of its own breaker; the rest comes from the application's `BREAKER_*` defaults. */
const BreakerOverrides = Schema.Struct({
  initialDelaySeconds: Schema.optionalKey(PositiveInt),
  maxDelaySeconds: Schema.optionalKey(MaxDelaySeconds)
});
export type BreakerOverrides = typeof BreakerOverrides.Type;
export type BreakerPolicyFactory = () => BreakerPolicy;
const isBreakerPolicy = (policy: unknown): policy is BreakerPolicy =>
  typeof policy === "object" &&
  policy !== null &&
  "success" in policy &&
  typeof policy.success === "function" &&
  "failure" in policy &&
  typeof policy.failure === "function";
const decodeOverrides = Schema.decodeUnknownResult(BreakerOverrides);

/** This dependency's breaker: its own settings where it has them, the application's defaults elsewhere. */
export const breakerFor = (defaults: BreakerConfig, dependency: AnyDependency): BreakerConfig => ({
  ...defaults,
  ...dependency.breaker
});

export interface Dependency<Name extends string, A, E> {
  <R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, Halted, R | Gated<Name>>;
  readonly dependencyName: Name;
  readonly guard: Context.Service<Gated<Name>, Guard>;
  readonly breaker: BreakerOverrides;
  readonly breakerPolicy: BreakerPolicyFactory;
}

export type AnyDependency = Dependency<any, any, any>;

export const make = <const Name extends string, A, E>(
  name: Name,
  options: {
    /** The effect's value or its own error. A defect or the timeout never reaches it: both are `failed`. */
    readonly classify: (result: Result.Result<A, E>) => Verdict;
    readonly timeout?: Duration.Input;
    /** This dependency's own breaker settings; anything left out follows the application's `BREAKER_*`. */
    readonly breaker?: BreakerOverrides;
    /** Creates this dependency's call-counting/trip policy. Must return a fresh instance for each replica. */
    readonly breakerPolicy: BreakerPolicyFactory;
  }
): Dependency<Name, A, E> => {
  if (typeof options.breakerPolicy !== "function") {
    throw new Error(`dependency ${name}: breakerPolicy must be a factory`);
  }
  const guard = guardFor(name);
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  // A declaration, not input: an impossible setting stops the application as it loads, naming the dependency.
  const breaker = Result.getOrThrowWith(
    decodeOverrides(options.breaker ?? {}),
    (error) => new Error(`dependency ${name}: invalid breaker settings: ${error.message}`)
  );
  const policies = new WeakSet<object>();

  const judge = (exit: Exit.Exit<A, E | Cause.TimeoutError>): Verdict =>
    Exit.match(exit, {
      onSuccess: (value) => options.classify(Result.succeed(value)),
      onFailure: (cause) =>
        O.match(Cause.findErrorOption(cause), {
          onNone: (): Verdict => ({ outcome: "failed", reason: "defect" }),
          onSome: (error) =>
            Cause.isTimeoutError(error)
              ? { outcome: "failed", reason: "timeout" }
              : options.classify(Result.fail(error as E))
        })
    });

  const halt = (stop: Stop, reason: string, role: Role, streak = 0) =>
    Effect.fail(new Halted({ dependency: name, stop, reason, role, streak }));

  const attempt = Effect.fnUntraced(function*<R>(
    effect: Effect.Effect<A, E, R>,
    report: Report,
    role: Role,
    caller: Caller
  ) {
    const startedIn = caller.epoch();
    const exit = yield* Effect.exit(Effect.timeout(effect, timeout));
    const judged = judge(exit);
    const verdict: Verdict = judged.outcome === "throttled" && !caller.throttling
      ? { ...judged, outcome: "failed" }
      : judged;
    const streak = yield* report(verdict.outcome !== "failed");
    yield* Metric.update(
      Metric.withAttributes(Telemetry.calls, {
        consumer: caller.consumer,
        dependency: name,
        outcome: verdict.outcome,
        status: verdict.reason
      }),
      1
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
    caller: Caller
  ) =>
    Effect.flatMap(g.takePermit, (permit) =>
      PermitAnswer.$match(permit, {
        HeldElsewhere: () =>
          verdict("no-permit").pipe(
            Effect.andThen(Metric.update(Metric.withAttributes(Telemetry.permitLost, { dependency: name }), 1)),
            Effect.andThen(halt("no-permit", "no-permit", "probe"))
          ),
        // No verdict: the call the permit holder is making decides. The message is held until it has.
        HeldHere: ({ settled }) => Effect.andThen(settled, halt("no-permit", "probe-in-flight", "probe")),
        Taken: ({ giveBack }) =>
          attempt(effect, (ok) => Effect.as(verdict(ok ? "ok" : "failed"), ok ? 0 : 1), "probe", caller).pipe(
            Effect.ensuring(giveBack)
          )
      }));

  const call = Effect.fnUntraced(function*<R>(effect: Effect.Effect<A, E, R>) {
    const g = yield* guard;
    const caller = yield* CurrentCaller;
    return yield* O.match(yield* g.registration, {
      // Tripped while this message was in flight: the call would only add load to what is already failing.
      onNone: () => halt("open", "open", "work"),
      onSome: (registration) =>
        registration.phase === "closed"
          ? attempt(effect, registration.report, "work", caller)
          : probe(effect, registration.verdict, g, caller)
    });
  });

  const breakerPolicy = () => {
    const policy: unknown = options.breakerPolicy();
    if (!isBreakerPolicy(policy)) {
      throw new Error(
        `dependency ${name}: breakerPolicy factory must return an object with success and failure methods`
      );
    }
    if (policies.has(policy)) {
      throw new Error(`dependency ${name}: breakerPolicy factory must return a fresh policy instance`);
    }
    policies.add(policy);
    return policy;
  };

  return Object.assign(call, { dependencyName: name, guard, breaker, breakerPolicy });
};
