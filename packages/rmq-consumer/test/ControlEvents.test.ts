import { test } from "node:test";
import assert from "node:assert/strict";
import { Deferred, Duration, Effect, Fiber, Option as O, Ref } from "effect";
import { TestClock } from "effect/testing";
import { SEQUENCED_EVENT, State } from "@egress/domain/Model.ts";
import { APPLY_BUDGET, APPLY_RETRY, makeApplier, persist } from "../src/ControlEvents.ts";
import * as DaemonState from "../src/DaemonState.ts";

/** The control-event path with the real reducer and no broker: `settle` and `publish` stand in for reconcile and the triggers. */

const T0 = 1_700_000_000_000;

type Event = { readonly sequence: number; readonly state: State };

/** `dispatch` and `isCurrent` as the daemon builds them, on one shared `Ref`. */
const makeDaemon = Effect.gen(function* () {
  const state = yield* Ref.make(DaemonState.initialState(T0));
  const dispatch = (event: Event) =>
    Ref.modify(state, (prior) => {
      const transition = DaemonState.reduce(
        prior,
        {
          _tag: "CircuitChanged",
          type: SEQUENCED_EVENT,
          lease: O.none(),
          state: event.state,
          sequence: event.sequence,
          at: T0,
        },
        true,
      );
      return [transition, transition.next] as const;
    });
  const isCurrent = (next: DaemonState.DaemonState) =>
    Effect.map(Ref.get(state), (now) => DaemonState.isCurrent(now, next.applied));
  return { state, dispatch, isCurrent };
});

/** Runs on a TestClock, advanced well past every retry and budget while `body` is forked. */
const withTestClock = <A, E>(body: Effect.Effect<A, E>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(body);
      yield* TestClock.adjust(Duration.sum(APPLY_BUDGET, APPLY_BUDGET));
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );

test("a newer event waits until the older one has reconciled", async () => {
  const log = await Effect.runPromise(
    Effect.gen(function* () {
      const log: Array<string> = [];
      const { dispatch, isCurrent } = yield* makeDaemon;
      const releaseFirst = yield* Deferred.make<void>();
      const firstSettling = yield* Deferred.make<void>();

      const apply = yield* makeApplier({
        dispatch: (event: Event) =>
          Effect.tap(dispatch(event), () => Effect.sync(() => log.push(`dispatch ${event.sequence}`))),
        settle: (event) =>
          Effect.gen(function* () {
            if (event.sequence === 1) {
              yield* Deferred.succeed(firstSettling, undefined);
              yield* Deferred.await(releaseFirst);
            }
            log.push(`settle ${event.sequence}`);
          }),
        isCurrent,
        publish: (action) => Effect.sync(() => void log.push(`publish ${action._tag}`)),
      });

      const first = yield* Effect.forkChild(apply({ sequence: 1, state: State.HALF_OPEN }));
      yield* Deferred.await(firstSettling);
      const second = yield* Effect.forkChild(apply({ sequence: 2, state: State.OPEN }));
      // Give the second every chance to overtake while the first is reconciling.
      yield* Effect.yieldNow;
      yield* Effect.sleep("20 millis");
      assert.deepEqual(log, ["dispatch 1"], "the second event must not dispatch while the first reconciles");

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      return log;
    }),
  );
  assert.deepEqual(log.slice(0, 2), ["dispatch 1", "settle 1"]);
  assert.ok(log.indexOf("dispatch 2") > log.indexOf("settle 1"));
});

test("an OPEN does not wait on the publish an earlier event owes", async () => {
  const { opened, firstStillPublishing } = await Effect.runPromise(
    Effect.gen(function* () {
      const { dispatch, isCurrent } = yield* makeDaemon;
      const publishing = yield* Deferred.make<void>();
      const apply = yield* makeApplier({
        dispatch,
        settle: () => Effect.void,
        isCurrent,
        // The probe trigger's confirm never comes: a broker alarm.
        publish: () => Deferred.succeed(publishing, undefined).pipe(Effect.andThen(Effect.never)),
      });
      const first = yield* Effect.forkChild(apply({ sequence: 1, state: State.HALF_OPEN }));
      yield* Deferred.await(publishing);
      const opened = yield* apply({ sequence: 2, state: State.OPEN });
      const firstStillPublishing = first.pollUnsafe() === undefined;
      yield* Fiber.interrupt(first);
      return { opened, firstStillPublishing };
    }),
  );
  assert.equal(opened, true);
  assert.equal(firstStillPublishing, true);
});

test("a trigger is not published once a newer event has replaced the one that owed it", async () => {
  const { published, superseded } = await Effect.runPromise(
    Effect.gen(function* () {
      const { dispatch, isCurrent } = yield* makeDaemon;
      const published: Array<string> = [];
      const superseded: Array<string> = [];
      const checking = yield* Deferred.make<void>();
      const releaseCheck = yield* Deferred.make<void>();
      const apply = yield* makeApplier({
        dispatch,
        settle: () => Effect.void,
        // The first event's check is held until the newer event has landed.
        isCurrent: (next) =>
          O.getOrThrow(next.applied).sequence === 1
            ? Deferred.succeed(checking, undefined).pipe(
                Effect.andThen(Deferred.await(releaseCheck)),
                Effect.andThen(isCurrent(next)),
              )
            : isCurrent(next),
        publish: (action) => Effect.sync(() => void published.push(action._tag)),
        superseded: (action) => Effect.sync(() => void superseded.push(action._tag)),
      });
      const first = yield* Effect.forkChild(apply({ sequence: 1, state: State.HALF_OPEN }));
      yield* Deferred.await(checking);
      yield* apply({ sequence: 2, state: State.OPEN });
      yield* Deferred.succeed(releaseCheck, undefined);
      yield* Fiber.join(first);
      return { published, superseded };
    }),
  );
  assert.deepEqual(published, []);
  assert.deepEqual(superseded, ["PublishProbeTrigger"]);
});

test("a transient publish failure is retried in place", async () => {
  const { applied, attempts } = await withTestClock(
    Effect.gen(function* () {
      const { dispatch, isCurrent } = yield* makeDaemon;
      let attempts = 0;
      const apply = yield* makeApplier({
        dispatch,
        settle: () => Effect.void,
        isCurrent,
        publish: () => persist(Effect.suspend(() => (++attempts === 1 ? Effect.fail("broker blip") : Effect.void))),
      });
      const applied = yield* apply({ sequence: 1, state: State.HALF_OPEN });
      return { applied, attempts };
    }),
  );
  assert.equal(applied, true);
  assert.equal(attempts, 2);
});

test("a step that keeps failing gives up after its retries, leaving the transition applied", async () => {
  const { exit, attempts, circuit } = await withTestClock(
    Effect.gen(function* () {
      const { state, dispatch, isCurrent } = yield* makeDaemon;
      let attempts = 0;
      const apply = yield* makeApplier({
        dispatch,
        settle: () => Effect.void,
        isCurrent,
        publish: () =>
          persist(
            Effect.suspend(() => {
              attempts++;
              return Effect.fail("broker down");
            }),
          ),
      });
      const exit = yield* Effect.exit(apply({ sequence: 1, state: State.HALF_OPEN }));
      return { exit, attempts, circuit: (yield* Ref.get(state)).circuit };
    }),
  );
  assert.equal(exit._tag, "Failure");
  assert.equal(attempts, APPLY_RETRY.times + 1);
  assert.equal(circuit, State.HALF_OPEN, "nothing undone: the snapshot and the fleet cover the trigger");
});

test("a publish that never settles gives up within the budget", async () => {
  const exit = await withTestClock(
    Effect.gen(function* () {
      const { dispatch, isCurrent } = yield* makeDaemon;
      const apply = yield* makeApplier({
        dispatch,
        settle: () => Effect.void,
        isCurrent,
        publish: () => persist(Effect.never),
      });
      return yield* Effect.exit(apply({ sequence: 1, state: State.HALF_OPEN }));
    }),
  );
  assert.equal(exit._tag, "Failure");
});

test("a stale event neither reconciles nor publishes", async () => {
  const log = await Effect.runPromise(
    Effect.gen(function* () {
      const { dispatch, isCurrent } = yield* makeDaemon;
      const log: Array<string> = [];
      const apply = yield* makeApplier({
        dispatch,
        settle: (event: Event) => Effect.sync(() => void log.push(`settle ${event.sequence}`)),
        isCurrent,
        publish: (action) => Effect.sync(() => void log.push(action._tag)),
      });
      yield* apply({ sequence: 2, state: State.OPEN });
      assert.equal(yield* apply({ sequence: 1, state: State.HALF_OPEN }), false);
      return log;
    }),
  );
  assert.deepEqual(log, ["settle 2"]);
});
