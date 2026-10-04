import type { Consumer } from "@egress/rmq/Client.ts";
import { Deferred, Effect, Fiber } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import * as Breaker from "../src/Breaker.ts";

/**
 * The state machine against a fake world: no broker, no dependency, no timers. What the fake records is what
 * would reach RabbitMQ (which consumers exist, which tokens are in flight), because in this breaker those are the state.
 */

const cfg: Breaker.BreakerConfig = { initialDelaySeconds: 2, maxDelaySeconds: 60 };
const testPolicyFactory: Breaker.BreakerPolicyFactory = () => {
  let failures = 0;
  return {
    success: () => {
      failures = 0;
    },
    failure: () => ++failures >= 3
  };
};

test("a hold doubles per failed probe, jittered into its upper half, and stops at the ceiling", () => {
  const at = (attempt: number, r: number) => Breaker.holdSeconds(cfg, attempt, r);
  assert.deepEqual([0, 1, 2, 3].map((n) => at(n, 1)), [2, 4, 8, 16]);
  assert.deepEqual([0, 1, 2, 3].map((n) => at(n, 0)), [1, 2, 4, 8]);
  assert.equal(at(20, 1), 60);
  assert.equal(at(20, 0), 30);
});

test("a hold is never shorter than the chain's one-second resolution", () => {
  assert.equal(Breaker.holdSeconds({ ...cfg, initialDelaySeconds: 1 }, 0, 0), 1);
});

test("every phase has its own gauge value, closed first", () => {
  assert.deepEqual(Breaker.PHASE_CODE, { closed: 0, open: 1, "half-open": 2 });
});

/** A world the test steers: `wake` releases the token in flight, `report` plays the calls a consumer makes. */
const world = () => {
  const log: string[] = [];
  const consumers: Array<{
    role: string;
    report: Breaker.Report;
    verdict: (v: Breaker.ProbeVerdict) => Effect.Effect<void>;
    live: boolean;
  }> = [];
  const holds: Array<{ seconds: number; attempt: number; wake: Deferred.Deferred<void>; }> = [];
  const add = (role: string, report: Breaker.Report, verdict: (v: Breaker.ProbeVerdict) => Effect.Effect<void>) =>
    Effect.sync(() => {
      const entry = { role, report, verdict, live: true };
      consumers.push(entry);
      log.push(`subscribe ${role}`);
      return entry as unknown as Consumer;
    });
  const io: Breaker.Io = {
    subscribe: (report) =>
      add("work", report, () => Effect.sync(() => assert.fail("a work consumer gives no verdict"))),
    probe: (verdict) => add("probe", (ok) => Effect.as(verdict(ok ? "ok" : "failed"), ok ? 0 : 1), verdict),
    retire: (c) =>
      Effect.sync(() => {
        (c as unknown as { live: boolean; }).live = false;
        log.push("retire");
      }),
    hold: (seconds, attempt) =>
      Effect.gen(function*() {
        const wake = yield* Deferred.make<void>();
        holds.push({ seconds, attempt, wake });
        log.push(`hold ${attempt}`);
        yield* Deferred.await(wake);
        return attempt;
      }),
    onPhase: (phase) => Effect.sync(() => void log.push(phase))
  };
  const until = (done: () => boolean) =>
    Effect.gen(function*() {
      for (let i = 0; i < 200 && !done(); i++) yield* Effect.sleep(1);
      assert.ok(done(), `never got there: ${log.join(" > ")}`);
    });
  return { io, log, consumers, holds, until };
};

const drive = (
  body: (w: ReturnType<typeof world>) => Effect.Effect<void>,
  policyFactory?: Breaker.BreakerPolicyFactory
) =>
  Effect.runPromise(
    Effect.gen(function*() {
      const w = world();
      const fiber = yield* Effect.forkChild(Breaker.supervise(cfg, w.io, policyFactory ?? testPolicyFactory));
      yield* body(w);
      yield* Fiber.interrupt(fiber);
    })
  );

test("a supplied policy controls trips and receives closed and half-open outcomes", () => {
  let factories = 0;
  const states: Array<[string, Breaker.BreakerPolicyState]> = [];
  return drive(
    (w) =>
      Effect.gen(function*() {
        yield* w.until(() => w.consumers.length === 1);
        const work = w.consumers[0]!;
        yield* work.report(false);
        assert.equal(w.holds.length, 0, "the custom policy has not reached its trip threshold");
        yield* work.report(false);
        yield* w.until(() => w.holds.length === 1);
        yield* Deferred.succeed(w.holds[0]!.wake, undefined);
        yield* w.until(() => w.consumers.length === 2);
        yield* w.consumers[1]!.report(false);
        yield* w.until(() => w.holds.length === 2);
        yield* Deferred.succeed(w.holds[1]!.wake, undefined);
        yield* w.until(() => w.consumers.length === 3);
        yield* w.consumers[2]!.report(true);
        yield* w.until(() => w.consumers.length === 4);
        assert.equal(factories, 1, "one policy instance is created for this supervised breaker");
        assert.deepEqual(states, [
          ["failure", "closed"],
          ["failure", "closed"],
          ["failure", "half-open"],
          ["success", "half-open"]
        ]);
      }),
    () => {
      factories += 1;
      return {
        state: undefined,
        success: (state) => states.push(["success", state]),
        failure: (state) => {
          states.push(["failure", state]);
          return state === "closed" && states.filter(([kind]) => kind === "failure").length >= 2;
        }
      };
    }
  );
});

test("it stays closed while successes interrupt the failures, and trips on the Nth in a row", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      const work = w.consumers[0]!;
      yield* Effect.forEach([false, false, true, false, false], work.report);
      yield* Effect.sleep(5);
      assert.deepEqual(w.log, ["closed", "subscribe work"], "two, a success, two: never three in a row");
      yield* work.report(false);
      yield* w.until(() => w.holds.length === 1);
      assert.deepEqual(w.log, ["closed", "subscribe work", "retire", "open", "hold 0"]);
    })
  ));

test("open means no consumer: nothing is subscribed until the token comes back", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      yield* Effect.forEach([false, false, false], w.consumers[0]!.report);
      yield* w.until(() => w.holds.length === 1);
      yield* Effect.sleep(10);
      assert.equal(w.consumers.filter((c) => c.live).length, 0);
      assert.equal(w.consumers.length, 1);
    })
  ));

test("the token's return half-opens with a single probe; a good one closes the breaker", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      yield* Effect.forEach([false, false, false], w.consumers[0]!.report);
      yield* w.until(() => w.holds.length === 1);
      yield* Deferred.succeed(w.holds[0]!.wake, undefined);
      yield* w.until(() => w.consumers.length === 2);
      assert.equal(w.consumers[1]!.role, "probe");
      yield* w.consumers[1]!.report(true);
      yield* w.until(() => w.consumers.length === 3);
      assert.equal(w.consumers[2]!.role, "work");
      assert.deepEqual(w.log.slice(-5), ["half-open", "subscribe probe", "retire", "closed", "subscribe work"]);
    })
  ));

test("a failed probe reopens with a longer hold, carried by the token's attempt", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      yield* Effect.forEach([false, false, false], w.consumers[0]!.report);
      yield* w.until(() => w.holds.length === 1);
      yield* Deferred.succeed(w.holds[0]!.wake, undefined);
      yield* w.until(() => w.consumers.length === 2);
      yield* w.consumers[1]!.report(false);
      yield* w.until(() => w.holds.length === 2);
      assert.equal(w.holds[1]!.attempt, 1);
      assert.ok(w.holds[1]!.seconds > w.holds[0]!.seconds || w.holds[1]!.seconds >= 2, "the hold grows");
      assert.equal(w.consumers.length, 2, "and nothing consumes while it does");
      assert.equal(w.consumers.filter((c) => c.live).length, 0);
    })
  ));

test("a breaker that has closed forgets: the next outage starts from the first hold again", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      yield* Effect.forEach([false, false, false], w.consumers[0]!.report);
      yield* w.until(() => w.holds.length === 1);
      yield* Deferred.succeed(w.holds[0]!.wake, undefined);
      yield* w.until(() => w.consumers.length === 2);
      yield* w.consumers[1]!.report(true);
      yield* w.until(() => w.consumers.length === 3);
      yield* Effect.forEach([false, false, false], w.consumers[2]!.report);
      yield* w.until(() => w.holds.length === 2);
      assert.equal(w.holds[1]!.attempt, 0);
    })
  ));

test("a work consumer is told how long the run of failures is, and a success ends it", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      const work = w.consumers[0]!;
      assert.deepEqual(yield* Effect.forEach([false, false, true, false], work.report), [1, 2, 0, 1]);
    })
  ));

test("a probe that loses the permit race holds again at the same attempt: nothing was learned", () =>
  drive((w) =>
    Effect.gen(function*() {
      yield* w.until(() => w.consumers.length === 1);
      yield* Effect.forEach([false, false, false], w.consumers[0]!.report);
      yield* w.until(() => w.holds.length === 1);
      yield* Deferred.succeed(w.holds[0]!.wake, undefined);
      yield* w.until(() => w.consumers.length === 2);
      yield* w.consumers[1]!.verdict("failed");
      yield* w.until(() => w.holds.length === 2);
      assert.equal(w.holds[1]!.attempt, 1);
      yield* Deferred.succeed(w.holds[1]!.wake, undefined);
      yield* w.until(() => w.consumers.length === 3);
      yield* w.consumers[2]!.verdict("no-permit");
      yield* w.until(() => w.holds.length === 3);
      assert.equal(w.holds[2]!.attempt, 1, "a lost race does not grow the hold");
      assert.equal(w.consumers.filter((c) => c.live).length, 0, "and it is open again, not consuming");
      assert.deepEqual(w.log.slice(-4), ["subscribe probe", "retire", "open", "hold 1"]);
    })
  ));
