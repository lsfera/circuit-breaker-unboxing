import { test } from "node:test";
import assert from "node:assert/strict";
import { Cause, Effect, Exit, Option as O } from "effect";
import type { ProbeVerdict } from "../src/Breaker.ts";
import * as Dependency from "../src/Dependency.ts";
import type { Caller, Registration, Verdict } from "../src/Dependency.ts";

/**
 * The wrapper against a fake breaker and caller: what it reports, what it tells the concurrency limit, and whether
 * it halts the action. No broker, no real dependency.
 */

const byNumber = (exit: Exit.Exit<number, string>): Verdict =>
  Exit.match(exit, {
    onSuccess: (n) => ({ outcome: n === 200 ? "ok" : n === 429 ? "throttled" : n === 422 ? "client_error" : "failed", reason: String(n) }),
    onFailure: () => ({ outcome: "failed", reason: "error" }),
  });

const Thing = Dependency.make("thing", { classify: byNumber, timeout: "20 millis" });
const Dupe = Dependency.make("dupe", {
  classify: (exit: Exit.Exit<number, string>): Verdict =>
    Exit.isFailure(exit) ? { outcome: "ok", reason: "duplicate" } : { outcome: "ok", reason: "ok" },
});

type Breaker = { readonly phase: "open" } | { readonly phase: "closed"; readonly streak: number } | { readonly phase: "half-open" };

const world = (breaker: Breaker, options: { permit?: boolean; throttling?: boolean } = {}) => {
  const reports: Array<boolean> = [];
  const verdicts: Array<ProbeVerdict> = [];
  const observed: Array<string> = [];
  const permits = { taken: 0, returned: 0 };
  const registration: O.Option<Registration> =
    breaker.phase === "open"
      ? O.none()
      : breaker.phase === "closed"
        ? O.some({ phase: "closed", report: (ok) => Effect.sync(() => (reports.push(ok), ok ? 0 : breaker.streak)) })
        : O.some({ phase: "half-open", verdict: (v) => Effect.sync(() => void verdicts.push(v)) });
  const guard: Dependency.Guard = {
    registration: Effect.succeed(registration),
    takePermit: Effect.sync(() =>
      options.permit === false ? O.none() : (permits.taken++, O.some(Effect.sync(() => void permits.returned++))),
    ),
  };
  const caller: Caller = {
    consumer: "test",
    throttling: options.throttling ?? true,
    epoch: () => 0,
    observe: (v) => Effect.sync(() => void observed.push(v.outcome)),
  };
  const run = <A, E>(effect: Effect.Effect<A, E, Dependency.Gated<"thing"> | Dependency.Gated<"dupe">>) =>
    Effect.runPromise(
      Effect.exit(effect).pipe(
        Effect.provideService(Thing.guard, guard),
        Effect.provideService(Dupe.guard, guard),
        Effect.provideService(Dependency.CurrentCaller, caller),
      ),
    );
  return { reports, verdicts, observed, permits, run };
};

const halted = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit)
    ? O.match(Cause.findErrorOption(exit.cause), {
        onNone: () => assert.fail(`expected a halt, got ${String(exit)}`),
        onSome: (error) => error as Dependency.Halted,
      })
    : assert.fail(`expected a halt, got ${String(exit)}`);

test("an ok answer returns the value, reports a success, and tells the concurrency limit", async () => {
  const w = world({ phase: "closed", streak: 1 });
  assert.deepEqual(await w.run(Thing(Effect.succeed(200))), Exit.succeed(200));
  assert.deepEqual(w.reports, [true]);
  assert.deepEqual(w.observed, ["ok"]);
});

test("any other answer halts the action with the dependency, its reason and the streak it reported", async () => {
  const failing = world({ phase: "closed", streak: 3 });
  const h = halted(await failing.run(Thing(Effect.succeed(503))));
  assert.deepEqual(
    { dependency: h.dependency, stop: h.stop, reason: h.reason, role: h.role, streak: h.streak },
    { dependency: "thing", stop: "failed", reason: "503", role: "work", streak: 3 },
  );
  assert.deepEqual(failing.reports, [false]);
  const refused = world({ phase: "closed", streak: 1 });
  assert.equal(halted(await refused.run(Thing(Effect.succeed(422)))).stop, "client_error");
  assert.deepEqual(refused.reports, [true], "the dependency answered: a success for the breaker");
});

test("no answer in time is `failed`/`timeout`, decided before the application's classifier", async () => {
  const slow = world({ phase: "closed", streak: 1 });
  const h = halted(await slow.run(Thing(Effect.never)));
  assert.deepEqual([h.stop, h.reason], ["failed", "timeout"]);
});

test("a defect in the wrapped effect is `failed`/`defect`", async () => {
  const dying = world({ phase: "closed", streak: 1 });
  const h = halted(await dying.run(Thing(Effect.die("boom"))));
  assert.deepEqual([h.stop, h.reason], ["failed", "defect"]);
});

test("throttled is a failure when the concurrency limit is not adapting", async () => {
  const adapting = world({ phase: "closed", streak: 1 });
  assert.equal(halted(await adapting.run(Thing(Effect.succeed(429)))).stop, "throttled");
  assert.deepEqual(adapting.reports, [true], "full is not broken");
  const fixed = world({ phase: "closed", streak: 1 }, { throttling: false });
  assert.equal(halted(await fixed.run(Thing(Effect.succeed(429)))).stop, "failed");
  assert.deepEqual(fixed.reports, [false]);
});

test("a failure the application calls ok halts with `ok`: there is no value to carry on with", async () => {
  const dupe = world({ phase: "closed", streak: 1 });
  assert.equal(halted(await dupe.run(Dupe(Effect.fail("duplicate key")))).stop, "ok");
});

test("while the breaker is open the effect is not run at all", async () => {
  const w = world({ phase: "open" });
  let ran = false;
  const h = halted(await w.run(Thing(Effect.sync(() => ((ran = true), 200)))));
  assert.equal(h.stop, "open");
  assert.equal(ran, false);
});

test("half-open: the probe takes the fleet's permit, gives the breaker its verdict, and hands the permit back", async () => {
  const w = world({ phase: "half-open" });
  assert.deepEqual(await w.run(Thing(Effect.succeed(200))), Exit.succeed(200));
  assert.deepEqual(w.verdicts, ["ok"]);
  assert.deepEqual(w.permits, { taken: 1, returned: 1 });
});

test("half-open without the permit: no call, verdict `no-permit`, and the message is released", async () => {
  let ran = false;
  const w = world({ phase: "half-open" }, { permit: false });
  const h = halted(await w.run(Thing(Effect.sync(() => ((ran = true), 200)))));
  assert.deepEqual([h.stop, h.role], ["no-permit", "probe"]);
  assert.deepEqual(w.verdicts, ["no-permit"]);
  assert.equal(ran, false);
});
