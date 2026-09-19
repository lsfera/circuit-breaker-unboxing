import { test } from "node:test";
import assert from "node:assert/strict";
import { CircuitState } from "cockatiel";
import { Effect, Option } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import type { RmqService } from "@egress/rmq/Client.ts";
import * as Breaker from "../src/Breaker.ts";

/**
 * Exercises the real cockatiel library against a fake failing function — no
 * network, no broker — rather than re-testing cockatiel's own internals.
 * Real timers, short delays: cockatiel's backoff runs on wall-clock time with
 * no virtual-clock hook to drive instead.
 */

test("stays closed and rethrows the real error until the threshold is reached", async () => {
  // cockatiel's ConsecutiveBreaker trips on the Nth failure itself
  // (`++count >= threshold`), not after an (N+1)th — confirmed by reading
  // ConsecutiveBreaker.js rather than assuming "more than N" from its doc
  // comment, which describes the effect loosely.
  const breaker = Breaker.make({ consecutiveFailures: 3, initialDelayMs: 20, maxDelayMs: 50 });
  const boom = () => Promise.reject(new Error("boom"));

  for (let i = 0; i < 2; i++) {
    await assert.rejects(breaker.execute(boom), /boom/);
    assert.equal(breaker.state, CircuitState.Closed);
  }
});

test("opens after the threshold, rejecting locally without calling the function", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 3, initialDelayMs: 20, maxDelayMs: 50 });
  const boom = () => Promise.reject(new Error("boom"));

  for (let i = 0; i < 3; i++) await breaker.execute(boom).catch(() => {});
  assert.equal(breaker.state, CircuitState.Open);

  let called = false;
  const err = await breaker.execute(() => {
    called = true;
    return Promise.resolve();
  }).catch((e: unknown) => e);

  assert.equal(called, false, "the function must not run while the breaker is open");
  assert.equal(Breaker.isBrokenCircuitError(err), true);
});

test("half-opens after the backoff and closes on a successful probe", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 1, initialDelayMs: 15, maxDelayMs: 50 });
  await breaker.execute(() => Promise.reject(new Error("boom"))).catch(() => {});
  assert.equal(breaker.state, CircuitState.Open);

  await new Promise((resolve) => setTimeout(resolve, 30));

  await breaker.execute(() => Promise.resolve("ok"));
  assert.equal(breaker.state, CircuitState.Closed);
});

test("STATE_CODE gives every reachable state a stable number, closed is the initial one", () => {
  assert.equal(Breaker.STATE_CODE[CircuitState.Closed], 0);
  assert.equal(Breaker.STATE_CODE[CircuitState.Open], 1);
  assert.equal(Breaker.STATE_CODE[CircuitState.HalfOpen], 2);
  assert.equal(Breaker.INITIAL_STATE_CODE, Breaker.STATE_CODE[CircuitState.Closed]);
});

/**
 * A single in-memory token standing in for the real `<apiId>.probe-permit`
 * queue — no broker, matching this file's own "no network" style for
 * cockatiel above. `get`'s mutation of `token` has no `await` in it, so two
 * back-to-back `run(...)` calls with nothing awaited between them cannot
 * interleave: the first caller's fiber runs synchronously up to (and past)
 * that mutation before the second caller's fiber gets a turn, the same
 * single-threaded guarantee any two synchronous statements have. That's
 * what makes "exactly one of two simultaneous callers wins" testable
 * without a real race.
 */
const fakePermitRmq = (): RmqService => {
  let token: string | null = "permit";
  const unimplemented =
    (op: string) =>
    (..._args: ReadonlyArray<unknown>) =>
      Effect.die(new Error(`${op}: not used by withPermit`));
  return Rmq.of({
    declareQueue: unimplemented("declareQueue"),
    declareTopicExchange: unimplemented("declareTopicExchange"),
    bind: unimplemented("bind"),
    consume: unimplemented("consume"),
    get: () =>
      Effect.sync(() => {
        if (token === null) return Option.none();
        const held = token;
        token = null;
        return Option.some({
          body: held,
          properties: {},
          ack: Effect.sync(() => {}),
          nack: Effect.sync(() => {
            token = held;
          }),
        });
      }),
    publisherToExchange: unimplemented("publisherToExchange"),
    publisherToQueue: unimplemented("publisherToQueue"),
    send: unimplemented("send"),
    cancelConsumer: unimplemented("cancelConsumer"),
    closeConsumer: unimplemented("closeConsumer"),
    lost: Effect.never,
    isConnected: Effect.succeed(true),
    resetConnection: Effect.sync(() => {}),
  });
};

test("withPermit lets only one of two racing replicas reach the network, and hands the permit back after", async () => {
  const rmq = fakePermitRmq();
  const run = <A>(effect: Effect.Effect<A, unknown, Rmq>) =>
    Effect.runPromise(Effect.provideService(effect, Rmq, rmq));

  let calls = 0;
  const attempt = () => {
    calls++;
    return Promise.resolve("ok");
  };

  // Kicked off back-to-back, nothing awaited between them — see the fake's
  // own doc comment for why that's enough to make the winner deterministic.
  const first = run(Breaker.withPermit("payments-provider", attempt));
  const second = run(Breaker.withPermit("payments-provider", attempt));
  const [a, b] = await Promise.allSettled([first, second]);

  assert.equal(a.status, "fulfilled", "the first caller should win the permit");
  assert.equal(b.status, "rejected", "the second caller should lose it");
  assert.equal(
    b.status === "rejected" && b.reason instanceof Breaker.NoPermit,
    true,
    "the loser fails with NoPermit, not a network error — it never called attempt",
  );
  assert.equal(calls, 1, "the loser must not call attempt at all");

  // The permit is handed back (nack) regardless of outcome, so a later,
  // non-overlapping caller can still acquire it.
  calls = 0;
  await run(Breaker.withPermit("payments-provider", attempt));
  assert.equal(calls, 1, "the permit must be available again after being released");
});
