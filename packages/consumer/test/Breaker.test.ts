import { test } from "node:test";
import assert from "node:assert/strict";
import { CircuitState } from "cockatiel";
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
