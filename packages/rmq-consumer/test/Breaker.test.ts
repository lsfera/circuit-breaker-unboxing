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
  // ConsecutiveBreaker trips on the Nth failure itself (`++count >= threshold`), not on an (N+1)th.
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

test("classify: a 2xx is ok, a 4xx other than 408 and 429 the request's fault, and anything else the third party's", () => {
  for (const status of [200, 204, 299]) assert.equal(Breaker.classify(status), "ok", String(status));
  for (const status of [400, 401, 404, 409, 418, 422, 499]) assert.equal(Breaker.classify(status), "client_error", String(status));
  for (const status of [100, 199, 300, 304, 399, 408, 429, 500, 502, 503, 504, 599, "timeout", "network"] as const) {
    assert.equal(Breaker.classify(status), "failed", String(status));
  }
});

const failed = () => Promise.resolve(503);
const refused = () => Promise.resolve(422);

test("a returned failure counts against the threshold like a throw, and is handed back to the caller", async () => {
  for (const result of [503, 408, 429, "timeout", "network"] as const) {
    const breaker = Breaker.make({ consecutiveFailures: 2, initialDelayMs: 20, maxDelayMs: 50 });

    assert.equal(await breaker.execute(() => Promise.resolve(result)), result);
    assert.equal(breaker.state, CircuitState.Closed, String(result));
    await breaker.execute(() => Promise.resolve(result));
    assert.equal(breaker.state, CircuitState.Open, String(result));
  }
});

test("a client_error is a success: it resets the streak and never opens the breaker", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 3, initialDelayMs: 20, maxDelayMs: 50 });

  for (let i = 0; i < 2; i++) await breaker.execute(failed);
  await breaker.execute(refused);
  for (let i = 0; i < 2; i++) await breaker.execute(failed);
  assert.equal(breaker.state, CircuitState.Closed, "two failures, a refusal, then two more is not three in a row");

  for (let i = 0; i < 10; i++) await breaker.execute(refused);
  assert.equal(breaker.state, CircuitState.Closed);
});

test("a client_error answer to the half-open probe closes the breaker: the third party answered", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 1, initialDelayMs: 15, maxDelayMs: 50 });
  await breaker.execute(failed);
  assert.equal(breaker.state, CircuitState.Open);

  await new Promise((resolve) => setTimeout(resolve, 30));

  await breaker.execute(refused);
  assert.equal(breaker.state, CircuitState.Closed);
});

test("STATE_CODE gives every reachable state a stable number, closed is the initial one", () => {
  assert.equal(Breaker.STATE_CODE[CircuitState.Closed], 0);
  assert.equal(Breaker.STATE_CODE[CircuitState.Open], 1);
  assert.equal(Breaker.STATE_CODE[CircuitState.HalfOpen], 2);
  assert.equal(Breaker.INITIAL_STATE_CODE, Breaker.STATE_CODE[CircuitState.Closed]);
});
