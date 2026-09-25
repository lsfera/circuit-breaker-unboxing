import { test } from "node:test";
import assert from "node:assert/strict";
import { CircuitState } from "cockatiel";
import { Effect, Option as O } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import { RmqError } from "@egress/rmq/Client.ts";
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

test("a probe sees HalfOpen only from inside execute: before it, the breaker still reads Open", async () => {
  // consumer.ts decides whether a call needs the probe permit from inside the function it hands to execute().
  const breaker = Breaker.make({ consecutiveFailures: 1, initialDelayMs: 15, maxDelayMs: 50 });
  await breaker.execute(() => Promise.reject(new Error("boom"))).catch(() => {});
  await new Promise((resolve) => setTimeout(resolve, 30));

  assert.equal(breaker.state, CircuitState.Open, "the backoff has elapsed, but nothing has half-opened it yet");
  let inside: CircuitState | undefined;
  await breaker.execute(() => {
    inside = breaker.state;
    return Promise.resolve("ok");
  });
  assert.equal(inside, CircuitState.HalfOpen);
});

test("STATE_CODE gives every reachable state a stable number, closed is the initial one", () => {
  assert.equal(Breaker.STATE_CODE[CircuitState.Closed], 0);
  assert.equal(Breaker.STATE_CODE[CircuitState.Open], 1);
  assert.equal(Breaker.STATE_CODE[CircuitState.HalfOpen], 2);
  assert.equal(Breaker.INITIAL_STATE_CODE, Breaker.STATE_CODE[CircuitState.Closed]);
});

test("classify: a 2xx is ok, a 4xx other than 408 and 429 the request's fault, and anything else the third party's", () => {
  for (const status of [200, 204, 299]) assert.equal(Breaker.classify(status), "ok", String(status));
  for (const status of [400, 401, 404, 409, 418, 422, 499]) assert.equal(Breaker.classify(status), "client_error", String(status));
  for (const status of [100, 199, 300, 304, 399, 408, 429, 500, 502, 503, 504, 599, "timeout", "network"] as const) {
    assert.equal(Breaker.classify(status), "failed", String(status));
  }
});

test("classify: while throttling, a 429 is throttled and every other status reads as it did", () => {
  assert.equal(Breaker.classify(429, true), "throttled");
  assert.equal(Breaker.classify(408, true), "failed");
  assert.equal(Breaker.classify(503, true), "failed");
  assert.equal(Breaker.classify(422, true), "client_error");
  assert.equal(Breaker.classify(200, true), "ok");
});

test("a throttled call is not a breaker failure, however many come back", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 2, initialDelayMs: 20, maxDelayMs: 50 }, true);
  for (let i = 0; i < 10; i++) await breaker.execute(() => Promise.resolve(429));
  assert.equal(breaker.state, CircuitState.Closed);
});

test("a returned failure counts against the threshold like a throw, and is handed back to the caller", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 2, initialDelayMs: 20, maxDelayMs: 50 });

  assert.equal(await breaker.execute(() => Promise.resolve(503)), 503);
  assert.equal(breaker.state, CircuitState.Closed);
  await breaker.execute(() => Promise.resolve(503));
  assert.equal(breaker.state, CircuitState.Open);
});

test("a client_error is a success: it never opens the breaker, however many calls answer it", async () => {
  const breaker = Breaker.make({ consecutiveFailures: 2, initialDelayMs: 20, maxDelayMs: 50 });
  for (let i = 0; i < 10; i++) await breaker.execute(() => Promise.resolve(422));
  assert.equal(breaker.state, CircuitState.Closed);
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
const fakePermitRmq = (): RmqService & { readonly ready: () => number } => {
  // Ready tokens only, as `x-max-length` counts them: a held token is not in here.
  let ready = 1;
  const unimplemented =
    (op: string) =>
    (..._args: ReadonlyArray<unknown>) =>
      Effect.die(new Error(`${op}: not used by withPermit`));
  const rmq = Rmq.of({
    declareQueue: () => Effect.void,
    declareTopicExchange: unimplemented("declareTopicExchange"),
    bind: unimplemented("bind"),
    consume: unimplemented("consume"),
    get: () =>
      Effect.sync(() => {
        if (ready === 0) return O.none();
        ready--;
        return O.some({
          body: "permit",
          properties: {},
          messageId: O.none(),
          ack: Effect.sync(() => {}),
          nack: Effect.sync(() => {
            ready++;
          }),
        });
      }),
    publisherToExchange: unimplemented("publisherToExchange"),
    publisherToQueue: (queue) =>
      Effect.succeed({
        exchange: "",
        routingKey: queue,
        contentType: O.none(),
        contentEncoding: O.none(),
        type: O.none(),
        mandatory: true,
      }),
    // reject-publish: refused while a token is ready.
    send: () =>
      ready >= 1
        ? Effect.fail(new RmqError({ operation: "send", cause: "message nacked" }))
        : Effect.sync(() => {
            ready++;
          }),
    sendBatch: unimplemented("sendBatch"),
    cancelConsumer: unimplemented("cancelConsumer"),
    closeConsumer: unimplemented("closeConsumer"),
    lost: Effect.never,
    isConnected: Effect.succeed(true),
    resetConnection: Effect.sync(() => {}),
  });
  return Object.assign(rmq, { ready: () => ready });
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

test("a token seeded while a probe holds the permit is a duplicate, and the probe's return collapses it", async () => {
  const rmq = fakePermitRmq();
  const run = <A>(effect: Effect.Effect<A, unknown, Rmq>) =>
    Effect.runPromise(Effect.provideService(effect, Rmq, rmq));

  let release = () => {};
  const probing = run(
    Breaker.withPermit("payments-provider", () => new Promise<void>((resolve) => (release = resolve))),
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(rmq.ready(), 0, "the probe holds the only token");

  // A replica starting now: the broker accepts its seed, since the held token does not count.
  await run(Breaker.seedPermit("payments-provider"));
  assert.equal(rmq.ready(), 1, "a second token exists while the first is held");

  release();
  await probing;
  assert.equal(rmq.ready(), 1, "the return is refused while the duplicate is ready: one token again, not two");
});
