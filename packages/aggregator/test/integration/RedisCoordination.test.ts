import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { Effect, Option } from "effect";
import { Redis } from "ioredis";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import {
  CheckpointStore,
  LeaderElection,
  RedisCoordinationLayer,
} from "../../src/Coordination.ts";
import type { RedisLike } from "../../src/Coordination.ts";

/**
 * Everything in Coordination.test.ts runs against the in-memory layer and is
 * fast, deterministic, and requires nothing external — that is what
 * `pnpm test` runs by default. This file is the other half of the honesty
 * the README describes: `RedisCoordinationLayer`'s Lua scripts were
 * "reasoned from Redis's documented command semantics, not run against a
 * live Redis." This runs them against a real Redis, in a throwaway
 * Testcontainers-managed container, proving the exact same fencing property
 * directly rather than trusting the reasoning.
 *
 * Opt-in (`pnpm run test:redis`), not part of `pnpm test`/`pnpm run check`:
 * it needs Docker, uses real wall-clock sleeps instead of TestClock (Redis's
 * own PX expiry runs on real time, so simulated time cannot drive it), and
 * is correspondingly slower.
 *
 * Import `{ Redis }` by name, not the default export: ioredis's default
 * export does not resolve to a constructable type under this repo's
 * `nodenext` + `verbatimModuleSyntax` combination (no `exports` map on a
 * CJS package with ESM-shaped types), but the named export does.
 */

let container: StartedTestContainer | null = null;
let client: Redis | null = null;
let dockerAvailable = true;

before(async () => {
  try {
    container = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
  } catch {
    // No Docker daemon reachable in this environment — skip, don't fail.
    dockerAvailable = false;
    return;
  }
  client = new Redis({
    host: container.getHost(),
    port: container.getMappedPort(6379),
    maxRetriesPerRequest: 1,
  });
  await client.ping();
});

after(async () => {
  await client?.quit().catch(() => { });
  await container?.stop().catch(() => { });
});

/**
 * Docker availability is only known once `before()` has run, which happens
 * before any test body executes but *after* `test(...)` registration — a
 * static `{ skip }` option evaluated at registration time would always see
 * the initial `true` and never actually skip. Checking inside the test body
 * (which node:test guarantees runs after `before()`) is what makes this
 * correct; confirmed by pointing Docker at a nonexistent socket and watching
 * this skip cleanly instead of failing with a null-client error.
 */
const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
  if (dockerAvailable) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

/** ioredis -> RedisLike, exactly the one-line adapter the README promises. */
const asRedisLike = (redis: Redis): RedisLike => ({
  eval: (script, { keys, args }) =>
    redis.eval(script, keys.length, ...keys, ...args) as Promise<string | number | null>,
});

// A fresh key prefix per test so tests do not interfere with each other on
// the one shared container.
let prefixCounter = 0;
const freshLayer = () => {
  const prefix = `test:${Date.now()}:${prefixCounter++}`;
  return RedisCoordinationLayer(asRedisLike(client!), prefix);
};

test("acquire, renew keeps the token, real TTL expiry allows a strictly higher one", async (t) => {
  if (skipIfNoDocker(t)) return;
  await Effect.runPromise(Effect.provide(
    Effect.gen(function* () {
      const leader = yield* LeaderElection;
      const first = yield* leader.tryAcquireOrRenew("A", 200);
      assert.ok(Option.isSome(first));

      const renewed = yield* leader.tryAcquireOrRenew("A", 200);
      assert.deepEqual(renewed, first, "renewal by the same holder keeps the token");

      yield* Effect.promise(() => sleep(400)); // real time — outlive the 200ms TTL

      const handoff = yield* leader.tryAcquireOrRenew("B", 200);
      assert.ok(Option.isSome(handoff));
      assert.ok(
        (handoff as Option.Some<number>).value > (first as Option.Some<number>).value,
        "a real handoff must produce a strictly higher token",
      );
    }),
    freshLayer(),
  ));
});

test("a live holder blocks a competing acquire", async (t) => {
  if (skipIfNoDocker(t)) return;
  await Effect.runPromise(Effect.provide(
    Effect.gen(function* () {
      const leader = yield* LeaderElection;
      yield* leader.tryAcquireOrRenew("A", 10_000);
      const blocked = yield* leader.tryAcquireOrRenew("B", 10_000);
      assert.ok(Option.isNone(blocked), "B must not acquire while A's lease is live");
    }),
    freshLayer(),
  ));
});

test("a stale token is rejected even for an API no one has checkpointed yet", async (t) => {
  if (skipIfNoDocker(t)) return;
  await Effect.runPromise(Effect.provide(
    Effect.gen(function* () {
      const leader = yield* LeaderElection;
      const checkpoints = yield* CheckpointStore;

      const aToken = yield* leader.tryAcquireOrRenew("A", 200);
      yield* Effect.promise(() => sleep(400));
      const bToken = yield* leader.tryAcquireOrRenew("B", 200);
      assert.ok(Option.isSome(aToken) && Option.isSome(bToken));

      // Same property Coordination.test.ts proves in-memory: an untouched
      // key must not let a stale token win just because nothing higher has
      // written there yet. Here it is Redis's own Lua execution deciding,
      // not our in-memory Ref.
      const stale = yield* checkpoints
        .save("brand-new-api", (aToken as Option.Some<number>).value, {
          state: "OPEN",
          reason: "ALL_ENDPOINTS_EJECTED",
          sequence: 1,
          changedAt: 0,
          openBackoffMs: 4000,
        })
        .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
      assert.equal(stale, "fenced", "A's stale token must be rejected by real Redis, not just reasoned about");

      const fresh = yield* checkpoints
        .save("brand-new-api", (bToken as Option.Some<number>).value, {
          state: "OPEN",
          reason: "ALL_ENDPOINTS_EJECTED",
          sequence: 1,
          changedAt: 0,
          openBackoffMs: 4000,
        })
        .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
      assert.equal(fresh, "ok", "B's current token must be accepted");

      const loaded = yield* checkpoints.load("brand-new-api");
      assert.ok(Option.isSome(loaded));
      assert.equal(loaded.value.sequence, 1);
    }),
    freshLayer(),
  ));
});

test("release only removes the lease if the caller still holds it", async (t) => {
  if (skipIfNoDocker(t)) return;
  await Effect.runPromise(Effect.provide(
    Effect.gen(function* () {
      const leader = yield* LeaderElection;
      yield* leader.tryAcquireOrRenew("A", 200);
      // A stale release from a holder that no longer holds the lease must
      // not evict whoever holds it now.
      yield* Effect.promise(() => sleep(400));
      yield* leader.tryAcquireOrRenew("B", 10_000);
      yield* leader.release("A");
      const stillB = yield* leader.tryAcquireOrRenew("C", 200);
      assert.ok(Option.isNone(stillB), "A's stale release must not evict B's live lease");
    }),
    freshLayer(),
  ));
});
