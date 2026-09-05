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
  tokenCounter,
} from "../../src/Coordination.ts";
import type { RedisLike } from "../../src/Coordination.ts";

/**
 * Everything in Coordination.test.ts runs against the in-memory layer and is
 * fast, deterministic, and requires nothing external — that is what
 * `pnpm test` runs by default. This file is the other half:
 * `RedisCoordinationLayer`'s Lua scripts started out reasoned from Redis's
 * documented command semantics rather than run, and this executes them
 * against a real Redis in a throwaway Testcontainers-managed container,
 * proving the exact same fencing property by Redis's own Lua execution
 * instead of trusting the reasoning.
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
        tokenCounter((handoff as Option.Some<string>).value) >
          tokenCounter((first as Option.Some<string>).value),
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
        .save("brand-new-api", (aToken as Option.Some<string>).value, {
          state: "OPEN",
          reason: "ALL_ENDPOINTS_EJECTED",
          sequence: 1,
          changedAt: 0,
          openBackoffMs: 4000,
        })
        .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
      assert.equal(stale, "fenced", "A's stale token must be rejected by real Redis, not just reasoned about");

      const fresh = yield* checkpoints
        .save("brand-new-api", (bToken as Option.Some<string>).value, {
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

/**
 * A checkpoint is the one piece of this system's state that outlives the
 * process, which makes it the one piece that arrives as untrusted input.
 * Casting `JSON.parse` to `Checkpoint` would seed the breaker with
 * `undefined` fields and publish `NaN` sequences to every subscriber; reading
 * a value that does not decode as "no checkpoint" is a cold start, which the
 * aggregator already handles correctly.
 */
test("a corrupt checkpoint reads as absent, not as garbage state", async (t) => {
  if (skipIfNoDocker(t)) return;

  const prefix = `test:${Date.now()}:corrupt`;
  const layer = RedisCoordinationLayer(asRedisLike(client!), prefix);

  const { valid, truncated, notJson, wrongShape } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const store = yield* CheckpointStore;
        const leader = yield* LeaderElection;
        const token = yield* leader.tryAcquireOrRenew("writer", 10_000);
        assert.ok(Option.isSome(token));

        yield* store.save("good", token.value, {
          state: "OPEN",
          reason: "ALL_ENDPOINTS_EJECTED",
          sequence: 7,
          changedAt: 1_000,
          openBackoffMs: 4000,
        });

        // Written straight past the store, the way a truncated write or an
        // older build's format would actually arrive.
        const put = (api: string, raw: string) =>
          Effect.promise(() => client!.set(`${prefix}:checkpoint:${api}`, raw));
        yield* put("truncated", '{"state":"OPEN","sequence":');
        yield* put("garbage", "not json at all");
        yield* put("shape", '{"state":"NOPE","sequence":"seven"}');

        return {
          valid: yield* store.load("good"),
          truncated: yield* store.load("truncated"),
          notJson: yield* store.load("garbage"),
          wrongShape: yield* store.load("shape"),
        };
      }),
      layer,
    ),
  );

  assert.ok(Option.isSome(valid), "a well-formed checkpoint still loads");
  assert.equal((valid as Option.Some<{ sequence: number }>).value.sequence, 7);
  assert.ok(Option.isNone(truncated), "a truncated write must read as absent");
  assert.ok(Option.isNone(notJson), "a non-JSON value must read as absent");
  assert.ok(Option.isNone(wrongShape), "valid JSON of the wrong shape must read as absent");
});

/**
 * Fencing has to survive the coordinator losing its own state, and this is
 * the case where it did not.
 *
 * The token comes from `INCR`, so a Redis that restarts without persistence —
 * or fails over to an empty replica — starts issuing from 1 again. A leader
 * that was paused when that happened still holds, say, token 5, and the
 * checkpoint script's `attempted < current` test then reads `5 < 1`, which is
 * false. The stale leader is waved through and overwrites the new leader's
 * checkpoints: precisely the split brain fencing tokens exist to prevent,
 * reached by making the counter go backwards rather than by any race.
 *
 * The fix is that a token is not just a counter, it is an epoch and a
 * counter. A coordinator that has lost its state issues a new epoch, and a
 * token from the old one is not stale — it is unrecognisable, which is
 * stronger.
 */
test("a coordinator that loses its state does not let a stale leader win", async (t) => {
  if (skipIfNoDocker(t)) return;

  const prefix = `test:${Date.now()}:wipe`;
  const layer = RedisCoordinationLayer(asRedisLike(client!), prefix);

  const { staleWrite, freshWrite } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const leader = yield* LeaderElection;
        const store = yield* CheckpointStore;

        const first = yield* leader.tryAcquireOrRenew("A", 10_000);
        assert.ok(Option.isSome(first));
        const cp = {
          state: "OPEN",
          reason: "ALL_ENDPOINTS_EJECTED",
          sequence: 7,
          changedAt: 1_000,
          openBackoffMs: 4000,
        } as const;
        yield* store.save("payments", first.value, cp);

        // The coordinator loses everything: a restart with no persistence, or
        // a failover to an empty replica. Nothing about this is exotic — it
        // is the default configuration of the Redis in this repo's compose.
        yield* Effect.promise(() =>
          client!.del(
            `${prefix}:leader:holder`,
            `${prefix}:leader:token`,
            `${prefix}:leader:epoch`,
          ),
        );

        // B comes along and takes the lease from a blank coordinator.
        const second = yield* leader.tryAcquireOrRenew("B", 10_000);
        assert.ok(Option.isSome(second));

        // A was paused through all of this and still believes it leads.
        const staleWrite = yield* store
          .save("payments", first.value, { ...cp, sequence: 99 })
          .pipe(
            Effect.as("accepted" as const),
            Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)),
          );

        const freshWrite = yield* store
          .save("payments", second.value, { ...cp, sequence: 8 })
          .pipe(
            Effect.as("accepted" as const),
            Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)),
          );

        return { staleWrite, freshWrite };
      }),
      layer,
    ),
  );

  assert.equal(
    staleWrite,
    "fenced",
    "a leader holding a token from before the coordinator lost its state must not be able to write",
  );
  assert.equal(freshWrite, "accepted", "and the instance that actually holds the lease must be");
});
