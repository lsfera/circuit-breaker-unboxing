import { GenericContainer, Wait } from "testcontainers";
import { Redis } from "ioredis";
import type { StartedTestContainer } from "testcontainers";
import type { RedisLike } from "../../src/Coordination.ts";

/**
 * One Redis per test file, declared once.
 *
 * Both suites here need the same container, the same client options and the
 * same `RedisLike` adapter, and had their own copy of each — so the image tag
 * lived in two places and could be bumped in one.
 *
 * Not named `*.test.ts`, so the runner's glob does not pick it up.
 */
let container: StartedTestContainer | null = null;
let client: Redis | null = null;
let available = false;

export const startRedis = async (): Promise<void> => {
  try {
    container = await new GenericContainer("redis:8-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
  } catch {
    // No Docker daemon reachable in this environment — skip, don't fail.
    available = false;
    return;
  }
  client = new Redis({
    host: container.getHost(),
    port: container.getMappedPort(6379),
    maxRetriesPerRequest: 1,
  });
  await client.ping();
  available = true;
};

export const stopRedis = async (): Promise<void> => {
  await client?.quit().catch(() => {});
  await container?.stop().catch(() => {});
  client = null;
  container = null;
};

/**
 * Docker availability is only known once `before()` has run, which happens
 * before any test body executes but *after* `test(...)` registration — a
 * static `{ skip }` option evaluated at registration time would always see the
 * initial value and never actually skip. Checking inside the test body (which
 * node:test guarantees runs after `before()`) is what makes this correct;
 * confirmed by pointing Docker at a nonexistent socket and watching this skip
 * cleanly instead of failing with a null-client error.
 */
export const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
  if (available) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

/** ioredis -> RedisLike, exactly the one-line adapter the README promises. */
export const asRedisLike = (redis: Redis): RedisLike => ({
  eval: (script, { keys, args }) =>
    redis.eval(script, keys.length, ...keys, ...args) as Promise<string | number | null>,
});

/** The live client, once `startRedis` has run. */
export const redis = (): Redis => client!;

/** A fresh key prefix per layer, so tests do not collide on the one container. */
let prefixCounter = 0;
export const freshPrefix = (): string => `test:${Date.now()}:${prefixCounter++}`;
