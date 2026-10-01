import { Layer } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import { Redis } from "ioredis";
import type { StartedTestContainer } from "testcontainers";
import { asRedisLike } from "@egress/coordination-redis/RedisBackend.ts";
import { RedisCoordinationLayer } from "@egress/coordination-redis/RedisCoordination.ts";
import { RedisOutboxLayer } from "@egress/coordination-redis/RedisOutbox.ts";
import { conformance } from "./suite.ts";

/**
 * The conformance suite against a real Redis.
 *
 * Import `{ Redis }` by name, not the default export: ioredis's default export
 * does not resolve to a constructable type under this repo's `nodenext` +
 * `verbatimModuleSyntax` combination, but the named export does.
 */
let container: StartedTestContainer | null = null;
let client: Redis | null = null;
let prefixCounter = 0;

conformance({
  name: "redis",
  start: async () => {
    try {
      container = await new GenericContainer("redis:8-alpine")
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
        .start();
    } catch {
      return false;
    }
    client = new Redis({
      host: container.getHost(),
      port: container.getMappedPort(6379),
      maxRetriesPerRequest: 1,
    });
    await client.ping();
    return true;
  },
  stop: async () => {
    await client?.quit().catch(() => {});
    await container?.stop().catch(() => {});
  },
  fresh: () => {
    const redis = client!;
    const prefix = `test:${Date.now()}:${prefixCounter++}`;
    const like = asRedisLike(redis);
    return {
      layer: Layer.mergeAll(RedisCoordinationLayer(like, prefix), RedisOutboxLayer(like, prefix)),
      writeRawCheckpoint: async (apiId, raw) => {
        await redis.set(`${prefix}:checkpoint:${apiId}`, raw);
      },
      wipeLease: async () => {
        await redis.del(`${prefix}:leader:holder`, `${prefix}:leader:token`, `${prefix}:leader:epoch`);
      },
      pushRawOutboxHead: async (apiId, raw) => {
        await redis.lpush(`${prefix}:outbox:${apiId}`, raw);
      },
    };
  },
});
