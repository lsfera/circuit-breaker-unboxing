import { Effect, Layer } from "effect";
import { Redis } from "ioredis";
import type { CheckpointStore, LeaderElection } from "@egress/coordination/Coordination.ts";
import type { Outbox } from "@egress/coordination/Outbox.ts";
import { RedisCoordinationLayer } from "./RedisCoordination.ts";
import type { RedisLike } from "./RedisCoordination.ts";
import { RedisOutboxLayer } from "./RedisOutbox.ts";

/** ioredis -> RedisLike: the only thing the layers need from a client. */
export const asRedisLike = (redis: Redis): RedisLike => ({
  eval: (script, { keys, args }) =>
    redis.eval(script, keys.length, ...keys, ...args) as Promise<string | number | null>,
});

/**
 * Every coordination port over one Redis connection, opened for the layer's
 * lifetime. Unreachable is not a startup failure: each call fails, and the
 * aggregator stands down rather than leading unfenced.
 */
export const RedisBackendLayer = (
  url: string,
): Layer.Layer<LeaderElection | CheckpointStore | Outbox> =>
  Layer.unwrap(
    Effect.acquireRelease(
      Effect.sync(() =>
        new Redis(url, {
          // Fail fast: ioredis's default retries turn an outage into a hung tick.
          maxRetriesPerRequest: 1,
          enableOfflineQueue: false,
          connectTimeout: 1000,
        })
          // Otherwise ioredis prints a stack per reconnect attempt. The outage is
          // already reported, once, by the tick that stands down over it.
          .on("error", () => {}),
      ),
      (redis) => Effect.promise(() => redis.quit().then(() => {}, () => {})),
    ).pipe(
      Effect.map((redis) => {
        const like = asRedisLike(redis);
        return Layer.mergeAll(RedisCoordinationLayer(like), RedisOutboxLayer(like));
      }),
    ),
  );
