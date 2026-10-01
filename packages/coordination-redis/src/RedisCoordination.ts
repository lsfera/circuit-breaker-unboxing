import { Effect, Layer, Option as O, Predicate, Result } from "effect";
import {
  CheckpointFenced,
  CheckpointStore,
  formatToken,
  guarded,
  LeaderElection,
  newEpoch,
  parseToken,
  readCheckpoint,
  sameToken,
} from "@egress/coordination/Coordination.ts";
import type { Checkpoint, LeaseToken } from "@egress/coordination/Coordination.ts";

/**
 * The ports in @egress/coordination over Redis: each operation is one Lua
 * script, so nothing interleaves with it.
 */

export type RedisLike = {
  readonly eval: (
    script: string,
    options: { readonly keys: ReadonlyArray<string>; readonly args: ReadonlyArray<string> },
  ) => Promise<string | number | null>;
};

// KEYS: holder, counter, epoch.  ARGV: holderId, ttlMs, candidateEpoch.
//
// Missing either key means lost state: adopt the candidate epoch from zero. An
// epoch surviving without its counter would restart counting inside an epoch
// stale leaders still recognise.
const ACQUIRE_SCRIPT = `
local epoch = redis.call("GET", KEYS[3])
local counter = redis.call("GET", KEYS[2])
if epoch == false or counter == false then
  epoch = ARGV[3]
  redis.call("SET", KEYS[3], epoch)
  redis.call("SET", KEYS[2], 0)
end
local holder = redis.call("GET", KEYS[1])
if holder == false then
  local issued = redis.call("INCR", KEYS[2])
  redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
  return epoch .. ":" .. issued
elseif holder == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return epoch .. ":" .. redis.call("GET", KEYS[2])
else
  return "-1"
end
`;

const RELEASE_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

// KEYS: epoch, counter, checkpoint.  ARGV: attemptedToken, payload.
// Fenced against the lease's own keys, in one script so nothing interleaves.
const CHECKPOINT_SCRIPT = `
local epoch = redis.call("GET", KEYS[1])
local counter = tonumber(redis.call("GET", KEYS[2]) or "0")
local current = (epoch or "none") .. ":" .. counter
local sep = string.find(ARGV[1], ":", 1, true)
if sep == nil or epoch == false then
  return current
end
local attemptedEpoch = string.sub(ARGV[1], 1, sep - 1)
local attemptedCounter = tonumber(string.sub(ARGV[1], sep + 1))
if attemptedEpoch ~= epoch or attemptedCounter == nil or attemptedCounter < counter then
  return current
end
redis.call("SET", KEYS[3], ARGV[2])
return ARGV[1]
`;

/**
 * Every call to Redis goes through this, the outbox's too. A call given up on
 * may still run, but over one connection it runs before anything issued after
 * it, so a late save or append cannot overtake a newer one.
 */
export const evalGuarded = (
  redis: RedisLike,
  operation: string,
  script: string,
  options: { readonly keys: ReadonlyArray<string>; readonly args: ReadonlyArray<string> },
) => guarded(operation, () => redis.eval(script, options));

export const RedisCoordinationLayer = (
  redis: RedisLike,
  keyPrefix = "egress:aggregator",
): Layer.Layer<LeaderElection | CheckpointStore> =>
  Layer.mergeAll(
    Layer.succeed(LeaderElection, {
      tryAcquireOrRenew: (holderId, ttlMs) =>
        evalGuarded(redis, "tryAcquireOrRenew", ACQUIRE_SCRIPT, {
          keys: [
            `${keyPrefix}:leader:holder`,
            `${keyPrefix}:leader:token`,
            `${keyPrefix}:leader:epoch`,
          ],
          args: [holderId, String(ttlMs), newEpoch()],
        }).pipe(
          // "-1": someone else holds it. Unparseable reads the same way.
          Effect.map((result) => {
            const raw = String(result);
            return raw === "-1" ? O.none<LeaseToken>() : parseToken(raw);
          }),
        ),
      release: (holderId) =>
        evalGuarded(redis, "release", RELEASE_SCRIPT, {
          keys: [`${keyPrefix}:leader:holder`],
          args: [holderId],
        }).pipe(Effect.asVoid),
    }),
    Layer.succeed(CheckpointStore, {
      save: (apiId, token, checkpoint) =>
        evalGuarded(redis, "save", CHECKPOINT_SCRIPT, {
          keys: [
            `${keyPrefix}:leader:epoch`,
            `${keyPrefix}:leader:token`,
            `${keyPrefix}:checkpoint:${apiId}`,
          ],
          args: [formatToken(token), JSON.stringify(checkpoint)],
        }).pipe(
          // The script echoes the current token; anything but ours is fenced.
          Effect.flatMap((result) =>
            O.match(parseToken(String(result)), {
              onNone: () =>
                Effect.fail(
                  new CheckpointFenced({ apiId, attempted: token, current: O.none() }),
                ),
              onSome: (current) =>
                sameToken(current, token)
                  ? Effect.void
                  : Effect.fail(
                      new CheckpointFenced({ apiId, attempted: token, current: O.some(current) }),
                    ),
            }),
          ),
        ),
      load: (apiId) =>
        evalGuarded(redis, "load", "return redis.call('GET', KEYS[1])", {
          keys: [`${keyPrefix}:checkpoint:${apiId}`],
          args: [],
        }).pipe(
          Effect.flatMap((reply) =>
            O.match(O.liftPredicate(reply, Predicate.isString), {
              // No key: nothing checkpointed yet.
              onNone: () => Effect.succeed(O.none<Checkpoint>()),
              // Unreadable is a logged cold start, not a failure: failing would cost
              // leadership over one key, and a restarted sequence is detected downstream.
              onSome: (raw) =>
                Result.match(readCheckpoint(raw), {
                  onSuccess: (checkpoint) => Effect.succeed(O.some(checkpoint)),
                  onFailure: (why) =>
                    Effect.as(
                      Effect.logWarning(`checkpoint for ${apiId} is ${why} — resuming that API from nothing`),
                      O.none<Checkpoint>(),
                    ),
                }),
            }),
          ),
        ),
    }),
  );
