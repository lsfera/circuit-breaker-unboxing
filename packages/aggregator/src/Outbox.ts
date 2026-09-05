import { Context, Effect, Layer, Ref } from "effect";
import { CoordinationUnavailable } from "./Coordination.ts";
import type { RedisLike } from "./Coordination.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Where an event goes when the subscriber will not take it.
 *
 * The aggregator's guarantee — one gapless, strictly ordered sequence per API
 * — held everywhere except the last hop. A webhook that failed its three
 * retries landed in a 200-entry in-memory list that exists to be *looked at*,
 * not replayed, and that list dies with the process. So the contract the
 * README points at was intact inside the aggregator and best-effort at the
 * edge, which is the half a subscriber actually sees.
 *
 * This is the durable half. It is deliberately the same shape as
 * `CheckpointStore`: one `RedisLike` port, one `eval`, scripts that do their
 * read and write in the same round trip, and an in-memory implementation that
 * is not a mock but what solo mode genuinely uses.
 *
 * What it is not: a queue with delivery semantics. There is exactly one
 * consumer — the leader's tick loop — and ordering is per API, which is the
 * only ordering the published contract claims.
 */

/**
 * Per-API bound. A subscriber that stays down does not get to consume the
 * aggregator's memory, or Redis's, on its way out.
 *
 * Overflow drops the *oldest*, which is the choice worth explaining. Dropping
 * the newest would leave the subscriber's most recent knowledge permanently
 * stale while the outbox held ancient history; dropping the oldest means the
 * subscriber sees a gap — which its own delivery-integrity check is built to
 * detect and count — and then catches up to the truth. A gap you can see beats
 * a state you cannot trust, and the next snapshot republishes the current
 * state anyway.
 */
export const OUTBOX_MAX_PER_API = 500;

/** How many entries one drain pass may replay per API. Bounded for the same reason the redrive is: a recovery must not become its own thundering herd. */
export const OUTBOX_DRAIN_LIMIT = 50;

export class Outbox extends Context.Service<
  Outbox,
  {
    /** Persist an undelivered event. Returns how many entries were dropped to stay inside the bound. */
    readonly append: (event: CircuitEvent) => Effect.Effect<number, CoordinationUnavailable>;
    /** The oldest `limit` entries for one API, in the order they were appended. */
    readonly peek: (
      apiId: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<CircuitEvent>, CoordinationUnavailable>;
    /** Drop the first `count` entries for one API — called only after they have actually been delivered. */
    readonly commit: (apiId: string, count: number) => Effect.Effect<void, CoordinationUnavailable>;
    /** Which APIs currently have anything pending. */
    readonly apis: Effect.Effect<ReadonlyArray<string>, CoordinationUnavailable>;
    readonly depth: (apiId: string) => Effect.Effect<number, CoordinationUnavailable>;
  }
>()("Outbox") {}

// ---------------------------------------------------------------------------
// In memory: what solo mode uses, and what the unit tests drive.
// ---------------------------------------------------------------------------

export const makeInMemoryOutbox = Effect.gen(function* () {
  const entries = yield* Ref.make(new Map<string, ReadonlyArray<CircuitEvent>>());

  const append = (event: CircuitEvent) =>
    Ref.modify(entries, (map) => {
      const apiId = event.data.apiId;
      const next = [...(map.get(apiId) ?? []), event];
      const dropped = Math.max(0, next.length - OUTBOX_MAX_PER_API);
      return [dropped, new Map(map).set(apiId, next.slice(dropped))];
    });

  const peek = (apiId: string, limit: number) =>
    Ref.get(entries).pipe(Effect.map((map) => (map.get(apiId) ?? []).slice(0, limit)));

  const commit = (apiId: string, count: number) =>
    Ref.update(entries, (map) => {
      const rest = (map.get(apiId) ?? []).slice(count);
      const next = new Map(map);
      if (rest.length === 0) next.delete(apiId);
      else next.set(apiId, rest);
      return next;
    });

  const apis = Ref.get(entries).pipe(Effect.map((map) => [...map.keys()]));
  const depth = (apiId: string) =>
    Ref.get(entries).pipe(Effect.map((map) => (map.get(apiId) ?? []).length));

  return { append, peek, commit, apis, depth } as const;
});

export const InMemoryOutboxLayer = Layer.effect(Outbox, makeInMemoryOutbox);

// ---------------------------------------------------------------------------
// Redis: the same port CheckpointStore uses, deliberately.
// ---------------------------------------------------------------------------

/**
 * `RedisLike.eval` returns one scalar, and these scripts need to return lists.
 * Rather than widen the port — the whole point of it being one method is that
 * any client adapts to it in one line — the scripts encode with `cjson`.
 *
 * `cjson.encode` on an empty Lua table produces `{}`, an object, not `[]`, so
 * every script that can return nothing says so explicitly. A JSON parse that
 * yields an object where the caller expects an array is the kind of bug that
 * only shows up when the queue is empty, which is most of the time.
 */
const APPEND_SCRIPT = `
redis.call("SADD", KEYS[2], ARGV[2])
redis.call("RPUSH", KEYS[1], ARGV[1])
local len = redis.call("LLEN", KEYS[1])
local max = tonumber(ARGV[3])
if len > max then
  local dropped = len - max
  redis.call("LTRIM", KEYS[1], dropped, -1)
  return dropped
end
return 0
`;

const PEEK_SCRIPT = `
local items = redis.call("LRANGE", KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items == 0 then return "[]" end
return cjson.encode(items)
`;

const COMMIT_SCRIPT = `
redis.call("LTRIM", KEYS[1], tonumber(ARGV[1]), -1)
if redis.call("LLEN", KEYS[1]) == 0 then
  redis.call("DEL", KEYS[1])
  redis.call("SREM", KEYS[2], ARGV[2])
end
return redis.call("LLEN", KEYS[1])
`;

const APIS_SCRIPT = `
local members = redis.call("SMEMBERS", KEYS[1])
if #members == 0 then return "[]" end
return cjson.encode(members)
`;

const evalGuarded = (
  redis: RedisLike,
  operation: string,
  script: string,
  options: { readonly keys: ReadonlyArray<string>; readonly args: ReadonlyArray<string> },
) =>
  Effect.tryPromise({
    try: () => redis.eval(script, options),
    catch: (cause) => new CoordinationUnavailable({ operation, cause: String(cause) }),
  });

/**
 * A stored entry that cannot be parsed back into an event is dropped rather
 * than replayed, and the same reasoning as the checkpoint validator applies:
 * garbage in this position is a version skew or a corrupt write, and
 * delivering it to a subscriber that trusts the schema is worse than losing
 * it. Only the shape the publisher guarantees is let through.
 */
const parseEvent = (raw: unknown): CircuitEvent | null => {
  if (typeof raw !== "string") return null;
  try {
    const parsed = JSON.parse(raw) as CircuitEvent;
    return typeof parsed?.data?.apiId === "string" && typeof parsed?.data?.sequence === "number"
      ? parsed
      : null;
  } catch {
    return null;
  }
};

const parseList = (result: string | number | null): ReadonlyArray<unknown> => {
  if (typeof result !== "string") return [];
  try {
    const parsed = JSON.parse(result) as unknown;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

export const RedisOutboxLayer = (redis: RedisLike, keyPrefix = "egress:aggregator") =>
  Layer.succeed(Outbox, {
    append: (event) =>
      evalGuarded(redis, "outbox.append", APPEND_SCRIPT, {
        keys: [`${keyPrefix}:outbox:${event.data.apiId}`, `${keyPrefix}:outbox:apis`],
        args: [JSON.stringify(event), event.data.apiId, String(OUTBOX_MAX_PER_API)],
      }).pipe(Effect.map((dropped) => Number(dropped) || 0)),

    peek: (apiId, limit) =>
      evalGuarded(redis, "outbox.peek", PEEK_SCRIPT, {
        keys: [`${keyPrefix}:outbox:${apiId}`],
        args: [String(limit)],
      }).pipe(
        Effect.map((result) =>
          parseList(result)
            .map(parseEvent)
            .filter((e): e is CircuitEvent => e !== null),
        ),
      ),

    commit: (apiId, count) =>
      evalGuarded(redis, "outbox.commit", COMMIT_SCRIPT, {
        keys: [`${keyPrefix}:outbox:${apiId}`, `${keyPrefix}:outbox:apis`],
        args: [String(count), apiId],
      }).pipe(Effect.asVoid),

    apis: evalGuarded(redis, "outbox.apis", APIS_SCRIPT, {
      keys: [`${keyPrefix}:outbox:apis`],
      args: [],
    }).pipe(
      Effect.map((result) => parseList(result).filter((x): x is string => typeof x === "string")),
    ),

    depth: (apiId) =>
      evalGuarded(redis, "outbox.depth", `return redis.call("LLEN", KEYS[1])`, {
        keys: [`${keyPrefix}:outbox:${apiId}`],
        args: [],
      }).pipe(Effect.map((n) => Number(n) || 0)),
  });
