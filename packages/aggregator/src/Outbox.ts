import { Context, Effect, Layer, Option as O, Predicate, Result, Schema } from "effect";
import { CoordinationUnavailable, evalGuarded } from "./Coordination.ts";
import type { RedisLike } from "./Coordination.ts";
import { decodeCircuitEvent, readerFor } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Where an event waits when the webhook subscriber will not take it. One consumer
 * (the leader's tick), ordered per API. Same port and shape as `CheckpointStore`.
 */

/** Overflow drops the oldest: the subscriber sees a detectable gap, not permanent staleness. */
export const OUTBOX_MAX_PER_API = 500;

/** How many entries one drain pass may replay per API. Bounded for the same reason the redrive is: a recovery must not become its own thundering herd. */
export const OUTBOX_DRAIN_LIMIT = 50;

/** `None`: no longer decodes. It keeps its position; filtering it once sent events twice (ADR 006). */
export type Entry = O.Option<CircuitEvent>;

/**
 * `from` is the absolute position of the first entry. Drains commit a position,
 * not a count: the bound can drop the oldest mid-drain, and a count would then
 * trim undelivered entries.
 */
export type Peeked = {
  readonly from: number;
  readonly entries: ReadonlyArray<Entry>;
};

export class Outbox extends Context.Service<
  Outbox,
  {
    /** Persist an undelivered event. Returns how many entries were dropped to stay inside the bound. */
    readonly append: (event: CircuitEvent) => Effect.Effect<number, CoordinationUnavailable>;
    /** The oldest `limit` entries for one API, in the order they were appended. */
    readonly peek: (apiId: string, limit: number) => Effect.Effect<Peeked, CoordinationUnavailable>;
    /**
     * Drop every entry before position `through` — called only once they have
     * been delivered, or found undeliverable. Entries already dropped by the
     * bound are not counted twice.
     */
    readonly commit: (apiId: string, through: number) => Effect.Effect<void, CoordinationUnavailable>;
    /** Which APIs currently have anything pending. */
    readonly apis: Effect.Effect<ReadonlyArray<string>, CoordinationUnavailable>;
    readonly depth: (apiId: string) => Effect.Effect<number, CoordinationUnavailable>;
  }
>()("@egress/aggregator/Outbox") {}

// ---------------------------------------------------------------------------
// Redis: the same port CheckpointStore uses, deliberately.
// ---------------------------------------------------------------------------

// The port returns one scalar, so list results are `cjson`-encoded. An empty Lua
// table encodes as `{}`, so every script that can return nothing says `[]`.
// KEYS[3] throughout is the absolute position of the list's first entry — see
// `Peeked`. It outlives the list, so a commit from an older peek never trims
// entries appended after the list last emptied.
const APPEND_SCRIPT = `
redis.call("SADD", KEYS[2], ARGV[2])
redis.call("RPUSH", KEYS[1], ARGV[1])
local len = redis.call("LLEN", KEYS[1])
local max = tonumber(ARGV[3])
if len > max then
  local dropped = len - max
  redis.call("LTRIM", KEYS[1], dropped, -1)
  redis.call("INCRBY", KEYS[3], dropped)
  return dropped
end
return 0
`;

const PEEK_SCRIPT = `
local head = tonumber(redis.call("GET", KEYS[2]) or "0")
local items = redis.call("LRANGE", KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items == 0 then return cjson.encode({ from = head, entries = "none" }) end
return cjson.encode({ from = head, entries = items })
`;

const COMMIT_SCRIPT = `
local head = tonumber(redis.call("GET", KEYS[3]) or "0")
local trim = tonumber(ARGV[1]) - head
if trim > 0 then
  redis.call("LTRIM", KEYS[1], trim, -1)
  redis.call("INCRBY", KEYS[3], trim)
end
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

/** Anything but a JSON array of strings reads as empty. */
const readStringList = readerFor(Schema.Array(Schema.String));

const NOTHING_PEEKED: Peeked = { from: 0, entries: [] };

/** PEEK_SCRIPT's answer. `entries` is the string "none" for an empty list, since cjson encodes `{}` for one. */
const readPeeked = readerFor(
  Schema.Struct({
    from: Schema.Natural,
    entries: Schema.Union([Schema.Array(Schema.String), Schema.Literal("none")]),
  }),
);

const stringList = (result: string | number | null): ReadonlyArray<string> =>
  Predicate.isString(result)
    ? Result.getOrElse(readStringList(result), () => [])
    : [];

export const RedisOutboxLayer = (redis: RedisLike, keyPrefix = "egress:aggregator") =>
  Layer.succeed(Outbox, {
    append: (event) =>
      evalGuarded(redis, "outbox.append", APPEND_SCRIPT, {
        keys: [
          `${keyPrefix}:outbox:${event.data.apiId}`,
          `${keyPrefix}:outbox:apis`,
          `${keyPrefix}:outbox:${event.data.apiId}:head`,
        ],
        args: [JSON.stringify(event), event.data.apiId, String(OUTBOX_MAX_PER_API)],
      }).pipe(Effect.map((dropped) => Number(dropped) || 0)),

    peek: (apiId, limit) =>
      evalGuarded(redis, "outbox.peek", PEEK_SCRIPT, {
        keys: [`${keyPrefix}:outbox:${apiId}`, `${keyPrefix}:outbox:${apiId}:head`],
        args: [String(limit)],
      }).pipe(
        // An undecodable entry is never replayed, but keeps its position.
        Effect.map((result): Peeked =>
          Predicate.isString(result)
            ? Result.match(readPeeked(result), {
                onFailure: () => NOTHING_PEEKED,
                onSuccess: ({ from, entries }) => ({
                  from,
                  entries: (entries === "none" ? [] : entries).map((raw) =>
                    Result.getSuccess(decodeCircuitEvent(raw)),
                  ),
                }),
              })
            : NOTHING_PEEKED,
        ),
      ),

    commit: (apiId, through) =>
      evalGuarded(redis, "outbox.commit", COMMIT_SCRIPT, {
        keys: [
          `${keyPrefix}:outbox:${apiId}`,
          `${keyPrefix}:outbox:apis`,
          `${keyPrefix}:outbox:${apiId}:head`,
        ],
        args: [String(through), apiId],
      }).pipe(Effect.asVoid),

    apis: evalGuarded(redis, "outbox.apis", APIS_SCRIPT, {
      keys: [`${keyPrefix}:outbox:apis`],
      args: [],
    }).pipe(Effect.map(stringList)),

    depth: (apiId) =>
      evalGuarded(redis, "outbox.depth", `return redis.call("LLEN", KEYS[1])`, {
        keys: [`${keyPrefix}:outbox:${apiId}`],
        args: [],
      }).pipe(Effect.map((n) => Number(n) || 0)),
  });
