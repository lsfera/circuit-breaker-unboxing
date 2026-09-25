import { Context, Effect, Layer, Ref, Option as O, Predicate, Result, Schema } from "effect";
import { CoordinationUnavailable } from "./Coordination.ts";
import type { RedisLike } from "./Coordination.ts";
import { decodeCircuitEvent, readerFor } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Where an event goes when the subscriber will not take it — the durable half of
 * the guarantee, which otherwise held everywhere except the last hop.
 *
 * Same shape as `CheckpointStore`: one `RedisLike` port, scripts that read and
 * write in one round trip, and an in-memory implementation that solo mode
 * genuinely uses rather than a mock.
 *
 * Not a queue with delivery semantics: one consumer, the leader's tick loop, and
 * ordering per API, which is the only ordering the contract claims.
 */

/**
 * Per-API bound, so a subscriber that stays down cannot consume the aggregator's
 * memory on its way out.
 *
 * Overflow drops the *oldest*: the subscriber then sees a gap, which its
 * delivery-integrity check detects, and catches up. Dropping the newest would
 * leave it permanently stale while the outbox held ancient history.
 */
export const OUTBOX_MAX_PER_API = 500;

/** How many entries one drain pass may replay per API. Bounded for the same reason the redrive is: a recovery must not become its own thundering herd. */
export const OUTBOX_DRAIN_LIMIT = 50;

/**
 * One stored entry, in list order. `None` is an entry that no longer decodes —
 * a version skew, or a corrupt write.
 *
 * It keeps its position rather than being filtered out on the way up, because
 * a drain commits by *count*: an entry that vanished here would take a
 * delivered event's position with it, and the trim would stop short and leave
 * that event to be sent a second time.
 */
export type Entry = O.Option<CircuitEvent>;

/**
 * What `peek` saw, and where. `from` is the absolute position of the first
 * entry: every entry ever appended for an API has one, and the bound's drops
 * move the head forward. A drain commits the position it got to, not a count,
 * because an append can drop the oldest entries while the drain is posting
 * them, and trimming a count from the new head would remove entries nobody
 * delivered.
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
// In memory: what solo mode uses, and what the unit tests drive.
// ---------------------------------------------------------------------------

export const makeInMemoryOutbox = Effect.gen(function* () {
  /** Per API: the pending events, and the absolute position of the first. */
  type Queue = { readonly head: number; readonly events: ReadonlyArray<CircuitEvent> };
  const queues = yield* Ref.make(new Map<string, Queue>());
  const queueOf = (map: Map<string, Queue>, apiId: string): Queue =>
    map.get(apiId) ?? { head: 0, events: [] };

  const append = (event: CircuitEvent) =>
    Ref.modify(queues, (map) => {
      const apiId = event.data.apiId;
      const q = queueOf(map, apiId);
      const next = [...q.events, event];
      const dropped = Math.max(0, next.length - OUTBOX_MAX_PER_API);
      return [dropped, new Map(map).set(apiId, { head: q.head + dropped, events: next.slice(dropped) })];
    });

  const peek = (apiId: string, limit: number) =>
    Ref.get(queues).pipe(
      Effect.map((map): Peeked => {
        const q = queueOf(map, apiId);
        return { from: q.head, entries: q.events.slice(0, limit).map(O.some) };
      }),
    );

  // The head is kept when the queue empties, so a commit from an older peek
  // can never trim entries appended after it.
  const commit = (apiId: string, through: number) =>
    Ref.update(queues, (map) => {
      const q = queueOf(map, apiId);
      const trim = Math.max(0, through - q.head);
      return new Map(map).set(apiId, { head: q.head + trim, events: q.events.slice(trim) });
    });

  const apis = Ref.get(queues).pipe(
    Effect.map((map) => [...map].filter(([, q]) => q.events.length > 0).map(([apiId]) => apiId)),
  );
  const depth = (apiId: string) =>
    Ref.get(queues).pipe(Effect.map((map) => queueOf(map, apiId).events.length));

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
 * What every list-returning script above encodes: a JSON array of strings.
 * Anything else — a Lua error string, a `{}` from an empty table, a number —
 * reads as an empty list rather than as elements nobody checked the type of.
 */
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
        // A stored entry that cannot be decoded is not replayed: garbage here
        // is a version skew or a corrupt write, and handing it to a subscriber
        // that trusts the schema is worse than losing it. It still comes back
        // as a `None` in its own position — see `Entry`. An answer that does
        // not decode at all reads as nothing pending, with nothing to commit.
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
