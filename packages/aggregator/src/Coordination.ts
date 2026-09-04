import { Context, Data, Effect, Layer, Option, Ref, Schema } from "effect";
import { ReasonSchema, StateSchema } from "@egress/domain/Model.ts";
import type { Reason, State } from "@egress/domain/Model.ts";

/**
 * What makes N aggregator instances safe to run at once. Two problems, one
 * module: exactly one instance may publish at a time (LeaderElection), and
 * whichever instance takes over next must continue the sequence rather than
 * restart it at zero (CheckpointStore). `Aggregator.ts` is the only caller;
 * everything else stays exactly as pure/testable as before.
 *
 * The in-memory layer makes a single aggregator process behave correctly
 * (and is what main.ts wires by default — solo mode is just "one instance
 * that always wins the lease," not a special case). The `RedisLike` port
 * makes an actual multi-process deployment possible: hand a real Redis
 * client to `RedisCoordinationLayer` and the Lua scripts below give the same
 * fencing guarantee across processes.
 *
 * Both paths are run, not just reasoned about. `Coordination.test.ts` drives
 * two instances against the in-memory layer; `test/integration/
 * RedisCoordination.test.ts` drives the same properties against a real Redis
 * container; and `docker compose up` deploys it — two aggregator containers,
 * `--ha=redis`, one shared `redis` service. The README's "High availability"
 * section is the single place that tracks what has actually been observed;
 * this comment deliberately does not restate it.
 */

export type LeaseToken = number;

/**
 * The minimum state needed to resume publishing for one API without
 * replaying history. `replicas`, per-replica votes, and `candidate` are
 * deliberately not carried across a failover — they repopulate from the
 * next few polls, same as any cold start. What must survive is exactly what
 * subscribers depend on staying monotonic (`sequence`) or correct
 * (`openBackoffMs` — resetting this after a failover would let a still-flaky
 * upstream get probed sooner than its real backoff allows).
 */
export type Checkpoint = {
  readonly state: State;
  readonly reason: Reason;
  readonly sequence: number;
  readonly changedAt: number;
  readonly openBackoffMs: number;
};

export class CheckpointFenced extends Data.TaggedError("CheckpointFenced")<{
  readonly apiId: string;
  readonly attempted: LeaseToken;
  readonly current: LeaseToken;
}> {}

/**
 * Runtime identity, as opposed to AggregatorConfig's breaker tuning — this is
 * "which process am I," not "how should the state machine behave."
 */
export type HaSettings = {
  readonly instanceId: string;
  readonly leaseTtlMs: number;
};

export const defaultHaSettings: HaSettings = {
  instanceId: `solo-${Math.random().toString(36).slice(2, 10)}`,
  leaseTtlMs: 5000,
};

export const HaSettings = Context.Reference<HaSettings>("HaSettings", {
  defaultValue: () => defaultHaSettings,
});

/**
 * A fencing token strictly increases every time leadership changes hands
 * (never on a mere renewal) — that's what lets CheckpointStore reject writes
 * from an instance that thinks it's still leader but has actually been
 * superseded (e.g. after a long GC pause outlives the lease TTL).
 */
export class LeaderElection extends Context.Service<
  LeaderElection,
  {
    readonly tryAcquireOrRenew: (
      holderId: string,
      ttlMs: number,
    ) => Effect.Effect<Option.Option<LeaseToken>>;
    readonly release: (holderId: string) => Effect.Effect<void>;
  }
>()("LeaderElection") {}

export class CheckpointStore extends Context.Service<
  CheckpointStore,
  {
    /**
     * Rejects with CheckpointFenced if `token` is not the *current* lease
     * token — checked against the same authoritative counter LeaderElection
     * issues from, not a per-API "last write wins" value. That distinction
     * is the whole point: fencing against a per-key value only stops a stale
     * writer once someone else has already written that exact key, which
     * leaves a real window open for an API the new leader hasn't touched
     * yet. Fencing against the shared counter closes it immediately on
     * handoff, for every key at once.
     */
    readonly save: (
      apiId: string,
      token: LeaseToken,
      checkpoint: Checkpoint,
    ) => Effect.Effect<void, CheckpointFenced>;
    readonly load: (apiId: string) => Effect.Effect<Option.Option<Checkpoint>>;
  }
>()("CheckpointStore") {}

// ---------------------------------------------------------------------------
// In-memory implementation — correct for a single process, and what lets a
// test run two independent "aggregator instances" against one shared
// coordinator to exercise real failover/fencing without a second process.
// LeaderElection and CheckpointStore are built together from one shared
// token Ref for exactly the reason in the CheckpointStore doc comment above.
// ---------------------------------------------------------------------------

type LockState = {
  readonly holderId: string;
  readonly token: LeaseToken;
  readonly expiresAt: number;
} | null;

export const makeInMemoryCoordination = Effect.gen(function* () {
  const lock = yield* Ref.make<LockState>(null);
  const checkpoints = yield* Ref.make(new Map<string, Checkpoint>());

  const tryAcquireOrRenew = (holderId: string, ttlMs: number) =>
    Effect.clockWith((c) => c.currentTimeMillis).pipe(
      Effect.flatMap((now) =>
        Ref.modify(lock, (current) => {
          if (current && current.expiresAt > now && current.holderId !== holderId) {
            return [Option.none(), current]; // someone else holds a live lease
          }
          if (current && current.holderId === holderId && current.expiresAt > now) {
            // Renewal: same token, extended TTL.
            return [Option.some(current.token), { ...current, expiresAt: now + ttlMs }];
          }
          // Expired or never held: a genuine handoff, token strictly increases.
          const token = (current?.token ?? 0) + 1;
          return [Option.some(token), { holderId, token, expiresAt: now + ttlMs }];
        }),
      ),
    );

  const release = (holderId: string) =>
    Ref.update(lock, (current) => (current?.holderId === holderId ? null : current));

  const currentToken = Ref.get(lock).pipe(Effect.map((l) => l?.token ?? 0));

  const save = (apiId: string, token: LeaseToken, checkpoint: Checkpoint) =>
    currentToken.pipe(
      Effect.flatMap((current) =>
        token < current
          ? Effect.fail(new CheckpointFenced({ apiId, attempted: token, current }))
          : Ref.update(checkpoints, (map) => new Map(map).set(apiId, checkpoint)),
      ),
    );

  const load = (apiId: string) =>
    Ref.get(checkpoints).pipe(Effect.map((map) => Option.fromUndefinedOr(map.get(apiId))));

  return {
    leaderElection: { tryAcquireOrRenew, release },
    checkpointStore: { save, load },
  };
});

/** Both services, sharing the one token counter that makes fencing correct. */
export const InMemoryCoordinationLayer: Layer.Layer<LeaderElection | CheckpointStore> =
  Layer.unwrap(
    Effect.map(makeInMemoryCoordination, ({ leaderElection, checkpointStore }) =>
      Layer.mergeAll(
        Layer.succeed(LeaderElection, leaderElection),
        Layer.succeed(CheckpointStore, checkpointStore),
      ),
    ),
  );

// ---------------------------------------------------------------------------
// Redis-backed port, wired by main.ts under `--ha=redis`.
//
// `RedisLike` is deliberately the smallest surface any real client (ioredis,
// node-redis) satisfies with a one-line adapter — one `eval` method. main.ts
// happens to pin ioredis for the demo deployment, but nothing in this module
// knows that, so swapping the client is an adapter change and not a rewrite.
// ---------------------------------------------------------------------------

/**
 * A checkpoint read back out of Redis is untrusted input, whatever wrote it.
 * Decoding it rather than casting means a truncated write, a value left by an
 * older build, or anything else that does not match this shape is treated as
 * "no checkpoint" — a cold start, which the aggregator already handles
 * correctly — instead of seeding the breaker with a `sequence` of `undefined`
 * and publishing `NaN` to every subscriber. Same stance the daemons take on
 * an undecodable control event, applied to the one piece of state that
 * outlives the process.
 */
const CheckpointFromJson = Schema.Struct({
  state: StateSchema,
  // The reason is validated against the real set rather than accepted as any
  // string and cast. A cast here would have made this function look like a
  // validator while letting anything through the one field it did not check.
  reason: ReasonSchema,
  sequence: Schema.Number,
  changedAt: Schema.Number,
  openBackoffMs: Schema.Number,
});

const decodeCheckpoint = Schema.decodeUnknownOption(CheckpointFromJson);

export type RedisLike = {
  readonly eval: (
    script: string,
    options: { readonly keys: ReadonlyArray<string>; readonly args: ReadonlyArray<string> },
  ) => Promise<string | number | null>;
};

const ACQUIRE_SCRIPT = `
local holder = redis.call("GET", KEYS[1])
if holder == false then
  local token = redis.call("INCR", KEYS[2])
  redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
  return token
elseif holder == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return tonumber(redis.call("GET", KEYS[2]))
else
  return -1
end
`;

const RELEASE_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

// Fences against KEYS[1], the *same* token key ACQUIRE_SCRIPT writes to —
// not a per-API value — so a handoff closes the window for every API at
// once, including ones the new leader has not published for yet. GET+SET
// happen inside one script, so there is no read-then-write gap for Redis to
// interleave a concurrent writer into.
const CHECKPOINT_SCRIPT = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if tonumber(ARGV[1]) < current then
  return current
end
redis.call("SET", KEYS[2], ARGV[2])
return tonumber(ARGV[1])
`;

export const RedisCoordinationLayer = (
  redis: RedisLike,
  keyPrefix = "egress:aggregator",
): Layer.Layer<LeaderElection | CheckpointStore> =>
  Layer.mergeAll(
    Layer.succeed(LeaderElection, {
      tryAcquireOrRenew: (holderId, ttlMs) =>
        Effect.promise(() =>
          redis.eval(ACQUIRE_SCRIPT, {
            keys: [`${keyPrefix}:leader:holder`, `${keyPrefix}:leader:token`],
            args: [holderId, String(ttlMs)],
          }),
        ).pipe(
          Effect.map((result) => {
            const token = Number(result);
            return token > 0 ? Option.some(token) : Option.none();
          }),
        ),
      release: (holderId) =>
        Effect.promise(() =>
          redis.eval(RELEASE_SCRIPT, {
            keys: [`${keyPrefix}:leader:holder`],
            args: [holderId],
          }),
        ).pipe(Effect.asVoid),
    }),
    Layer.succeed(CheckpointStore, {
      save: (apiId, token, checkpoint) =>
        Effect.promise(() =>
          redis.eval(CHECKPOINT_SCRIPT, {
            keys: [`${keyPrefix}:leader:token`, `${keyPrefix}:checkpoint:${apiId}`],
            args: [String(token), JSON.stringify(checkpoint)],
          }),
        ).pipe(
          Effect.flatMap((result) => {
            const current = Number(result);
            return current === token
              ? Effect.void
              : Effect.fail(new CheckpointFenced({ apiId, attempted: token, current }));
          }),
        ),
      load: (apiId) =>
        Effect.promise(() =>
          redis.eval("return redis.call('GET', KEYS[1])", {
            keys: [`${keyPrefix}:checkpoint:${apiId}`],
            args: [],
          }),
        ).pipe(
          Effect.map((raw) => {
            if (typeof raw !== "string") return Option.none<Checkpoint>();
            try {
              return decodeCheckpoint(JSON.parse(raw));
            } catch {
              return Option.none<Checkpoint>(); // not even JSON
            }
          }),
        ),
    }),
  );
