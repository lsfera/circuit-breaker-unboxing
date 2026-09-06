import { Context, Data, Duration, Effect, Layer, Option, Ref, Schema } from "effect";
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

/**
 * `"<epoch>:<counter>"`.
 *
 * The counter alone was not enough, and the way it failed is worth keeping in
 * front of whoever reads this. It came from `INCR`, so a coordinator that lost
 * its state — a Redis restart without persistence, a failover to an empty
 * replica — started issuing from 1 again. A leader paused through that still
 * held token 5, and `attempted < current` then read `5 < 1`, which is false:
 * the stale leader was waved through and overwrote the new leader's
 * checkpoints. Split brain, reached by making the counter go backwards rather
 * than by any race. Verified against a real Redis before it was fixed.
 *
 * The epoch is generated once, when a coordinator finds itself with no state.
 * A token from a previous epoch is then not merely stale, it is
 * unrecognisable — which is the stronger and simpler property.
 */
export type LeaseToken = string;

/** Tokens are only ordered within an epoch; across epochs they are incomparable by design. */
export const tokenEpoch = (token: LeaseToken): string => token.slice(0, token.indexOf(":"));
export const tokenCounter = (token: LeaseToken): number =>
  Number(token.slice(token.indexOf(":") + 1));

/** Fresh identity for a coordinator that has no state to inherit. */
const newEpoch = (): string => Math.random().toString(36).slice(2, 10);

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
 * The coordinator could not be reached.
 *
 * This is a *failure*, deliberately, and the distinction is the whole point.
 * These calls used to be `Effect.promise`, which turns a rejected promise into
 * a defect — and a defect propagating out of the tick means `Effect.repeat`
 * terminates and the aggregator's control loop is gone for good, in a process
 * that stays up and keeps serving 200s. Measured: a 55-second Redis outage
 * stopped the loop after seven more ticks and it never restarted, and a total
 * upstream failure afterwards published nothing at all.
 *
 * Typed, it is something the caller can reason about, and there is exactly one
 * safe reading of it: an instance that cannot confirm it still holds the lease
 * must not behave as leader.
 */
export class CoordinationUnavailable extends Data.TaggedError("CoordinationUnavailable")<{
  readonly operation: string;
  readonly cause: string;
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
    ) => Effect.Effect<Option.Option<LeaseToken>, CoordinationUnavailable>;
    readonly release: (holderId: string) => Effect.Effect<void, CoordinationUnavailable>;
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
    ) => Effect.Effect<void, CheckpointFenced | CoordinationUnavailable>;
    /**
     * Fails rather than returning `None` when the store is unreachable. The
     * difference matters more than it looks: `None` means "this API has never
     * been checkpointed", which the aggregator correctly reads as a cold start
     * — and a cold start resets `sequence` to zero. A blip must never be
     * allowed to look like a fresh API.
     */
    readonly load: (
      apiId: string,
    ) => Effect.Effect<Option.Option<Checkpoint>, CoordinationUnavailable>;
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
  readonly counter: number;
  readonly expiresAt: number;
} | null;

export const makeInMemoryCoordination = Effect.gen(function* () {
  // One coordinator, one epoch: an in-memory store cannot lose its state
  // without the process going with it, so the epoch never rotates here. It
  // exists so this layer and the Redis one have the same shape rather than
  // two notions of what a token is.
  const epoch = newEpoch();
  const lock = yield* Ref.make<LockState>(null);
  const checkpoints = yield* Ref.make(new Map<string, Checkpoint>());

  const tryAcquireOrRenew = (holderId: string, ttlMs: number) =>
    Effect.clockWith((c) => c.currentTimeMillis).pipe(
      Effect.flatMap((now) =>
        Ref.modify(lock, (current) => {
          if (current && current.expiresAt > now && current.holderId !== holderId) {
            return [Option.none<LeaseToken>(), current]; // someone else holds a live lease
          }
          if (current && current.holderId === holderId && current.expiresAt > now) {
            // Renewal: same token, extended TTL.
            return [
              Option.some(`${epoch}:${current.counter}`),
              { ...current, expiresAt: now + ttlMs },
            ];
          }
          // Expired or never held: a genuine handoff, counter strictly increases.
          const next = (current?.counter ?? 0) + 1;
          return [
            Option.some(`${epoch}:${next}`),
            { holderId, counter: next, expiresAt: now + ttlMs },
          ];
        }),
      ),
    );

  const release = (holderId: string) =>
    Ref.update(lock, (current) => (current?.holderId === holderId ? null : current));

  const currentToken = Ref.get(lock).pipe(
    Effect.map((l): LeaseToken => `${epoch}:${l?.counter ?? 0}`),
  );

  const save = (apiId: string, token: LeaseToken, checkpoint: Checkpoint) =>
    currentToken.pipe(
      Effect.flatMap((current) =>
        tokenEpoch(token) !== epoch || tokenCounter(token) < tokenCounter(current)
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

// KEYS: holder, counter, epoch.  ARGV: holderId, ttlMs, candidateEpoch.
//
// A coordinator missing *either* the epoch or the counter has lost its state,
// and adopts the caller's candidate epoch starting from zero. Checking both is
// deliberate: an epoch that survived while the counter did not would otherwise
// let the counter restart inside an epoch that stale leaders still recognise,
// which is the same bug wearing a disguise.
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

// Fences against KEYS[1], the *same* token key ACQUIRE_SCRIPT writes to —
// not a per-API value — so a handoff closes the window for every API at
// once, including ones the new leader has not published for yet. GET+SET
// happen inside one script, so there is no read-then-write gap for Redis to
// interleave a concurrent writer into.
// KEYS: epoch, counter, checkpoint.  ARGV: attemptedToken, payload.
//
// Epoch first, then counter. A token from a previous epoch is not stale, it is
// unrecognisable — which is what closes the window a counter alone left open
// when the coordinator lost its state and started issuing from 1 again.
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

/** Every call to the store goes through this: a rejected promise is a failure, never a defect. */
/**
 * How long a single coordination call may take before it counts as
 * unavailable.
 *
 * "The instance stands down and retries next tick" was only ever true if the
 * call *returns*. It does not always: a one-sided partition — this instance
 * cannot reach Redis, the other one can — had the client queueing the command
 * against a connection it kept failing to establish, so the promise simply did
 * not settle. Measured: the tick loop advanced twice in twenty-five seconds and
 * then stopped, `/livez` went 503, and the instance neither led nor stood down.
 * It hung.
 *
 * Well under `leaseTtlMs` on purpose: a leader has to be able to fail a call,
 * notice, and still renew inside its lease.
 */
export const COORDINATION_TIMEOUT_MS = 1000;

const evalGuarded = (
  redis: RedisLike,
  operation: string,
  script: string,
  options: { readonly keys: ReadonlyArray<string>; readonly args: ReadonlyArray<string> },
) =>
  Effect.tryPromise({
    try: () => redis.eval(script, options),
    catch: (cause) => new CoordinationUnavailable({ operation, cause: String(cause) }),
  }).pipe(
    Effect.timeout(Duration.millis(COORDINATION_TIMEOUT_MS)),
    Effect.catchTag("TimeoutError", () =>
      Effect.fail(
        new CoordinationUnavailable({
          operation,
          cause: `no answer within ${COORDINATION_TIMEOUT_MS}ms`,
        }),
      ),
    ),
  );

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
          Effect.map((result) => {
            const token = String(result);
            return token === "-1" ? Option.none<LeaseToken>() : Option.some(token);
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
          args: [token, JSON.stringify(checkpoint)],
        }).pipe(
          Effect.flatMap((result) => {
            const current = String(result);
            return current === token
              ? Effect.void
              : Effect.fail(new CheckpointFenced({ apiId, attempted: token, current }));
          }),
        ),
      load: (apiId) =>
        evalGuarded(redis, "load", "return redis.call('GET', KEYS[1])", {
          keys: [`${keyPrefix}:checkpoint:${apiId}`],
          args: [],
        }).pipe(
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
