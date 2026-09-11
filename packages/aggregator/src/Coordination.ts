import { Clock, Context, Data, Duration, Effect, Layer, Option as O, Ref, Result, Schema } from "effect";
import { readerFor, ReasonSchema, StateSchema } from "@egress/domain/Model.ts";
import { randomUUID } from "node:crypto";

/**
 * What makes N aggregator instances safe to run at once: exactly one may publish
 * (LeaderElection), and whoever takes over continues the sequence rather than
 * restarting at zero (CheckpointStore). `Aggregator.ts` is the only caller.
 *
 * The in-memory layer is not a special case — solo mode is one instance that
 * always wins its own lease. `RedisLike` is a one-method port so a real client
 * plugs in without this module depending on one.
 */

/**
 * `"<epoch>:<counter>"`. The counter alone is not enough: a coordinator that
 * loses its state starts counting from 1 again, and a paused leader holding a
 * higher token would then out-rank the live one. The epoch is minted when a
 * coordinator finds no state to inherit, so a token from before that is not
 * merely stale, it is unrecognisable.
 */
/**
 * A fencing token: which coordinator issued it, and how many handoffs had
 * happened when it did. A record rather than the string the coordinator stores,
 * so a counter that cannot be ordered cannot be built — the string form lives at
 * the boundary in `formatToken`/`parseToken`, not in every comparison.
 */
export type LeaseToken = {
  /** Which coordinator minted it. Tokens from different epochs are incomparable. */
  readonly epoch: string;
  /** Strictly increases within an epoch, once per genuine handoff. */
  readonly counter: number;
};

/**
 * Fresh identity for a coordinator that has no state to inherit. A UUID rather
 * than a short random string: an epoch that collides with one it is supposed to
 * be unrecognisable to is a fencing token that fails open.
 */
const newEpoch = (): string => randomUUID();

/**
 * The wire form, declared once rather than split across a parser and a formatter
 * kept in agreement by hand. `Natural` is what makes an unorderable counter
 * impossible to decode.
 */
const LeaseTokenFromString = Schema.TemplateLiteralParser([
  Schema.NonEmptyString,
  ":",
  Schema.Natural,
]);

const decodeToken = Schema.decodeUnknownOption(LeaseTokenFromString);
const encodeToken = Schema.encodeSync(LeaseTokenFromString);

/** The wire form: the coordinator stores a string, so one is produced here and nowhere else. */
export const formatToken = (token: LeaseToken): string =>
  encodeToken([token.epoch, ":", token.counter] as never);

/**
 * Read a token back off the wire. `None` for anything that is not
 * `<epoch>:<non-negative integer>`.
 */
export const parseToken = (raw: string): O.Option<LeaseToken> =>
  O.map(decodeToken(raw), ([epoch, , counter]) => ({ epoch, counter }));

/** Two tokens are the same handoff. Records need this said explicitly; strings got it for free. */
export const sameToken = (a: LeaseToken, b: LeaseToken): boolean =>
  a.epoch === b.epoch && a.counter === b.counter;

/**
 * Is `attempted` superseded by `current`?
 *
 * Total, and the only place the ordering rule lives: a different epoch is
 * incomparable and therefore fenced, and within an epoch the counter decides.
 */
export const isFenced = (attempted: LeaseToken, current: LeaseToken): boolean =>
  attempted.epoch !== current.epoch || attempted.counter < current.counter;

/**
 * The minimum state needed to resume publishing for one API without
 * replaying history. `replicas`, per-replica votes, and `candidate` are
 * deliberately not carried across a failover — they repopulate from the
 * next few polls, same as any cold start. What must survive is exactly what
 * subscribers depend on staying monotonic (`sequence`) or correct
 * (`openBackoffMs` — resetting this after a failover would let a still-flaky
 * upstream get probed sooner than its real backoff allows).
 */
const CheckpointFromJson = Schema.Struct({
  state: StateSchema,
  reason: ReasonSchema,
  // Naturals, not numbers, for the reason the published event's sequence is
  // one: these are read back from a store that outlives the process, and each
  // is either ordered against another (`sequence`, `changedAt`) or used as a
  // duration to wait out (`openBackoffMs`). A value that cannot be ordered, or
  // a negative backoff that makes the next probe immediate and permanent, is
  // not a checkpoint — it reads as none, and a cold start is already handled.
  sequence: Schema.Natural,
  changedAt: Schema.Natural,
  openBackoffMs: Schema.Natural,
});

export type Checkpoint = typeof CheckpointFromJson.Type;

export class CheckpointFenced extends Data.TaggedError("CheckpointFenced")<{
  readonly apiId: string;
  readonly attempted: LeaseToken;
  /** `None` when the coordinator returned something that is not a token at all. */
  readonly current: O.Option<LeaseToken>;
}> {}

/**
 * The coordinator could not be reached — a *failure*, not a defect. A defect
 * escaping the tick terminates `Effect.repeat`, which ends the control loop for
 * good in a process that stays up and keeps serving 200s.
 *
 * There is one safe reading: an instance that cannot confirm it holds the lease
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

const defaultHaSettings: HaSettings = {
  instanceId: `solo-${randomUUID()}`,
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
    ) => Effect.Effect<O.Option<LeaseToken>, CoordinationUnavailable>;
    readonly release: (holderId: string) => Effect.Effect<void, CoordinationUnavailable>;
  }
>()("LeaderElection") {}

export class CheckpointStore extends Context.Service<
  CheckpointStore,
  {
    /**
     * Rejects with CheckpointFenced unless `token` is the current lease token,
     * checked against the one counter LeaderElection issues from. Fencing per-key
     * instead would only stop a stale writer after someone else had written that
     * exact key, leaving every untouched API open.
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
    ) => Effect.Effect<O.Option<Checkpoint>, CoordinationUnavailable>;
  }
>()("CheckpointStore") {}

// ---------------------------------------------------------------------------
// In-memory implementation. Not a stub, and not a leftover — it is load
// bearing three ways:
//
//  - It is the *default* runtime path. `pnpm start` passes no `--ha`, so the
//    console, the sim fleet and everything in the README's quickstart run on
//    this. Solo is not a special case of the HA machinery; it is what that
//    machinery does when only one instance is running.
//  - It is what lets a test run two independent "aggregator instances" against
//    one shared coordinator, so failover, re-promotion and fencing are
//    exercised for real without a second process or a container.
//  - It is the reference the Redis implementation has to match. Two layers,
//    one set of semantics, and the same tests pointed at both.
//
// LeaderElection and CheckpointStore are built together from one shared token
// Ref for exactly the reason in the CheckpointStore doc comment above.
// ---------------------------------------------------------------------------

type Lock = {
  readonly holderId: string;
  readonly counter: number;
  readonly expiresAt: number;
};

export const makeInMemoryCoordination = Effect.gen(function* () {
  // One coordinator, one epoch: an in-memory store cannot lose its state
  // without the process going with it, so the epoch never rotates here. It
  // exists so this layer and the Redis one have the same shape rather than
  // two notions of what a token is.
  const epoch = newEpoch();
  const lock = yield* Ref.make<O.Option<Lock>>(O.none());
  const checkpoints = yield* Ref.make(new Map<string, Checkpoint>());

  const tryAcquireOrRenew = (holderId: string, ttlMs: number) =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        Ref.modify(lock, (current) => {
          type Outcome = readonly [O.Option<LeaseToken>, O.Option<Lock>];
          /** Someone else holds a live lease: no token, and the lock is left alone. */
          const deny: Outcome = [O.none(), current];
          /** Same holder, same token, extended TTL. */
          const renew = (held: Lock): Outcome => [
            O.some({ epoch, counter: held.counter }),
            O.some({ ...held, expiresAt: now + ttlMs }),
          ];
          /** Expired or never held: a genuine handoff, counter strictly increases. */
          const handOver = (previous: number): Outcome => [
            O.some({ epoch, counter: previous + 1 }),
            O.some({ holderId, counter: previous + 1, expiresAt: now + ttlMs }),
          ];
          return O.match(current, {
            onNone: () => handOver(0),
            onSome: (held) =>
              held.expiresAt <= now
                ? handOver(held.counter)
                : held.holderId === holderId
                  ? renew(held)
                  : deny,
          });
        }),
      ),
    );

  const release = (holderId: string) =>
    Ref.update(lock, O.filter((held) => held.holderId !== holderId));

  const currentToken = Ref.get(lock).pipe(
    Effect.map((l): LeaseToken => ({
      epoch,
      counter: O.getOrElse(
        O.map(l, (held) => held.counter),
        () => 0,
      ),
    })),
  );

  const save = (apiId: string, token: LeaseToken, checkpoint: Checkpoint) =>
    currentToken.pipe(
      Effect.flatMap((current) =>
        isFenced(token, current)
          ? Effect.fail(
              new CheckpointFenced({ apiId, attempted: token, current: O.some(current) }),
            )
          : Ref.update(checkpoints, (map) => new Map(map).set(apiId, checkpoint)),
      ),
    );

  const load = (apiId: string) =>
    Ref.get(checkpoints).pipe(Effect.map((map) => O.fromUndefinedOr(map.get(apiId))));

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
 * A checkpoint read back is untrusted input, whatever wrote it. Through the
 * same reader the event contract uses, so "malformed" means one thing across
 * this system: anything that does not decode reads as "no checkpoint" — a cold
 * start, which is handled — instead of seeding the breaker with `undefined`.
 */
const readCheckpoint = readerFor(CheckpointFromJson);

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

/**
 * How long a coordination call may take before it counts as unavailable.
 * "Stands down and retries next tick" is only true if the call *returns* — a
 * one-sided partition can leave the client queueing against a connection it
 * never establishes, and the promise never settles.
 *
 * Well under `leaseTtlMs`: a leader must be able to fail a call, notice, and
 * still renew inside its lease.
 */
const COORDINATION_TIMEOUT_MS = 1000;

/** Every call to the store goes through this: a rejected promise is a failure, never a defect. */
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
          // Parsed at the boundary, once. "-1" is the script's way of saying
          // someone else holds a live lease; anything else that will not parse
          // is a coordinator returning something this code did not write, and
          // is treated the same way — no token.
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
          // The script echoes back the token it considers current. Anything
          // other than the one we sent means we were fenced — including a
          // value that will not parse, which is a coordinator that lost its
          // state and is a stale writer's problem either way.
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
          Effect.flatMap((raw) => {
            // Absent is ordinary: an API nobody has checkpointed yet.
            if (typeof raw !== "string") return Effect.succeed(O.none<Checkpoint>());

            // Unreadable is not, and it stays an `Option` rather than becoming
            // a failure on purpose. Failing here would take the instance out
            // of leadership over one bad key, where resuming from nothing
            // costs one API its sequence continuity — and *that* is caught
            // downstream, because a sequence starting over is exactly what the
            // delivery contract check is watching for. What it must not be is
            // silent.
            return Result.match(readCheckpoint(raw), {
              onSuccess: (checkpoint) => Effect.succeed(O.some(checkpoint)),
              onFailure: (why) =>
                Effect.as(
                  Effect.logWarning(
                    `checkpoint for ${apiId} is ${why} — resuming that API from nothing`,
                  ),
                  O.none<Checkpoint>(),
                ),
            });
          }),
        ),
    }),
  );
