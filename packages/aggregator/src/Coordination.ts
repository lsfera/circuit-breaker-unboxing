import { Clock, Context, Data, Duration, Effect, Layer, Option as O, Predicate, Ref, Result, Schema } from "effect";
import { readerFor, ReasonSchema, StateSchema } from "@egress/domain/Model.ts";
import { randomUUID } from "node:crypto";

/**
 * Exactly one instance publishes (LeaderElection), and a successor continues the
 * sequence (CheckpointStore). Solo mode is the in-memory layer: one instance
 * always winning its own lease.
 */

/**
 * A fencing token, `<epoch>:<counter>` on the wire. The epoch is minted when a
 * coordinator finds no state to inherit, so a counter restarting at 1 cannot let
 * a stale leader's higher counter outrank the live one (ADR 006).
 */
export type LeaseToken = {
  /** Which coordinator minted it. Tokens from different epochs are incomparable. */
  readonly epoch: string;
  /** Strictly increases within an epoch, once per genuine handoff. */
  readonly counter: number;
};

/** A UUID: an epoch colliding with an old one is a fencing token that fails open. */
const newEpoch = (): string => randomUUID();

/** `Natural` makes an unorderable counter impossible to decode. */
const LeaseTokenFromString = Schema.TemplateLiteralParser([
  Schema.NonEmptyString,
  ":",
  Schema.Natural,
]);

const decodeToken = Schema.decodeUnknownOption(LeaseTokenFromString);
const encodeToken = Schema.encodeSync(LeaseTokenFromString);

export const formatToken = (token: LeaseToken): string =>
  encodeToken([token.epoch, ":", token.counter] as const);

export const parseToken = (raw: string): O.Option<LeaseToken> =>
  O.map(decodeToken(raw), ([epoch, , counter]) => ({ epoch, counter }));

/** Not `===`: on records that is identity. */
export const sameToken = (a: LeaseToken, b: LeaseToken): boolean =>
  a.epoch === b.epoch && a.counter === b.counter;

/** The only place the ordering lives: another epoch is fenced; within one, the counter decides. */
export const isFenced = (attempted: LeaseToken, current: LeaseToken): boolean =>
  attempted.epoch !== current.epoch || attempted.counter < current.counter;

/**
 * What must survive a failover: `sequence` (subscribers depend on it) and
 * `openBackoffMs` (a reset would probe a flaky upstream early). The rest
 * repopulates from the next polls.
 */
const CheckpointFromJson = Schema.Struct({
  state: StateSchema,
  reason: ReasonSchema,
  // Naturals: an unorderable value or a negative backoff reads as no checkpoint.
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
 * A failure, not a defect: a defect ends the tick loop in a process that keeps
 * serving 200s. An instance that cannot confirm its lease must not act as leader.
 */
export class CoordinationUnavailable extends Data.TaggedError("CoordinationUnavailable")<{
  readonly operation: string;
  readonly cause: string;
}> {}

export type HaSettings = {
  readonly instanceId: string;
  readonly leaseTtlMs: number;
};

const defaultHaSettings: HaSettings = {
  instanceId: `solo-${randomUUID()}`,
  leaseTtlMs: 5000,
};

export const HaSettings = Context.Reference<HaSettings>("@egress/aggregator/Coordination/HaSettings", {
  defaultValue: () => defaultHaSettings,
});

/** The token increases on every handoff, never on a renewal. */
export class LeaderElection extends Context.Service<
  LeaderElection,
  {
    readonly tryAcquireOrRenew: (
      holderId: string,
      ttlMs: number,
    ) => Effect.Effect<O.Option<LeaseToken>, CoordinationUnavailable>;
    readonly release: (holderId: string) => Effect.Effect<void, CoordinationUnavailable>;
  }
>()("@egress/aggregator/Coordination/LeaderElection") {}

export class CheckpointStore extends Context.Service<
  CheckpointStore,
  {
    /**
     * Fenced against the one lease counter, not per key: per-key fencing only stops
     * a stale writer after someone else wrote that same API.
     */
    readonly save: (
      apiId: string,
      token: LeaseToken,
      checkpoint: Checkpoint,
    ) => Effect.Effect<void, CheckpointFenced | CoordinationUnavailable>;
    /** Fails when unreachable: `None` means a cold start, which resets the sequence. */
    readonly load: (
      apiId: string,
    ) => Effect.Effect<O.Option<Checkpoint>, CoordinationUnavailable>;
  }
>()("@egress/aggregator/Coordination/CheckpointStore") {}

// The default runtime path, the two-instance test harness, and the reference the
// Redis layer must match.

type Lock = {
  readonly holderId: string;
  readonly counter: number;
  readonly expiresAt: number;
};

export const makeInMemoryCoordination = Effect.gen(function* () {
  // Never rotates: this store cannot lose its state without the process.
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

/** Untrusted input: anything that does not decode reads as a cold start. */
const readCheckpoint = readerFor(CheckpointFromJson);

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
 * A one-sided partition leaves the promise unsettled for ever. Well under the
 * lease TTL, so a leader can fail a call and still renew in time.
 */
const COORDINATION_TIMEOUT_MS = 1000;

/** Every call to Redis goes through this, the outbox's too: a rejected or hung promise is a failure, never a defect. */
export const evalGuarded = (
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
