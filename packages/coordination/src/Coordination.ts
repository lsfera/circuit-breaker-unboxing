import { Context, Data, Duration, Effect, Option as O, Schema } from "effect";
import { readerFor, ReasonSchema, StateSchema } from "@egress/domain/Model.ts";
import { randomUUID } from "node:crypto";

/**
 * Exactly one instance publishes (LeaderElection), and a successor continues the
 * sequence (CheckpointStore). These are the ports; the backends are
 * @egress/coordination-redis and @egress/coordination-postgres. Exclusion needs
 * state outside the process, so there is no in-process backend (the unit tests'
 * double lives in the aggregator's test/support/).
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
export const newEpoch = (): string => randomUUID();

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

export const HaSettings = Context.Reference<HaSettings>("@egress/coordination/Coordination/HaSettings", {
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
>()("@egress/coordination/Coordination/LeaderElection") {}

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
>()("@egress/coordination/Coordination/CheckpointStore") {}

/** Untrusted input: anything that does not decode reads as a cold start. */
export const readCheckpoint = readerFor(CheckpointFromJson);

/**
 * A one-sided partition leaves the promise unsettled for ever. Well under the
 * lease TTL, so a leader can fail a call and still renew in time.
 */
export const COORDINATION_TIMEOUT_MS = 1000;

/**
 * Every call a backend makes goes through this: a rejected or hung promise is a
 * failure, never a defect. The signal aborts when the call is given up on, for
 * a driver that can cancel what it sent.
 */
export const guarded = <A>(operation: string, run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({
    try: run,
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
