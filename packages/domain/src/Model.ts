import { Context, Data, Option as O, Result, Schema } from "effect";

/**
 * The vocabulary, declared once each.
 *
 * The schema owns the list, and the object call sites read (`State.CLOSED`) is
 * derived from it — so a new state is one edit rather than two lists that
 * nothing checks against each other.
 */
const vocabulary = <const L extends ReadonlyArray<string>>(
  literals: L,
): { readonly [K in L[number]]: K } =>
  Object.fromEntries(literals.map((l) => [l, l])) as { readonly [K in L[number]]: K };

export const StateSchema = Schema.Literals(["CLOSED", "DEGRADED", "OPEN", "HALF_OPEN"]);
export type State = typeof StateSchema.Type;
export const State = vocabulary(StateSchema.literals);

export const ReasonSchema = Schema.Literals([
  "HEALTHY",
  "OUTLIER_EJECTION",
  "ALL_ENDPOINTS_EJECTED",
  "THRESHOLD_OVERFLOW",
  "PROBE_SUCCEEDED",
  "PROBE_FAILED",
  "OPEN_TIMEOUT_ELAPSED",
]);
export type Reason = typeof ReasonSchema.Type;
export const Reason = vocabulary(ReasonSchema.literals);

/** No schema: a vote is derived from a report and consumed in-process, never decoded. */
export const Vote = vocabulary(["OK", "DEGRADED", "DOWN"] as const);
export type Vote = (typeof Vote)[keyof typeof Vote];

/**
 * State as a number, for the one place a state has to be graphed rather than
 * read: a Prometheus gauge, drawn as a stepped line per API.
 *
 * It lives in the domain because both sides of the system publish it now —
 * the aggregator's view of what it decided, and each daemon's view of what
 * it was told. Two encodings would make those two lines silently
 * incomparable on the same dashboard, which is the whole reason to plot them
 * together.
 */
export const STATE_CODE: Record<State, number> = {
  CLOSED: 0,
  DEGRADED: 1,
  OPEN: 2,
  HALF_OPEN: 3,
};

/**
 * One replica's local view of one API. Maps 1:1 onto Envoy cluster stats, so
 * the simulated and real sources are interchangeable:
 *
 *   healthy         <- cluster.<api>.membership_healthy
 *   total           <- cluster.<api>.membership_total
 *   ejectionsActive <- cluster.<api>.outlier_detection.ejections_active
 *   overflowTotal   <- pending_overflow + cx_overflow + retry_overflow
 */
export type ReplicaReport = {
  readonly replicaId: string;
  readonly apiId: string;
  readonly healthy: number;
  readonly total: number;
  readonly ejectionsActive: number;
  readonly overflowTotal: number; // monotonic; the breaker edge-detects deltas
  readonly observedAt: number;
};

export type ApiSnapshot = {
  readonly apiId: string;
  readonly state: State;
  readonly reason: Reason;
  readonly sequence: number;
  readonly healthyEndpoints: number;
  readonly totalEndpoints: number;
  readonly reportingReplicas: number;
  readonly votes: Record<Vote, number>;
  readonly observedSince: number;
  readonly changedAt: number;
  /** Per-replica detail — console and metrics only, never published. */
  readonly replicas: ReadonlyArray<{
    readonly replicaId: string;
    readonly vote: Vote;
    readonly healthy: number;
    readonly total: number;
    /**
     * Carried through rather than folded into the vote. Envoy keeps ejected
     * hosts in `membership_total` and drops them from `membership_healthy`,
     * so `healthy < total` and `ejectionsActive > 0` say the same thing
     * about a *reachable* replica — but they stop agreeing the moment a host
     * leaves membership for some other reason (an EDS update, a shrinking
     * upstream), and telling those two apart is exactly what someone
     * debugging a surprise DEGRADED needs. It is surfaced, not consulted:
     * the breaker's vote stays derived from membership alone.
     */
    readonly ejectionsActive: number;
  }>;
};

/**
 * The published contract, declared once and used for both encoding and the
 * subscriber's decode.
 *
 * Two fields are stricter than the rest because two things downstream do more
 * than display them: `sequence` is the value the delivery contract orders by, so
 * anything unorderable must not decode, and `reason` is the vocabulary rather
 * than any string, so nothing has to cast it back. The counts stay `Number` —
 * they are reported, not reasoned with.
 */
const CircuitEventData = Schema.Struct({
  apiId: Schema.String,
  sequence: Schema.Natural,
  previousState: Schema.NullOr(StateSchema),
  state: StateSchema,
  reason: ReasonSchema,
  healthyEndpoints: Schema.Number,
  totalEndpoints: Schema.Number,
  observedSince: Schema.String,
  reportingReplicas: Schema.Number,
});

export const CircuitEvent = Schema.Struct({
  specversion: Schema.Literal("1.0"),
  type: Schema.Literals([
    "egress.circuit.state_changed",
    "egress.circuit.snapshot",
  ]),
  source: Schema.String,
  subject: Schema.String,
  id: Schema.String,
  time: Schema.String,
  datacontenttype: Schema.Literal("application/json"),
  data: CircuitEventData,
});

export type CircuitEvent = typeof CircuitEvent.Type;

/**
 * Why a message could not be read. The two are worth telling apart: a schema
 * mismatch is a version skew between the publisher and this reader, and
 * something that is not JSON at all means the publisher is not who we think.
 */
export type DecodeFailure = "malformed-json" | "schema-mismatch";

const parseJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/** Parse, then decode through one declaration, and say which step failed. */
export const readerFor = <S extends Schema.ConstraintDecoder<unknown>>(schema: S) => {
  const decode = Schema.decodeUnknownOption(schema);
  return (body: string): Result.Result<S["Type"], DecodeFailure> =>
    O.match(parseJson(body), {
      onNone: () => Result.fail("malformed-json"),
      onSome: (json) => Result.fromOption(decode(json), (): DecodeFailure => "schema-mismatch"),
    });
};

/**
 * The one reader for a published event, here rather than beside any single
 * transport because three of them carry it: AMQP to the daemon fleet, Redis to
 * the outbox that replays it, and SSE to a subscriber. They agree on what a
 * valid event is by sharing this, not by each checking a few fields.
 */
export const decodeCircuitEvent: (body: string) => Result.Result<CircuitEvent, DecodeFailure> =
  readerFor(CircuitEvent);

/**
 * The CloudEvents types this control plane publishes, closed by the schema that
 * publishes them: anything holding one of these is typed against the union
 * rather than `string`, so a misspelling is a compile error rather than an
 * event that silently matches nothing.
 */
export type EventType = CircuitEvent["type"];

/**
 * The one event type carrying the delivery guarantee. Snapshots deliberately
 * republish the current sequence so a late subscriber can sync, which is why
 * they are exempt rather than counted as duplicates.
 *
 * `satisfies`, not an annotation: the literal type is what lets
 * `event.type === SEQUENCED_EVENT` narrow, and the check is what keeps the
 * constant honest if the schema's union changes.
 */
export const SEQUENCED_EVENT = "egress.circuit.state_changed" satisfies EventType;

/** The heartbeat, exempt from the sequence rule for the reason above. */
export const SNAPSHOT_EVENT = "egress.circuit.snapshot" satisfies EventType;

/** What a sequence means against the highest already seen for that API. */
export type SequenceVerdict = "first" | "duplicate" | "gap" | "next";

/**
 * The published guarantee as one function: per-API sequences are gapless and
 * never repeat.
 *
 * `duplicate` on `<=`, not `===`. A sequence that goes *backwards* reuses a
 * number just as surely as one that repeats it, and that is the shape a
 * leadership bug actually produces: an instance resuming from stale in-memory
 * state republishes numbers a later leader already used. Checking only for
 * equality let that case fall through uncounted, which made the check that
 * exists to prove the contract blind to the likeliest way of breaking it.
 *
 * Two observers apply this — the aggregator's own webhook subscriber over HTTP,
 * and the daemon fleet over AMQP. They watch from genuinely different places;
 * they should not disagree about what a violation is.
 */
export const classifySequence = (
  highest: O.Option<number>,
  sequence: number,
): SequenceVerdict =>
  O.match(highest, {
    onNone: () => "first" as const,
    onSome: (last) =>
      sequence <= last ? "duplicate" : sequence > last + 1 ? "gap" : "next",
  });

export class DeliveryFailed extends Data.TaggedError("DeliveryFailed")<{
  readonly sink: string;
  readonly apiId: string;
  readonly cause: string;
}> {}

export class StatsUnavailable extends Data.TaggedError("StatsUnavailable")<{
  readonly replicaId: string;
  readonly cause: string;
}> {}

export type AggregatorConfig = {
  /** Fraction of reporting replicas that must agree before a transition. */
  readonly quorum: number;
  /** A candidate state must persist this long before it is published. */
  readonly dwellMs: number;
  /** Minimum time in a state before another transition may publish. */
  readonly minStateMs: number;
  /** Time in OPEN before probing. Doubles per failed probe, capped. */
  readonly openMs: number;
  readonly maxOpenMs: number;
  /** Consecutive healthy observations needed to close from HALF_OPEN. */
  readonly probeSuccesses: number;
  /** Replicas silent longer than this stop counting toward quorum. */
  readonly replicaTimeoutMs: number;
  /** Periodic full-state republish per API, so late subscribers can sync. */
  readonly snapshotMs: number;
  readonly tickMs: number;
};

export const defaultConfig: AggregatorConfig = {
  quorum: 0.6,
  dwellMs: 2000,
  minStateMs: 3000,
  openMs: 4000,
  maxOpenMs: 16000,
  probeSuccesses: 3,
  replicaTimeoutMs: 5000,
  snapshotMs: 15000,
  tickMs: 250,
};

/**
 * Config as a Context.Reference rather than a Service: it has a default, so
 * nothing is forced to declare a dependency on it, but any test can override it
 * for one call with Effect.provideService.
 */
export const Config = Context.Reference<AggregatorConfig>("@egress/domain/Model/Config", {
  defaultValue: () => defaultConfig,
});
