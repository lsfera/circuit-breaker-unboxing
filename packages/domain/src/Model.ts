import { Context, Data, Option as O, Result, Schema } from "effect";

/** The schema owns each list; `State.CLOSED` and friends are derived from it. */
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

/** For Prometheus gauges. Shared so the aggregator's and the daemons' lines are comparable. */
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
     * Surfaced, not voted on: it disagrees with `healthy < total` when a host leaves
     * membership for a reason other than ejection.
     */
    readonly ejectionsActive: number;
  }>;
};

/**
 * The publishing leader's fencing token. A sequence alone cannot rank leaders: a
 * resumed leader and a successor re-deriving an unsaved event both reuse numbers.
 * `counter` ranks leaders within an epoch; epochs are incomparable.
 */
const LeaseSchema = Schema.Struct({
  epoch: Schema.NonEmptyString,
  counter: Schema.Natural,
});
export type Lease = typeof LeaseSchema.Type;

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
  /** Absent from a publisher that predates it, which is then ranked by sequence alone. */
  lease: Schema.optionalKey(LeaseSchema),
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

/** `malformed-json`: not our publisher. `schema-mismatch`: a version skew. */
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

/** One reader for every transport the event travels: AMQP, the Redis outbox, SSE. */
export const decodeCircuitEvent: (body: string) => Result.Result<CircuitEvent, DecodeFailure> =
  readerFor(CircuitEvent);

export type EventType = CircuitEvent["type"];

/** The only type the gapless guarantee covers; snapshots repeat the current sequence. */
export const SEQUENCED_EVENT = "egress.circuit.state_changed" satisfies EventType;

export const SNAPSHOT_EVENT = "egress.circuit.snapshot" satisfies EventType;

/** What a sequence means against the highest already seen for that API. */
export type SequenceVerdict = "first" | "duplicate" | "gap" | "next";

/**
 * Per-API sequences are gapless and never repeat. `<=`, not `===`: a leadership bug
 * shows as a sequence going backwards. Shared by the webhook and AMQP observers.
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

/** The last event a reader acted on, as `supersedes` ranks it. */
export type Applied = {
  readonly lease: O.Option<Lease>;
  readonly sequence: number;
};

/**
 * Whether an event replaces what a reader last acted on. A newer lease wins and an
 * older loses, whatever the sequence; within one lease a transition must move the
 * sequence forward and a snapshot must not move it back. A different epoch wins:
 * the coordinator lost its state, and a stale-epoch leader is fenced at its next
 * checkpoint.
 */
export const supersedes = (
  applied: O.Option<Applied>,
  incoming: { readonly type: EventType; readonly lease: O.Option<Lease>; readonly sequence: number },
): boolean =>
  O.match(applied, {
    onNone: () => true,
    onSome: (last) => {
      // No lease reads as epoch "" and counter 0: a publisher that predates it.
      const epoch = (lease: O.Option<Lease>) => O.getOrElse(O.map(lease, (l) => l.epoch), () => "");
      const counter = (lease: O.Option<Lease>) => O.getOrElse(O.map(lease, (l) => l.counter), () => 0);
      const sameEpoch = epoch(last.lease) === epoch(incoming.lease);
      const ahead = counter(incoming.lease) - counter(last.lease);
      const forward =
        incoming.type === SEQUENCED_EVENT
          ? incoming.sequence > last.sequence
          : incoming.sequence >= last.sequence;
      return !sameEpoch || ahead > 0 || (ahead === 0 && forward);
    },
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
  /** Re-entering DEGRADED this soon after closing counts as a relapse of the same incident. */
  readonly relapseWindowMs: number;
  /** The least a relapse must stay healthy before closing again: longer than a
   * flapping link's healthy windows. See `closeHoldMs` on `BreakerState`. */
  readonly relapseHoldMs: number;
  /** Cap on the earned close hold, so a link that never stops flapping still
   * gets probed for recovery rather than staying DEGRADED forever. */
  readonly maxCloseHoldMs: number;
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
  relapseWindowMs: 15000,
  relapseHoldMs: 8000,
  maxCloseHoldMs: 30000,
  replicaTimeoutMs: 5000,
  snapshotMs: 15000,
  tickMs: 250,
};

/** A `Context.Reference`, so it has a default and tests override it per call. */
export const Config = Context.Reference<AggregatorConfig>("@egress/domain/Model/Config", {
  defaultValue: () => defaultConfig,
});
