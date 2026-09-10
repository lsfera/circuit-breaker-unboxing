import { Context, Data, Schema } from "effect";

export const State = {
  CLOSED: "CLOSED",
  DEGRADED: "DEGRADED",
  OPEN: "OPEN",
  HALF_OPEN: "HALF_OPEN",
} as const;
export type State = (typeof State)[keyof typeof State];

export const Reason = {
  HEALTHY: "HEALTHY",
  OUTLIER_EJECTION: "OUTLIER_EJECTION",
  ALL_ENDPOINTS_EJECTED: "ALL_ENDPOINTS_EJECTED",
  THRESHOLD_OVERFLOW: "THRESHOLD_OVERFLOW",
  PROBE_SUCCEEDED: "PROBE_SUCCEEDED",
  PROBE_FAILED: "PROBE_FAILED",
  OPEN_TIMEOUT_ELAPSED: "OPEN_TIMEOUT_ELAPSED",
} as const;
export type Reason = (typeof Reason)[keyof typeof Reason];

export const Vote = { OK: "OK", DEGRADED: "DEGRADED", DOWN: "DOWN" } as const;
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
 * The vocabulary as schemas, so the literals are written once rather than
 * copied wherever something needs to validate them. Three places had their
 * own copy of the state list, and a checkpoint validator accepted any string
 * as a reason and cast it — which is not validation, it is a type assertion
 * wearing a schema's clothes.
 */
export const StateSchema = Schema.Literals(["CLOSED", "DEGRADED", "OPEN", "HALF_OPEN"]);
export const ReasonSchema = Schema.Literals([
  "HEALTHY",
  "OUTLIER_EJECTION",
  "ALL_ENDPOINTS_EJECTED",
  "THRESHOLD_OVERFLOW",
  "PROBE_SUCCEEDED",
  "PROBE_FAILED",
  "OPEN_TIMEOUT_ELAPSED",
]);

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
export const Config = Context.Reference<AggregatorConfig>("Config", {
  defaultValue: () => defaultConfig,
});
