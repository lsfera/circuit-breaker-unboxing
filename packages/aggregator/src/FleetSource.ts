import { Array as Arr, Clock, Context, Effect, Layer, Metric, Ref, Result, Schema } from "effect";
import { StatsUnavailable } from "@egress/domain/Model.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ReplicaReport } from "@egress/domain/Model.ts";

export type ApiSpec = {
  readonly apiId: string;
  readonly endpoints: number;
  readonly rps: number;
  /** 0..1 probability that a single upstream request fails with 5xx. */
  readonly failureRate: number;
};

/** Where replica reports come from: simulated, polled, or pushed — all produce `ReplicaReport`. */
export class FleetSource extends Context.Service<
  FleetSource,
  {
    readonly poll: Effect.Effect<ReadonlyArray<ReplicaReport>>;
    readonly specs: Effect.Effect<ReadonlyArray<ApiSpec>>;
    /** Only meaningful for the simulator; the Envoy layer rejects it. */
    readonly setFailureRate: (
      apiId: string,
      rate: number,
    ) => Effect.Effect<boolean>;
  }
>()("@egress/aggregator/FleetSource") {}

// ---------------------------------------------------------------------------
// Simulated fleet
// ---------------------------------------------------------------------------

type OutlierParams = {
  consecutive5xx: number;
  baseEjectionTimeMs: number;
  maxEjectionTimeMs: number;
  maxEjectionPercent: number;
  /** Mirrors successful_active_health_check_uneject_host in envoy.yaml. */
  healthCheckIntervalMs: number;
};

const OUTLIER: OutlierParams = {
  consecutive5xx: 5,
  baseEjectionTimeMs: 5000,
  maxEjectionTimeMs: 30000,
  maxEjectionPercent: 100,
  healthCheckIntervalMs: 1000,
};

type Endpoint = {
  consecutive5xx: number;
  ejectedUntil: number;
  ejectionCount: number;
  nextHealthCheck: number;
};

type ReplicaSim = {
  readonly replicaId: string;
  readonly endpoints: Map<string, Endpoint[]>;
  readonly overflow: Map<string, number>;
};

/**
 * Envoy's outlier detection, simulated against each replica's own sample, so
 * replicas disagree at a partial failure rate the way real ones do.
 */
const stepReplica = (
  replica: ReplicaSim,
  specs: ReadonlyArray<ApiSpec>,
  now: number,
  dtMs: number,
  replicaCount: number,
): ReplicaReport[] => {
  const out: ReplicaReport[] = [];

  for (const spec of specs) {
    const hosts = replica.endpoints.get(spec.apiId);
    if (!hosts) continue;

    for (const h of hosts) {
      if (h.ejectedUntil === 0) continue;
      if (now >= h.nextHealthCheck) {
        h.nextHealthCheck = now + OUTLIER.healthCheckIntervalMs;
        if (Math.random() >= spec.failureRate) {
          h.ejectedUntil = 0;
          h.consecutive5xx = 0;
          h.ejectionCount = 0; // verified good, so the backoff resets
          continue;
        }
      }
      if (now >= h.ejectedUntil) {
        h.ejectedUntil = 0;
        h.consecutive5xx = 0; // Envoy resets the counter on un-ejection
      }
    }

    const live = hosts.filter((h) => h.ejectedUntil === 0);
    const ejectedNow = hosts.length - live.length;
    const budget = Math.floor((hosts.length * OUTLIER.maxEjectionPercent) / 100);
    const reqs = Math.max(
      1,
      Math.round(((spec.rps / replicaCount) * dtMs) / 1000),
    );
    const perHost = live.length > 0 ? reqs / live.length : 0;

    for (const h of live) {
      const n = Math.max(1, Math.round(perHost));
      for (let i = 0; i < n; i++) {
        if (Math.random() < spec.failureRate) h.consecutive5xx += 1;
        else h.consecutive5xx = 0;
      }
      if (
        h.consecutive5xx >= OUTLIER.consecutive5xx &&
        hosts.length - live.length + 1 <= budget
      ) {
        h.ejectionCount += 1;
        h.nextHealthCheck = now + OUTLIER.healthCheckIntervalMs;
        h.ejectedUntil =
          now +
          Math.min(
            OUTLIER.baseEjectionTimeMs * h.ejectionCount,
            OUTLIER.maxEjectionTimeMs,
          );
      }
    }

    // Full load concentrating on a shrinking pool saturates the connection
    // pool: cluster.<api>.upstream_rq_pending_overflow.
    if (live.length > 0 && ejectedNow > 0) {
      const pressure = reqs / live.length;
      if (pressure > 12) {
        replica.overflow.set(
          spec.apiId,
          (replica.overflow.get(spec.apiId) ?? 0) + Math.round(pressure - 12),
        );
      }
    }

    const healthy = hosts.filter((h) => h.ejectedUntil === 0).length;
    out.push({
      replicaId: replica.replicaId,
      apiId: spec.apiId,
      healthy,
      total: hosts.length,
      ejectionsActive: hosts.length - healthy,
      overflowTotal: replica.overflow.get(spec.apiId) ?? 0,
      observedAt: now,
    });
  }
  return out;
};

export const SimFleetLayer = (
  initialSpecs: ReadonlyArray<ApiSpec>,
  replicaCount: number,
) =>
  Layer.effect(
    FleetSource,
    Effect.gen(function* () {
      const specsRef = yield* Ref.make<ReadonlyArray<ApiSpec>>([
        ...initialSpecs,
      ]);
      const lastAt = yield* Ref.make(0);

      const replicas: ReplicaSim[] = Array.from(
        { length: replicaCount },
        (_, i) => ({
          replicaId: `envoy-${String(i).padStart(2, "0")}`,
          endpoints: new Map(
            initialSpecs.map((s) => [
              s.apiId,
              Array.from({ length: s.endpoints }, () => ({
                consecutive5xx: 0,
                ejectedUntil: 0,
                ejectionCount: 0,
                nextHealthCheck: 0,
              })),
            ]),
          ),
          overflow: new Map(initialSpecs.map((s) => [s.apiId, 0])),
        }),
      );

      const poll = Effect.gen(function* () {
        // Clock comes from the runtime, so TestClock drives the simulation
        // deterministically in tests.
        const now = yield* Clock.currentTimeMillis;
        const prev = yield* Ref.getAndSet(lastAt, now);
        const dt = prev === 0 ? 250 : Math.max(1, now - prev);
        const specs = yield* Ref.get(specsRef);
        return replicas.flatMap((r) =>
          stepReplica(r, specs, now, dt, replicas.length),
        );
      });

      return {
        poll,
        specs: Ref.get(specsRef),
        setFailureRate: (apiId, rate) =>
          Ref.modify(specsRef, (specs) => {
            if (!specs.some((s) => s.apiId === apiId)) return [false, specs];
            const clamped = Math.min(1, Math.max(0, rate));
            return [
              true,
              specs.map((s) =>
                s.apiId === apiId ? { ...s, failureRate: clamped } : s,
              ),
            ];
          }),
      };
    }),
  );

// ---------------------------------------------------------------------------
// Real Envoy fleet
// ---------------------------------------------------------------------------

const SUFFIXES = [
  "membership_healthy",
  "membership_total",
  "outlier_detection.ejections_active",
  "upstream_rq_pending_overflow",
  "upstream_cx_overflow",
  "upstream_rq_retry_overflow",
] as const;

const PATTERN = new RegExp(
  `^cluster\\.(.+)\\.(${SUFFIXES.map((s) => s.replace(/\./g, "\\.")).join("|")})$`,
);

type EnvoyReplica = { readonly replicaId: string; readonly adminUrl: string };

/**
 * Envoy's `/stats?format=json`. Histograms share the array in another shape, so each
 * entry is decoded on its own and one that isn't a numeric stat is skipped, not fatal.
 */
const decodeStats = Schema.decodeUnknownOption(Schema.Struct({ stats: Schema.optionalKey(Schema.Array(Schema.Unknown)) }));
const decodeStat = Schema.decodeUnknownOption(
  Schema.Struct({ name: Schema.String, value: Schema.optionalKey(Schema.Finite) }),
);

/**
 * The membership pair is never defaulted: `{ healthy: 0, total: 6 }` means every
 * host is gone. A cluster missing either is not reported; the replica abstains.
 */
type ParsedStats = {
  readonly reports: ReadonlyArray<ReplicaReport>;
  /** Clusters seen in the stat set but not reported, because a vote could not be computed. */
  readonly incomplete: ReadonlyArray<string>;
};

export const parseStats = (
  replicaId: string,
  body: { stats?: ReadonlyArray<{ name: string; value?: number }> },
  now: number,
  keep: (cluster: string) => boolean,
): ParsedStats => {
  // One slot per cluster, built from the stats whose name matches the pattern
  // and whose cluster we were asked to keep.
  const byCluster = Arr.reduce(
    Arr.filterMap(body.stats ?? [], (stat) => {
      const matched = PATTERN.exec(stat.name);
      const cluster = matched?.[1];
      const suffix = matched?.[2];
      return cluster === undefined || suffix === undefined || !keep(cluster)
        ? Result.fail(stat.name)
        : Result.succeed({ cluster, suffix, value: stat.value });
    }),
    new Map<string, Record<string, number>>(),
    (acc, { cluster, suffix, value }) =>
      acc.set(cluster, {
        ...(acc.get(cluster) ?? {}),
        // A stat that matched the pattern but carried no value is the same as
        // one that never arrived — it is not a zero.
        ...(value === undefined ? {} : { [suffix]: value }),
      }),
  );

  // One rule, two outputs: a cluster missing either half of the membership pair
  // is `incomplete`, everything else is a report. `Result` is what keeps the
  // two from being decided by two separately-maintained conditions.
  const [reports, incomplete] = Arr.separate(
    Arr.map([...byCluster], ([apiId, s]): Result.Result<ReplicaReport, string> => {
      const healthy = s["membership_healthy"];
      const total = s["membership_total"];
      return healthy === undefined || total === undefined
        ? Result.fail(apiId)
        : Result.succeed({
            replicaId,
            apiId,
            healthy,
            total,
            // Zero is safe for these: not voted on, or edge-detected.
            ejectionsActive: s["outlier_detection.ejections_active"] ?? 0,
            overflowTotal:
              (s["upstream_rq_pending_overflow"] ?? 0) +
              (s["upstream_cx_overflow"] ?? 0) +
              (s["upstream_rq_retry_overflow"] ?? 0),
            observedAt: now,
          });
    }),
  );
  return { reports, incomplete };
};

/**
 * Clusters `parseStats` refused, reported once per replica and cluster: a stats
 * config that filters a gauge is a permanent condition, not a per-tick event.
 */
export const makeIncompleteReporter = () => {
  const warned = new Set<string>();
  return (replicaId: string, clusters: ReadonlyArray<string>) =>
    Effect.forEach(
      clusters.filter((c) => !warned.has(`${replicaId}/${c}`)),
      (cluster) => {
        warned.add(`${replicaId}/${cluster}`);
        return Effect.andThen(
          Effect.logWarning(
            `fleet: ${replicaId} reported cluster ${cluster} without ` +
              `membership_healthy/membership_total — not counted toward this API's ` +
              `quorum, because a missing count is not a count of zero`,
          ),
          Metric.update(
            Metric.withAttributes(Telemetry.replicasLost, { reason: "incomplete-stats" }),
            1,
          ),
        );
      },
      { discard: true },
    );
};

/** Polls each replica's admin /stats. Compose uses the push source instead. */
export const EnvoyFleetLayer = (
  replicas: ReadonlyArray<EnvoyReplica>,
  specs: ReadonlyArray<ApiSpec>,
) =>
  Layer.effect(
    FleetSource,
    Effect.gen(function* () {
      const known = new Set(specs.map((s) => s.apiId));

      /**
       * Which replicas answered last tick, so a departure is logged once rather
       * than four times a second at `tickMs`.
       */
      const answering = new Map<string, boolean>(
        replicas.map((r) => [r.replicaId, true] as const),
      );
      const noteIncomplete = makeIncompleteReporter();

      const departed = Effect.fnUntraced(function* (replica: EnvoyReplica, cause: unknown) {
        if (answering.get(replica.replicaId) === false) return;
        answering.set(replica.replicaId, false);
        yield* Effect.logWarning(
          `fleet: ${replica.replicaId} stopped answering (${String(cause)}) and no ` +
            `longer counts toward any quorum`,
        );
        yield* Metric.update(
          Metric.withAttributes(Telemetry.replicasLost, { reason: "unreachable" }),
          1,
        );
      });

      const returned = (replica: EnvoyReplica) =>
        answering.get(replica.replicaId) === false
          ? Effect.andThen(
              Effect.sync(() => answering.set(replica.replicaId, true)),
              Effect.logInfo(`fleet: ${replica.replicaId} is answering again`),
            )
          : Effect.void;

      const pollOne = Effect.fnUntraced(
        function* (replica: EnvoyReplica) {
          const now = yield* Clock.currentTimeMillis;
          const res = yield* Effect.tryPromise({
            try: (signal) =>
              fetch(`${replica.adminUrl}/stats?format=json`, { signal }),
            catch: (cause) =>
              new StatsUnavailable({
                replicaId: replica.replicaId,
                cause: String(cause),
              }),
          });
          const json = yield* Effect.tryPromise({
            try: (): Promise<unknown> => res.json(),
            catch: (cause) => new StatsUnavailable({ replicaId: replica.replicaId, cause: String(cause) }),
          });
          const body = yield* Effect.fromOption(decodeStats(json)).pipe(
            Effect.mapError(() => new StatsUnavailable({ replicaId: replica.replicaId, cause: "not Envoy's stats JSON" })),
          );
          yield* returned(replica);
          const stats = Arr.getSomes(Arr.map(body.stats ?? [], (stat) => decodeStat(stat)));
          const parsed = parseStats(replica.replicaId, { stats }, now, (c) => known.has(c));
          yield* noteIncomplete(replica.replicaId, parsed.incomplete);
          return parsed.reports as ReplicaReport[];
        },
        Effect.timeout("2 seconds"),
        // An unreachable replica abstains, and `departed` says so once. `catch`,
        // not `catchCause`: a parser defect must not pass as a missing replica.
        (effect, replica) =>
          Effect.catch(effect, (cause) => Effect.as(departed(replica, cause), [] as ReplicaReport[])),
      );

      return {
        poll: Effect.forEach(replicas, pollOne, { concurrency: "unbounded" }).pipe(
          Effect.map((xs) => xs.flat()),
        ),
        specs: Effect.succeed(specs),
        setFailureRate: () => Effect.succeed(false),
      };
    }),
  );
