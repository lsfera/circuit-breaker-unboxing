import { Context, Effect, Layer, Metric, Ref } from "effect";
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

/**
 * Where replica reports come from. The simulated and Envoy-backed layers are
 * interchangeable because both produce ReplicaReport — swapping ingestion for
 * the push-based MetricsService sink later means writing one more Layer and
 * changing nothing else.
 */
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
>()("FleetSource") {}

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
 * Runs the same outlier-detection algorithm Envoy does — consecutive 5xx,
 * ejection backoff proportional to how many times a host has already been
 * ejected, capped by max_ejection_time, and active health checks that un-eject
 * a host as soon as it proves healthy — against each replica's OWN sample.
 *
 * That independent sampling is the point: at a partial failure rate, replicas
 * legitimately disagree about which hosts are bad. Publishing straight from a
 * replica would emit that disagreement to subscribers as flapping.
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
        const now = yield* Effect.clockWith((c) => c.currentTimeMillis);
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
 * What one replica's stats say, and which clusters it did not say enough about.
 *
 * `membership_healthy` and `membership_total` are the only two stats a vote is
 * computed from, and neither may be defaulted: `{ healthy: 0, total: 6 }` is how
 * Envoy says *every host is gone*, so a missing gauge would decode as the most
 * consequential reading in the domain. A cluster missing either is not reported
 * at all — the replica abstains, which the quorum already handles and 009 counts.
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
  const byCluster = new Map<string, Record<string, number>>();
  for (const stat of body.stats ?? []) {
    const m = PATTERN.exec(stat.name);
    if (!m) continue;
    const cluster = m[1];
    const suffix = m[2];
    if (cluster === undefined || suffix === undefined) continue;
    if (!keep(cluster)) continue;
    const slot = byCluster.get(cluster) ?? {};
    // A stat that matched the pattern but carried no value is the same as one
    // that never arrived — it is not a zero.
    if (stat.value !== undefined) slot[suffix] = stat.value;
    byCluster.set(cluster, slot);
  }

  const reports: ReplicaReport[] = [];
  const incomplete: string[] = [];
  for (const [apiId, s] of byCluster) {
    const healthy = s["membership_healthy"];
    const total = s["membership_total"];
    if (healthy === undefined || total === undefined) {
      incomplete.push(apiId);
      continue;
    }
    reports.push({
      replicaId,
      apiId,
      healthy,
      total,
      // These four keep their zero default, and it is safe where the two
      // above were not: `ejectionsActive` is surfaced rather than voted on,
      // and the overflow counters are edge-detected as a delta, so a zero
      // reads as "nothing new" instead of as a state. They cannot reach here
      // without the membership pair anyway, which is the point of the guard.
      ejectionsActive: s["outlier_detection.ejections_active"] ?? 0,
      overflowTotal:
        (s["upstream_rq_pending_overflow"] ?? 0) +
        (s["upstream_cx_overflow"] ?? 0) +
        (s["upstream_rq_retry_overflow"] ?? 0),
      observedAt: now,
    });
  }
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

/**
 * Polls each replica's admin /stats. Polling is the prototype's ingestion path
 * because it needs no proto codegen; production should use the push-based
 * envoy.service.metrics.v3.MetricsService sink, which produces the same reports.
 */
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

      const departed = (replica: EnvoyReplica, cause: unknown) =>
        Effect.gen(function* () {
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

      const pollOne = (replica: EnvoyReplica) =>
        Effect.gen(function* () {
          const now = yield* Effect.clockWith((c) => c.currentTimeMillis);
          const res = yield* Effect.tryPromise({
            try: (signal) =>
              fetch(`${replica.adminUrl}/stats?format=json`, { signal }),
            catch: (cause) =>
              new StatsUnavailable({
                replicaId: replica.replicaId,
                cause: String(cause),
              }),
          });
          const body = yield* Effect.tryPromise({
            try: () => res.json() as Promise<{ stats?: { name: string; value?: number }[] }>,
            catch: (cause) =>
              new StatsUnavailable({
                replicaId: replica.replicaId,
                cause: String(cause),
              }),
          });
          yield* returned(replica);
          const parsed = parseStats(replica.replicaId, body, now, (c) => known.has(c));
          yield* noteIncomplete(replica.replicaId, parsed.incomplete);
          return parsed.reports as ReplicaReport[];
        }).pipe(
          Effect.timeout("2 seconds"),
          // One unreachable replica must not fail the whole poll — the quorum
          // rule already tolerates a missing replica.
          //
          // `catchAll`, not `catchCause`: the expected failures here are a
          // replica being unreachable and the poll timing out, and both mean
          // "no report from this one". A *defect* means a bug — a parser that
          // throws on a stat it did not expect, say — and disguising that as
          // an unreachable replica would turn a crash into a fleet that
          // quietly reports fewer members, which is far harder to notice.
          // (`Effect.catch` is v4's failure-only catch; v3's `catchAll` is gone.)
          //
          // Tolerated, but no longer unremarked: the poll continues without
          // this replica, and `departed` says so once so the shrinking
          // denominator is visible rather than merely survivable.
          Effect.catch((cause) =>
            Effect.as(departed(replica, cause), [] as ReplicaReport[]),
          ),
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
