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

export type EnvoyReplica = { readonly replicaId: string; readonly adminUrl: string };

export const parseStats = (
  replicaId: string,
  body: { stats?: ReadonlyArray<{ name: string; value?: number }> },
  now: number,
  keep: (cluster: string) => boolean,
): ReplicaReport[] => {
  const byCluster = new Map<string, Record<string, number>>();
  for (const stat of body.stats ?? []) {
    const m = PATTERN.exec(stat.name);
    if (!m) continue;
    const cluster = m[1];
    const suffix = m[2];
    if (cluster === undefined || suffix === undefined) continue;
    if (!keep(cluster)) continue;
    const slot = byCluster.get(cluster) ?? {};
    slot[suffix] = stat.value ?? 0;
    byCluster.set(cluster, slot);
  }
  return [...byCluster].map(([apiId, s]) => ({
    replicaId,
    apiId,
    healthy: s["membership_healthy"] ?? 0,
    total: s["membership_total"] ?? 0,
    ejectionsActive: s["outlier_detection.ejections_active"] ?? 0,
    overflowTotal:
      (s["upstream_rq_pending_overflow"] ?? 0) +
      (s["upstream_cx_overflow"] ?? 0) +
      (s["upstream_rq_retry_overflow"] ?? 0),
    observedAt: now,
  }));
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
       * Which replicas answered last tick.
       *
       * A replica that stops answering leaves the fleet, and every quorum in
       * @egress/domain is a fraction of whoever is left — so a departure is
       * worth one line and one increment. The state is here rather than a
       * counter per failed poll because `tickMs` is 250ms: without it, one
       * unreachable replica would be four log lines a second saying the same
       * thing, which is how the line that matters gets lost. See
       * docs/decisions/009-what-the-quorum-is-a-quorum-of.md.
       */
      const answering = new Map<string, boolean>(
        replicas.map((r) => [r.replicaId, true] as const),
      );

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
          return parseStats(replica.replicaId, body, now, (c) => known.has(c));
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
