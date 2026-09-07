import { Context, Duration, Effect, Layer, Metric, Option as O, Ref, Schedule } from "effect";
import * as Breaker from "@egress/domain/Breaker.ts";
import { Config } from "@egress/domain/Model.ts";
import { CheckpointStore, HaSettings, LeaderElection } from "./Coordination.ts";
import { EventBus, EventSink, snapshotEvent, stateChanged } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Checkpoint, LeaseToken } from "./Coordination.ts";
import type { ApiSnapshot, CircuitEvent, Reason, State } from "@egress/domain/Model.ts";

type Registry = {
  readonly breakers: ReadonlyMap<string, Breaker.BreakerState>;
  readonly lastSnapshotAt: ReadonlyMap<string, number>;
};

export class Aggregator extends Context.Service<
  Aggregator,
  {
    /** One pass: poll the fleet, advance every breaker, publish what changed. */
    readonly tick: Effect.Effect<ReadonlyArray<CircuitEvent>>;
    readonly snapshots: Effect.Effect<ReadonlyArray<ApiSnapshot>>;
    readonly stateOf: (apiId: string) => Effect.Effect<State | null>;
    /** Whether this instance currently holds the publishing lease. */
    readonly isLeader: Effect.Effect<boolean>;
    /**
     * The tick loop. Runs until interrupted — the caller decides how to fork
     * it, so its lifetime is tied to a scope the caller owns.
     *
     * On the way out it surrenders the lease if this instance holds it, so a
     * planned stop hands leadership over immediately instead of leaving the
     * standby to wait out the TTL.
     */
    readonly run: Effect.Effect<void>;
    /**
     * When the loop last completed a pass, in epoch millis; 0 before the
     * first one.
     *
     * This is the liveness signal, and it is deliberately not leadership: a
     * standby ticks on exactly the same schedule as a leader, it just does
     * nothing but try to acquire. A health check that conflated the two would
     * report the standby unhealthy for doing its job correctly.
     */
    readonly lastTickAt: Effect.Effect<number>;
    /** The loop's interval, so a health check can say what "stalled" means in its own terms. */
    readonly tickMs: number;
  }
>()("Aggregator") {}

/**
 * Rebuild a BreakerState from its last published checkpoint rather than
 * `Breaker.initial`, when one exists. Only `state`/`reason`/`sequence`/
 * `changedAt`/`openBackoffMs` are restored — see Checkpoint's doc comment
 * for why the rest is safe to let repopulate from the next few polls.
 */
const seedFromCheckpoint = (
  apiId: string,
  checkpoint: O.Option<Checkpoint>,
  cfg: Parameters<typeof Breaker.initial>[1],
  now: number,
): Breaker.BreakerState => {
  const base = Breaker.initial(apiId, cfg, now);
  if (O.isNone(checkpoint)) return base;
  const cp = checkpoint.value;
  return {
    ...base,
    state: cp.state,
    reason: cp.reason,
    sequence: cp.sequence,
    changedAt: cp.changedAt,
    candidate: cp.state,
    candidateSince: cp.changedAt,
    openBackoffMs: cp.openBackoffMs,
  };
};

export const AggregatorLayer = Layer.effect(
  Aggregator,
  Effect.gen(function* () {
    const cfg = yield* Config;
    const ha = yield* HaSettings;
    const fleet = yield* FleetSource;
    const bus = yield* EventBus;
    const sink = yield* EventSink;
    const leader = yield* LeaderElection;
    const checkpoints = yield* CheckpointStore;

    const registry = yield* Ref.make<Registry>({
      breakers: new Map(),
      lastSnapshotAt: new Map(),
    });
    const leadership = yield* Ref.make<{ isLeader: boolean; token: LeaseToken | null }>({
      isLeader: false,
      token: null,
    });

    /**
     * Stop leading, and drop every breaker held in memory.
     *
     * The second half is not housekeeping — it is what keeps the sequence
     * guarantee intact across a demotion. Whoever leads next resumes from
     * the checkpoint and advances `sequence` past whatever is in memory
     * here; the rehydrate-on-acquire path in `tick` only fires for APIs
     * this instance has no breaker for, so a *warm* registry would let a
     * re-promoted instance silently resume from its own stale sequence and
     * republish numbers the other leader already used. Discarding it makes
     * re-promotion identical to a cold start, which is the path that is
     * actually exercised by a test.
     *
     * Losing the per-replica history costs nothing: it repopulates from the
     * next few polls, exactly as it does on a fresh start — see
     * Checkpoint's doc comment for why that is the whole point of keeping
     * checkpoints this small.
     */
    const demote = Effect.all(
      [
        Ref.set(leadership, { isLeader: false, token: null }),
        Ref.set(registry, { breakers: new Map(), lastSnapshotAt: new Map() }),
      ],
      { discard: true },
    );

    /** Flips only on change, so an outage is two log lines rather than four a second. */
    const coordinationOk = yield* Ref.make(true);

    /**
     * Stamped every pass, leader or not. `egress_aggregator_ticks_total`
     * already proves the loop is alive to Prometheus; this is the same fact
     * in a form a health check can read in one request, without a scrape
     * interval's worth of delay.
     */
    const lastTick = yield* Ref.make(0);

    const attemptTick = Effect.gen(
      function* () {
        yield* Metric.update(Telemetry.ticks, 1);
        yield* Effect.clockWith((c) => c.currentTimeMillis).pipe(
          Effect.flatMap((now) => Ref.set(lastTick, now)),
        );
        const tokenOpt = yield* leader.tryAcquireOrRenew(ha.instanceId, ha.leaseTtlMs);
        if (!(yield* Ref.get(coordinationOk))) {
          yield* Ref.set(coordinationOk, true);
          yield* Effect.logInfo(`${ha.instanceId}: coordination is reachable again`);
        }
        yield* Metric.update(Telemetry.isLeader, O.isSome(tokenOpt) ? 1 : 0);

        if (O.isNone(tokenOpt)) {
          // Standby: do not poll, do not step, do not publish. The only
          // thing a non-leader instance does is keep trying to acquire.
          yield* demote;
          return [];
        }
        const token = tokenOpt.value;
        const prior = yield* Ref.get(leadership);
        const justAcquired = !prior.isLeader || prior.token !== token;
        yield* Ref.set(leadership, { isLeader: true, token });

        const [pollDuration, reports] = yield* Effect.timed(fleet.poll);
        yield* Metric.update(Telemetry.fleetPollDuration, pollDuration);
        const now = yield* Effect.clockWith((c) => c.currentTimeMillis);

        // On a fresh acquisition, rehydrate any API this instance has not
        // seen yet from its last published checkpoint, so `sequence`
        // continues after a failover instead of restarting at zero.
        const known = yield* Ref.get(registry).pipe(Effect.map((reg) => reg.breakers));
        const toRehydrate = justAcquired
          ? [...new Set(reports.map((r) => r.apiId))].filter((id) => !known.has(id))
          : [];
        const rehydrated = yield* Effect.forEach(toRehydrate, (apiId) =>
          checkpoints.load(apiId).pipe(Effect.map((cp) => [apiId, cp] as const)),
        );
        const checkpointByApi = new Map(rehydrated);

        const events = yield* Ref.modify(registry, (reg) => {
          const breakers = new Map(reg.breakers);
          const lastSnapshotAt = new Map(reg.lastSnapshotAt);

          for (const report of reports) {
            const current =
              breakers.get(report.apiId) ??
              seedFromCheckpoint(
                report.apiId,
                checkpointByApi.get(report.apiId) ?? O.none(),
                cfg,
                now,
              );
            breakers.set(report.apiId, Breaker.ingest(current, report));
          }

          const out: CircuitEvent[] = [];
          for (const [apiId, before] of breakers) {
            const [after, change] = Breaker.step(before, now, cfg);
            breakers.set(apiId, after);

            if (change) {
              out.push(stateChanged(Breaker.snapshot(after), change.from));
              lastSnapshotAt.set(apiId, now);
              continue;
            }
            const last = lastSnapshotAt.get(apiId) ?? 0;
            if (now - last >= cfg.snapshotMs) {
              lastSnapshotAt.set(apiId, now);
              out.push(snapshotEvent(Breaker.snapshot(after)));
            }
          }
          return [out as ReadonlyArray<CircuitEvent>, { breakers, lastSnapshotAt }];
        });

        // Checkpoint every transition under this tick's token before telling
        // anyone else about it. A rejection means a newer instance has
        // already taken the lease — stop publishing immediately rather than
        // let a demoted instance keep talking; the remaining events in this
        // tick are dropped, not queued, since the new leader will re-derive
        // them itself from the next poll.
        const publishable: CircuitEvent[] = [];
        for (const e of events) {
          if (e.type === "egress.circuit.state_changed") {
            const after = (yield* Ref.get(registry)).breakers.get(e.data.apiId);
            const checkpoint: Checkpoint = {
              state: e.data.state,
              reason: e.data.reason as Reason,
              sequence: e.data.sequence,
              changedAt: now,
              openBackoffMs: after?.openBackoffMs ?? cfg.openMs,
            };
            const fenced = yield* checkpoints.save(e.data.apiId, token, checkpoint).pipe(
              Effect.as(false),
              Effect.catchTag("CheckpointFenced", (err) =>
                Effect.andThen(
                  Effect.logWarning(
                    `lost leadership publishing ${e.data.apiId}: ` +
                      `token ${err.attempted} superseded by ${err.current}`,
                  ),
                  Effect.succeed(true),
                ),
              ),
            );
            if (fenced) {
              // Same demotion as losing the lease outright, registry drop
              // included — being fenced *is* how this instance finds out
              // someone else has already moved the sequence on.
              yield* demote;
              yield* Metric.update(Telemetry.isLeader, 0);
              yield* Metric.update(
                Metric.withAttributes(Telemetry.fencingConflicts, { apiId: e.data.apiId }),
                1,
              );
              break;
            }
          }
          publishable.push(e);
        }

        // Gauges reflect the fleet's current view every tick, whether or not
        // anything published — a dashboard watching mid-dwell should not look
        // frozen just because no event crossed the publish threshold yet.
        yield* Ref.get(registry).pipe(
          Effect.flatMap((reg) =>
            Effect.forEach(
              [...reg.breakers.values()].map(Breaker.snapshot),
              (snap) =>
                Effect.all(
                  [
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitState, { apiId: snap.apiId }),
                      Telemetry.STATE_CODE[snap.state],
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitHealthyEndpoints, {
                        apiId: snap.apiId,
                      }),
                      snap.healthyEndpoints,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitTotalEndpoints, {
                        apiId: snap.apiId,
                      }),
                      snap.totalEndpoints,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitReportingReplicas, {
                        apiId: snap.apiId,
                      }),
                      snap.reportingReplicas,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitEjectionsActive, {
                        apiId: snap.apiId,
                      }),
                      snap.replicas.reduce((n, r) => n + r.ejectionsActive, 0),
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitSequence, { apiId: snap.apiId }),
                      snap.sequence,
                    ),
                  ],
                  { discard: true },
                ),
              { discard: true },
            ),
          ),
        );

        for (const e of publishable) {
          if (e.type === "egress.circuit.state_changed") {
            yield* Metric.update(
              Metric.withAttributes(Telemetry.circuitTransitions, {
                apiId: e.data.apiId,
                reason: e.data.reason,
                state: e.data.state,
              }),
              1,
            );
          } else {
            yield* Metric.update(
              Metric.withAttributes(Telemetry.circuitSnapshots, { apiId: e.data.apiId }),
              1,
            );
          }
        }

        // Publish to the in-process bus first (the console), then hand to the
        // sink, which forks delivery so a slow subscriber cannot stall the loop.
        yield* Effect.forEach(publishable, (e) => bus.publish(e), {
          discard: true,
        });
        yield* Effect.forEach(publishable, (e) => sink.deliver(e), {
          discard: true,
        });

        // Then replay whatever an earlier attempt could not deliver.
        //
        // Here, and only here, because this branch runs only when the lease
        // was acquired: two instances draining one shared outbox would deliver
        // every entry twice, which is exactly the break the sequence contract
        // exists to make visible. Forked for the same reason delivery is —
        // a subscriber that hangs must cost the loop nothing — and the sink
        // itself refuses to run two passes at once.
        yield* Effect.forkChild(sink.drainOutbox);
        return publishable;
      },
    );

    /**
     * A tick that cannot reach the coordinator is a *skipped* tick, not a dead
     * loop. Standing down is the only safe reading — an instance that cannot
     * confirm it still holds the lease must not act as leader — and the next
     * tick tries again, so recovery needs no intervention.
     *
     * The shape matters as much as the handling. This used to be an
     * `Effect.promise` deep in the Redis layer, so an outage arrived as a
     * defect, `Effect.repeat` terminated, and the loop was gone permanently in
     * a process that stayed up and kept answering 200. Measured before the
     * fix: 55 seconds without Redis ended the loop for good, and a total
     * upstream failure afterwards published nothing.
     */
    const tick: Effect.Effect<ReadonlyArray<CircuitEvent>> = attemptTick.pipe(
      Effect.catchTag("CoordinationUnavailable", (err) =>
        Effect.gen(function* () {
          yield* demote;
          yield* Metric.update(Telemetry.coordinationErrors, 1);
          // Say "not the leader" rather than saying nothing. `isLeader` is
          // only updated after an acquire attempt returns, so an instance that
          // has never reached the coordinator publishes no series at all —
          // and an alert written as `max(egress_aggregator_is_leader) == 0`
          // cannot fire on a metric that is absent. Found by partitioning one
          // instance from Redis and watching the gauge vanish instead of drop.
          yield* Metric.update(Telemetry.isLeader, 0);
          if (yield* Ref.get(coordinationOk)) {
            yield* Ref.set(coordinationOk, false);
            yield* Effect.logWarning(
              `${ha.instanceId}: coordination unavailable during ${err.operation}, ` +
                `standing down until it returns — ${err.cause}`,
            );
          }
          return [] as ReadonlyArray<CircuitEvent>;
        }),
      ),
    );

    const snapshots = Ref.get(registry).pipe(
      Effect.map((reg) =>
        [...reg.breakers.values()]
          .map(Breaker.snapshot)
          .sort((a, b) => a.apiId.localeCompare(b.apiId)),
      ),
    );

    const stateOf = (apiId: string) =>
      Ref.get(registry).pipe(
        Effect.map((reg) => reg.breakers.get(apiId)?.state ?? null),
      );

    const isLeader = Ref.get(leadership).pipe(Effect.map((l) => l.isLeader));

    /**
     * Hand the lease back on the way out.
     *
     * `LeaderElection.release` existed from the start and nothing ever called
     * it, so a lease was only ever surrendered by expiring. That is correct
     * for a crash and wasteful for a planned stop: every rolling deploy cost
     * the standby up to `leaseTtlMs` of waiting before it could acquire, with
     * nobody publishing in between. `release` only removes the lease if this
     * instance still holds it, so a demoted instance calling it is a no-op
     * rather than a way to evict whoever took over.
     *
     * Best effort by construction. This runs while the process is going away:
     * if the coordinator is unreachable, the lease expires the old way and
     * the outcome is exactly what it was before this existed. Failing here
     * would replace a clean shutdown with a noisy one and change nothing.
     */
    const releaseOnShutdown = Effect.gen(function* () {
      if (!(yield* Ref.get(leadership)).isLeader) return;
      yield* leader.release(ha.instanceId).pipe(
        Effect.flatMap(() =>
          Effect.logInfo(`${ha.instanceId}: lease released on shutdown`),
        ),
        Effect.catch((err) =>
          Effect.logWarning(
            `${ha.instanceId}: could not release the lease on shutdown ` +
              `(${err.operation}) — it will expire instead`,
          ),
        ),
      );
    });

    // The loop is a Schedule, not a setInterval. That is what lets TestClock
    // drive thousands of simulated seconds instantly and deterministically,
    // and what makes the loop interruptible as a value rather than via a
    // clearInterval handle someone has to remember to call.
    //
    // `ensuring` rather than a finalizer on the layer: the lease belongs to
    // this loop, so it should be given back exactly when the loop stops,
    // whether that is an interrupt from SIGTERM or the scope closing.
    const run = tick.pipe(
      Effect.repeat(Schedule.spaced(Duration.millis(cfg.tickMs))),
      Effect.asVoid,
      Effect.ensuring(releaseOnShutdown),
    );

    return {
      tick,
      snapshots,
      stateOf,
      isLeader,
      run,
      lastTickAt: Ref.get(lastTick),
      tickMs: cfg.tickMs,
    };
  }),
);
