import { Clock, Context, Duration, Effect, Layer, Metric, Option as O, Ref, Schedule } from "effect";
import * as Breaker from "@egress/domain/Breaker.ts";
import { Config, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import { CheckpointStore, HaSettings, LeaderElection } from "./Coordination.ts";
import { EventBus, EventSink, snapshotEvent, stateChanged } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import { formatToken } from "./Coordination.ts";
import type { Checkpoint, LeaseToken } from "./Coordination.ts";
import type { ApiSnapshot, CircuitEvent, State } from "@egress/domain/Model.ts";

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
    readonly stateOf: (apiId: string) => Effect.Effect<O.Option<State>>;
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
>()("Aggregator") { }

/**
 * Rebuild a BreakerState from its last published checkpoint rather than
 * `Breaker.initial`, when one exists. Only `state`/`reason`/`sequence`/
 * `changedAt`/`openBackoffMs` are restored — see Checkpoint's doc comment
 * for why the rest is safe to let repopulate from the next few polls.
 *
 * Two things here are load-bearing rather than ceremony, because spread
 * properties escape excess-property checking and tsc stays silent for both:
 *
 * - `getOrElse` leaves `Option` before the spread. Spreading the `Option`
 *   itself compiles, and contributes its one own key — `value` — so every
 *   restored field is dropped and a failover silently resumes from sequence 0.
 * - `Partial<Breaker.BreakerState>` types the overlay. Without it a mistyped
 *   field name spreads harmlessly and keeps its `initial` value, which is the
 *   same bug by a different route.
 *
 * Both were measured, and `Aggregator.test.ts`'s three checkpoint cases are
 * what catch them: the type system does not.
 */
const seedFromCheckpoint = (
  apiId: string,
  checkpoint: O.Option<Checkpoint>,
  cfg: Parameters<typeof Breaker.initial>[1],
  now: number,
): Breaker.BreakerState => ({
  ...Breaker.initial(apiId, cfg, now),
  ...O.getOrElse(
    O.map(
      checkpoint,
      (cp): Partial<Breaker.BreakerState> => ({
        state: cp.state,
        reason: cp.reason,
        sequence: cp.sequence,
        changedAt: cp.changedAt,
        candidate: cp.state,
        candidateSince: cp.changedAt,
        openBackoffMs: cp.openBackoffMs,
      }),
    ),
    () => ({}),
  ),
});

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
    const leadership = yield* Ref.make<{
      readonly isLeader: boolean;
      readonly token: O.Option<LeaseToken>;
    }>({
      isLeader: false,
      token: O.none(),
    });

    /**
     * Stop leading, and drop every breaker held in memory.
     *
     * Dropping them is what keeps the sequence guarantee across a demotion:
     * rehydrate-on-acquire only fires for APIs this instance has no breaker for,
     * so a warm registry would let a re-promoted instance resume from its own
     * stale sequence and republish numbers another leader already used. The
     * per-replica history costs nothing to lose; it repopulates in a few polls.
     */
    const demote = Effect.all(
      [
        Ref.set(leadership, { isLeader: false, token: O.none() }),
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
        yield* Clock.currentTimeMillis.pipe(
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
        yield* Ref.set(leadership, { isLeader: true, token: O.some(token) });

        const [pollDuration, reports] = yield* Effect.timed(fleet.poll);
        yield* Metric.update(Telemetry.fleetPollDuration, pollDuration);
        const now = yield* Clock.currentTimeMillis;

        // Rehydrate any API this instance is leading and has no breaker for,
        // so `sequence` continues after a failover instead of restarting at
        // zero.
        //
        // The condition is "no breaker yet", and nothing more. It used to also
        // require the *acquisition tick*, which quietly meant "only if reports
        // for that API happened to arrive on the same tick the lease was
        // taken". A polling source answers immediately, so this held in the
        // simulator and in every test; the push source cannot, because no
        // Envoy has streamed to a process that started milliseconds ago. In
        // `--source=envoy-push` — what docker-compose runs — every leader
        // change therefore cold-started every API at sequence 0, with the
        // checkpoint sitting in Redis unread and nothing logged. The daemon
        // fleet's own duplicate counter is what caught it.
        //
        // Dropping the guard costs nothing: `known.has(id)` is true from the
        // moment an API is seeded, so this is one load per API per instance,
        // not one per tick. An API first seen long after acquisition now
        // resumes too, which is the same intent applied honestly.
        const known = yield* Ref.get(registry).pipe(Effect.map((reg) => reg.breakers));
        const toRehydrate = [...new Set(reports.map((r) => r.apiId))].filter(
          (id) => !known.has(id),
        );
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

          const out = [...breakers].flatMap(([apiId, before]) => {
            const [after, change] = Breaker.step(before, now, cfg);
            breakers.set(apiId, after);

            // A transition wins over the heartbeat snapshot: both carry the
            // same state, and only the transition carries where it came from.
            const event = O.orElse(
              O.map(change, (transition) =>
                stateChanged(Breaker.snapshot(after), transition.from, now),
              ),
              () =>
                now - (lastSnapshotAt.get(apiId) ?? 0) >= cfg.snapshotMs
                  ? O.some(snapshotEvent(Breaker.snapshot(after), now))
                  : O.none(),
            );

            // Anything published restarts the snapshot clock, so a transition
            // is not followed by a redundant heartbeat. Stated once: the two
            // branches used to stamp it separately.
            const emitted = O.toArray(event);
            if (emitted.length > 0) lastSnapshotAt.set(apiId, now);
            return emitted;
          });
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
          if (e.type === SEQUENCED_EVENT) {
            const after = (yield* Ref.get(registry)).breakers.get(e.data.apiId);
            const checkpoint: Checkpoint = {
              state: e.data.state,
              // No cast: the published event's `reason` is the vocabulary
              // itself now, so what decodes off the wire is already a Reason.
              reason: e.data.reason,
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
                    `token ${formatToken(err.attempted)} superseded by ` +
                    O.match(err.current, {
                      onNone: () => "an unreadable one",
                      onSome: formatToken,
                    }),
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
          if (e.type === SEQUENCED_EVENT) {
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
     * loop: stand down, because an instance that cannot confirm it holds the lease
     * must not act as leader, and try again next tick.
     *
     * The shape matters as much as the handling — as a defect rather than a typed
     * failure, an outage would terminate `Effect.repeat` and end the loop for good
     * in a process that stays up and keeps answering 200.
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
        Effect.map((reg) => O.fromUndefinedOr(reg.breakers.get(apiId)?.state)),
      );

    const isLeader = Ref.get(leadership).pipe(Effect.map((l) => l.isLeader));

    /**
     * Hand the lease back on the way out, so a planned stop does not cost the
     * standby `leaseTtlMs` of waiting with nobody publishing. `release` only
     * removes the lease if this instance still holds it, so a demoted instance
     * calling it cannot evict whoever took over.
     *
     * Best effort: if the coordinator is unreachable the lease expires the old
     * way, and failing here would only make a clean shutdown noisy.
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
