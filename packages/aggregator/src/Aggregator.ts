import {
  Array as Arr,
  Clock,
  Context,
  Duration,
  Effect,
  Layer,
  Metric,
  Option as O,
  Ref,
  Result,
  Schedule,
} from "effect";
import * as Breaker from "@egress/domain/Breaker.ts";
import { Config, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import { CheckpointStore, HaSettings, LeaderElection } from "./Coordination.ts";
import { EventBus, EventSink, snapshotEvent, stateChanged } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import { formatToken } from "./Coordination.ts";
import type { Checkpoint, CoordinationUnavailable, LeaseToken } from "./Coordination.ts";
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
>()("@egress/aggregator/Aggregator") {
  /** Suspended: `make` is defined below the service it implements. */
  static readonly layer = Layer.effect(Aggregator, Effect.suspend(() => make));
}

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

const make = Effect.gen(function* () {
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

  /**
   * `demote`, plus fencing the sink — but only when this instance actually
   * held leadership going in. Every path that loses leadership (step-down,
   * a checkpoint fenced by a newer token, a denied acquire, coordination
   * gone unreachable) routes through here rather than calling `demote`
   * directly, so none of them can forget the fence. The guard is what keeps
   * a standby — already demoted, ticking every `tickMs` while it holds off
   * or simply fails to acquire — from resetting the connection on every
   * tick: `sink.resetConnection` runs once, on the transition, not for as
   * long as the instance stays a standby.
   */
  const demoteAndFence = Ref.get(leadership).pipe(
    Effect.map((l) => l.isLeader),
    Effect.tap(() => demote),
    Effect.flatMap((wasLeading) => (wasLeading ? sink.resetConnection : Effect.void)),
  );

  /** Flips only on change, so an outage is two log lines rather than four a second. */
  const coordinationOk = yield* Ref.make(true);

  /** Flips only on change, same reason as coordinationOk above. */
  const sinkReadyOk = yield* Ref.make(true);

  /**
   * When this instance last stepped down because its sink went not-ready,
   * `None` otherwise. Read only to debounce this instance's own next acquire —
   * see READINESS_HOLD_OFF_MS.
   */
  const steppedDownAt = yield* Ref.make<O.Option<number>>(O.none());

  /**
   * Give the lease back and start the readiness hold-off — shared by the
   * not-ready step-down below and by a sequenced event's delivery failing
   * (see `attemptTick`'s reduce): both are "this instance cannot do the one
   * job the lease gives it right now", and both want the standby able to
   * take over on this same tick rather than wait out the lease TTL. Best
   * effort, same reason `releaseOnShutdown`'s is: if the coordinator itself
   * is unreachable, the lease just expires the slower way.
   */
  const releaseAndHoldOff = (now: number) =>
    leader.release(ha.instanceId).pipe(
      Effect.andThen(Ref.set(steppedDownAt, O.some(now))),
      Effect.catch((err) =>
        Effect.logWarning(
          `${ha.instanceId}: could not release the lease while stepping down ` +
          `(${err.operation}) — it will expire instead`,
        ).pipe(Effect.andThen(Ref.set(steppedDownAt, O.some(now)))),
      ),
    );

  /**
   * How long this instance waits after stepping down for a not-ready sink
   * before it may try to acquire the lease again.
   *
   * One lease TTL: the same timescale a crash failover already waits out, so
   * there is nothing new here for an operator to learn. It is long enough
   * that a readiness signal flapping on a sub-second timescale does not turn
   * into a same-timescale fight over the lease — every step-down becomes
   * sticky for at least one TTL rather than reversible the very next tick.
   * When both instances lose the broker together, both hold off equally, so
   * whichever becomes ready and acquires first gets a full TTL to renew
   * before the other's own hold-off even expires, instead of the two of them
   * racing to reacquire on every tick the connection blips.
   */
  const READINESS_HOLD_OFF_MS = ha.leaseTtlMs;

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
      const tickNow = yield* Clock.currentTimeMillis;
      yield* Ref.set(lastTick, tickNow);

      const sinkReady = yield* sink.ready;
      yield* Metric.update(Telemetry.controlPlaneReady, sinkReady ? 1 : 0);

      if (!sinkReady) {
        // A leader that cannot deliver is not doing the one job the lease
        // gives it: give the lease back so the standby — which may still
        // reach the broker — can take over, rather than keep renewing while
        // publishing into nothing.
        const heldBefore = (yield* Ref.get(leadership)).isLeader;
        yield* Effect.when(releaseAndHoldOff(tickNow), Effect.succeed(heldBefore));
        yield* demoteAndFence;
        yield* Metric.update(Telemetry.isLeader, 0);
        yield* Ref.getAndSet(sinkReadyOk, false).pipe(
          Effect.flatMap((was) =>
            was
              ? Effect.logWarning(
                  `${ha.instanceId}: control plane not ready — stepping down and ` +
                  `publishing nothing until it recovers`,
                )
              : Effect.void,
          ),
        );
        return [];
      }
      yield* Ref.getAndSet(sinkReadyOk, true).pipe(
        Effect.flatMap((was) =>
          was ? Effect.void : Effect.logInfo(`${ha.instanceId}: control plane ready again`),
        ),
      );

      // Not ready is a reason to step down; readiness alone is not (yet) a
      // reason to try acquiring — see READINESS_HOLD_OFF_MS.
      const holdingOff = yield* Ref.get(steppedDownAt).pipe(
        Effect.map((since) =>
          O.match(since, {
            onNone: () => false,
            onSome: (at) => tickNow - at < READINESS_HOLD_OFF_MS,
          }),
        ),
      );
      if (holdingOff) {
        // Already demoted by the step-down above (or never leading at all):
        // demoteAndFence's own guard makes this a no-op fence, correctly, on
        // every tick spent waiting out READINESS_HOLD_OFF_MS.
        yield* demoteAndFence;
        yield* Metric.update(Telemetry.isLeader, 0);
        return [];
      }
      yield* Ref.set(steppedDownAt, O.none());

      const tokenOpt = yield* leader.tryAcquireOrRenew(ha.instanceId, ha.leaseTtlMs);
      yield* Ref.getAndSet(coordinationOk, true).pipe(
        Effect.flatMap((wasOk) =>
          wasOk
            ? Effect.void
            : Effect.logInfo(`${ha.instanceId}: coordination is reachable again`),
        ),
      );
      yield* Metric.update(Telemetry.isLeader, O.isSome(tokenOpt) ? 1 : 0);

      if (O.isNone(tokenOpt)) {
        // Standby: do not poll, do not step, do not publish. The only
        // thing a non-leader instance does is keep trying to acquire. If
        // this instance held the lease a moment ago (denied a renew — someone
        // else's acquire won the race), demoteAndFence's guard still catches
        // it as a genuine loss of leadership.
        yield* demoteAndFence;
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
      // The condition is "no breaker yet", and nothing more — but the set of
      // APIs it applies to is `fleet.specs`, the full configured list, not
      // `reports.map(apiId)`. It used to be the latter, which quietly meant
      // "only APIs this tick's poll happened to mention". A polling source
      // answers immediately, so this held in the simulator and in every
      // test; it used to fail one level up for the same reason a narrower
      // version of this guard used to fail (see the previous fix, still
      // below): a leader whose poll reports nothing *at all* — a telemetry
      // blackout, not just a slow first push — computed an empty
      // `toRehydrate` on every tick for as long as the blackout lasted, so a
      // leadership change during one left the new leader's registry (and
      // `/api/state`, and every gauge) with zero entries for every
      // configured API, not just the ones affected by whatever the blackout
      // was hiding. Reproduced live: block Envoy's push port to both
      // aggregators, kill the leader mid-blackout, and the standby takes
      // over holding nothing — not even the two unrelated APIs that were
      // sitting at CLOSED a moment before — until the firewall rule lifts.
      //
      // Keyed off `specs` instead, rehydration depends only on holding the
      // lease, matching what the guard already claimed to do. An API with
      // no live report this tick still advances on elapsed time alone via
      // `Breaker.step` below, exactly like any other registry entry a quiet
      // tick leaves untouched — seeding it here costs nothing extra.
      //
      // Dropping the original guard costs nothing: `known.has(id)` is true
      // from the moment an API is seeded, so this is one load per API per
      // instance, not one per tick. An API first seen long after acquisition
      // now resumes too, which is the same intent applied honestly.
      const known = yield* Ref.get(registry).pipe(Effect.map((reg) => reg.breakers));
      const specs = yield* fleet.specs;
      const toRehydrate = specs.map((s) => s.apiId).filter((id) => !known.has(id));
      const rehydrated = yield* Effect.forEach(toRehydrate, (apiId) =>
        checkpoints.load(apiId).pipe(Effect.map((cp) => [apiId, cp] as const)),
      );
      const checkpointByApi = new Map(rehydrated);

      const events = yield* Ref.modify(registry, (reg) => {
        // Seed every newly-rehydrated API before folding in this tick's
        // reports, so one exists to step even when no report arrived for it
        // this tick — the case `toRehydrate` above exists to cover.
        const seeded = Arr.reduce(toRehydrate, new Map(reg.breakers), (acc, apiId) =>
          acc.set(
            apiId,
            seedFromCheckpoint(apiId, checkpointByApi.get(apiId) ?? O.none(), cfg, now),
          ),
        );
        // Several replicas report the same API, so this is a left fold and
        // not a map: each report folds into the breaker the last one left.
        const ingested = Arr.reduce(reports, seeded, (acc, report) =>
          acc.set(
            report.apiId,
            Breaker.ingest(
              acc.get(report.apiId) ??
                seedFromCheckpoint(
                  report.apiId,
                  checkpointByApi.get(report.apiId) ?? O.none(),
                  cfg,
                  now,
                ),
              report,
            ),
          ),
        );

        const stepped = Arr.map([...ingested], ([apiId, before]) => {
          const [after, change] = Breaker.step(before, now, cfg);
          return {
            apiId,
            after,
            // A transition wins over the heartbeat snapshot: both carry the
            // same state, and only the transition carries where it came from.
            event: O.orElse(
              O.map(change, (transition) =>
                stateChanged(Breaker.snapshot(after), transition.from, now),
              ),
              () =>
                now - (reg.lastSnapshotAt.get(apiId) ?? 0) >= cfg.snapshotMs
                  ? O.some(snapshotEvent(Breaker.snapshot(after), now))
                  : O.none(),
            ),
          };
        });

        return [
          // Stamped with this tick's lease, so a reader can rank this leader
          // against a paused predecessor or a successor — see `supersedes`.
          Arr.flatMap(stepped, ({ event }) =>
            O.toArray(O.map(event, (e) => ({ ...e, data: { ...e.data, lease: token } }))),
          ) as ReadonlyArray<CircuitEvent>,
          {
            breakers: new Map(Arr.map(stepped, ({ apiId, after }) => [apiId, after] as const)),
            // Anything published restarts that API's snapshot clock, so a
            // transition is not followed by a redundant heartbeat.
            lastSnapshotAt: Arr.reduce(
              stepped,
              new Map(reg.lastSnapshotAt),
              (acc, { apiId, event }) =>
                O.match(event, { onNone: () => acc, onSome: () => acc.set(apiId, now) }),
            ),
          },
        ];
      });

      // Deliver every transition before checkpointing it, and checkpoint
      // under this tick's token before telling anyone else about it.
      //
      // Delivery first is what closes the gap this used to have: this sink's
      // AMQP leg now awaits the broker's confirm (AmqpControlPlaneSink.ts's
      // `deliver`) rather than forking and forgetting, so a checkpoint here
      // only ever advances past a sequence the broker actually took. A
      // rejected checkpoint still means the same thing it always did — a
      // newer instance has taken the lease — and a failed delivery means
      // this instance can no longer do the one job the lease gives it.
      // Either way: stop publishing immediately rather than let a demoted or
      // undeliverable instance keep talking; the remaining sequenced events
      // this tick are dropped, not queued or reordered, since the next
      // leader (this instance re-acquiring, or another) re-derives them from
      // its own next poll against the checkpoint actually saved.
      //
      // Not gapless in one narrow window: a leader that crashes after the
      // broker confirms but before the checkpoint save leaves its successor
      // to re-publish the same sequence, possibly with a different state.
      // The successor's lease outranks the crashed leader's, so daemons act
      // on its event (Model.ts's `supersedes`). The fence here is on the
      // checkpoint, not on the broker: a paused leader that resumes can still
      // publish once before its save is refused, and its older lease is what
      // makes daemons ignore that event.
      type Publishing = {
        readonly publishable: ReadonlyArray<CircuitEvent>;
        readonly stopped: boolean;
      };
      const { publishable } = yield* Effect.reduce(
        events,
        (): Publishing => ({ publishable: [], stopped: false }),
        (acc, e): Effect.Effect<Publishing, CoordinationUnavailable> =>
          // Once stopped, the remaining events are dropped rather than
          // queued: the new leader re-derives them from its own next poll.
          acc.stopped
            ? Effect.succeed(acc)
            : e.type !== SEQUENCED_EVENT
              ? Effect.succeed({ ...acc, publishable: Arr.append(acc.publishable, e) })
              : Effect.gen(function* () {
                  const delivery = yield* Effect.result(sink.deliver(e));
                  return yield* Result.match(delivery, {
                    onFailure: (err) =>
                      // Same demotion as losing the lease outright or being
                      // fenced below — an undelivered sequence must not be
                      // checkpointed, and this instance's in-memory registry
                      // has already moved past it, so it must not keep
                      // publishing on top rather than stand down and let a
                      // (possibly still-reachable) standby take over, same
                      // as the not-ready step-down above and for the same
                      // reason: give the lease back now rather than have the
                      // standby wait out the TTL.
                      Effect.as(
                        Effect.all(
                          [
                            Effect.logWarning(
                              `${ha.instanceId}: control plane did not confirm ` +
                              `${err.apiId} sequence ${e.data.sequence} (${err.cause}) — ` +
                              `stepping down rather than checkpoint an undelivered sequence`,
                            ),
                            releaseAndHoldOff(tickNow),
                            demoteAndFence,
                            Metric.update(Telemetry.isLeader, 0),
                          ],
                          { discard: true },
                        ),
                        { ...acc, stopped: true },
                      ),
                    onSuccess: () =>
                      Effect.gen(function* () {
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
                        const fenced = yield* checkpoints
                          .save(e.data.apiId, token, checkpoint)
                          .pipe(
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
                        // Same demotion as losing the lease outright, registry drop,
                        // reset connection and all — being fenced *is* how this
                        // instance finds out someone else has already moved the
                        // sequence on, and it was leading up to this exact instant,
                        // so demoteAndFence's guard always resets here.
                        return yield* fenced
                          ? Effect.as(
                              Effect.all(
                                [
                                  demoteAndFence,
                                  Metric.update(Telemetry.isLeader, 0),
                                  Metric.update(
                                    Metric.withAttributes(Telemetry.fencingConflicts, {
                                      apiId: e.data.apiId,
                                    }),
                                    1,
                                  ),
                                ],
                                { discard: true },
                              ),
                              { ...acc, stopped: true },
                            )
                          : Effect.succeed({
                              ...acc,
                              publishable: Arr.append(acc.publishable, e),
                            });
                      }),
                  });
                }),
      );

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

      yield* Effect.forEach(
        publishable,
        (e) =>
          Metric.update(
            e.type === SEQUENCED_EVENT
              ? Metric.withAttributes(Telemetry.circuitTransitions, {
                  apiId: e.data.apiId,
                  reason: e.data.reason,
                  state: e.data.state,
                })
              : Metric.withAttributes(Telemetry.circuitSnapshots, { apiId: e.data.apiId }),
            1,
          ),
        { discard: true },
      );

      // Publish to the in-process bus first (the console), then hand
      // snapshots to the sink. Sequenced events already went to the sink
      // above, awaited, before their checkpoint was allowed to advance —
      // only what is left, the fire-and-forget snapshot heartbeats, is
      // handed off here, forked so a slow confirm cannot stall the loop the
      // way an awaited one deliberately can for a sequenced event.
      yield* Effect.forEach(publishable, (e) => bus.publish(e), {
        discard: true,
      });
      yield* Effect.forEach(
        publishable.filter((e) => e.type !== SEQUENCED_EVENT),
        (e) => Effect.forkChild(sink.deliver(e)),
        { discard: true },
      );

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
        // Coordination going unreachable does not by itself mean another
        // instance has taken over — but this instance can no longer confirm
        // it still holds the lease, so it is exactly as unauthoritative as
        // any other demotion, and demoteAndFence's guard treats it the same.
        yield* demoteAndFence;
        yield* Metric.update(Telemetry.coordinationErrors, 1);
        // Say "not the leader" rather than saying nothing. `isLeader` is
        // only updated after an acquire attempt returns, so an instance that
        // has never reached the coordinator publishes no series at all —
        // and an alert written as `max(egress_aggregator_is_leader) == 0`
        // cannot fire on a metric that is absent. Found by partitioning one
        // instance from Redis and watching the gauge vanish instead of drop.
        yield* Metric.update(Telemetry.isLeader, 0);
        yield* Ref.getAndSet(coordinationOk, false).pipe(
          Effect.flatMap((wasOk) =>
            wasOk
              ? Effect.logWarning(
                  `${ha.instanceId}: coordination unavailable during ${err.operation}, ` +
                  `standing down until it returns — ${err.cause}`,
                )
              : Effect.void,
          ),
        );
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
  const releaseOnShutdown = Effect.when(
    leader.release(ha.instanceId).pipe(
      Effect.flatMap(() =>
        Effect.logInfo(`${ha.instanceId}: lease released on shutdown`),
      ),
      Effect.catch((err) =>
        Effect.logWarning(
          `${ha.instanceId}: could not release the lease on shutdown ` +
          `(${err.operation}) — it will expire instead`,
        ),
      ),
    ),
    Ref.get(leadership).pipe(Effect.map((held) => held.isLeader)),
  ).pipe(Effect.asVoid);

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

  return Aggregator.of({
    tick,
    snapshots,
    stateOf,
    isLeader,
    run,
    lastTickAt: Ref.get(lastTick),
    tickMs: cfg.tickMs,
  });
});
