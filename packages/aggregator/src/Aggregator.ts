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
import type { DeliveryFailed } from "@egress/domain/Model.ts";
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
    /** The tick loop, until interrupted; releases the lease on the way out so a planned stop hands over at once. */
    readonly run: Effect.Effect<void>;
    /** Liveness, not leadership: a standby ticks on the same schedule. 0 before the first pass. */
    readonly lastTickAt: Effect.Effect<number>;
    /** The loop's interval, so a health check can say what "stalled" means in its own terms. */
    readonly tickMs: number;
  }
>()("@egress/aggregator/Aggregator") {
  /** Suspended: `make` is defined below the service it implements. */
  static readonly layer = Layer.effect(Aggregator, Effect.suspend(() => make));
}

/**
 * A breaker seeded from its checkpoint, when there is one. Two lines tsc cannot
 * check, pinned by Aggregator.test.ts: `getOrElse` must leave `Option` before the
 * spread (spreading an `Option` contributes only `value`, resuming at sequence 0),
 * and `Partial<BreakerState>` must type the overlay (a misspelt field spreads
 * harmlessly).
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
   * Dropping the breakers keeps the sequence across a demotion: a warm registry
   * would let a re-promoted instance resume from its own stale sequence.
   */
  const demote = Effect.all(
    [
      Ref.set(leadership, { isLeader: false, token: O.none() }),
      Ref.set(registry, { breakers: new Map(), lastSnapshotAt: new Map() }),
    ],
    { discard: true },
  );

  /**
   * Every way of losing leadership goes through here, so none forgets the fence.
   * Fences only if this instance was leading, not on every standby tick.
   */
  const demoteAndFence = Ref.get(leadership).pipe(
    Effect.map((l) => l.isLeader),
    Effect.tap(() => demote),
    Effect.flatMap((wasLeading) => (wasLeading ? sink.resetConnection : Effect.void)),
  );

  /** Edge-triggered logging: an outage is two lines, not four a second. */
  const coordinationOk = yield* Ref.make(true);

  const sinkReadyOk = yield* Ref.make(true);

  const steppedDownAt = yield* Ref.make<O.Option<number>>(O.none());

  /** Give the lease back so the standby takes over this tick; best effort, else it expires. */
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
   * After stepping down for a not-ready sink, wait one lease TTL before acquiring
   * again, so a flapping readiness signal (or both instances losing the broker)
   * does not become a fight over the lease every tick.
   */
  const READINESS_HOLD_OFF_MS = ha.leaseTtlMs;

  /**
   * Transitions awaited at once in one tick. Bounded so a thousand APIs moving
   * together do not open a thousand publishes on one confirm channel.
   */
  const DELIVERY_CONCURRENCY = 32;

  const lastTick = yield* Ref.make(0);

  const attemptTick = Effect.gen(
    function* () {
      yield* Metric.update(Telemetry.ticks, 1);
      const tickNow = yield* Clock.currentTimeMillis;
      yield* Ref.set(lastTick, tickNow);

      const sinkReady = yield* sink.ready;
      yield* Metric.update(Telemetry.controlPlaneReady, sinkReady ? 1 : 0);

      if (!sinkReady) {
        // A leader that cannot deliver hands the lease to a standby that may.
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

      const holdingOff = yield* Ref.get(steppedDownAt).pipe(
        Effect.map((since) =>
          O.match(since, {
            onNone: () => false,
            onSome: (at) => tickNow - at < READINESS_HOLD_OFF_MS,
          }),
        ),
      );
      if (holdingOff) {
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
        // Standby: only tries to acquire.
        yield* demoteAndFence;
        return [];
      }
      const token = tokenOpt.value;
      yield* Ref.set(leadership, { isLeader: true, token: O.some(token) });

      const [pollDuration, reports] = yield* Effect.timed(fleet.poll);
      yield* Metric.update(Telemetry.fleetPollDuration, pollDuration);
      const now = yield* Clock.currentTimeMillis;

      // Rehydrate every configured API with no breaker yet — keyed on `specs`, not
      // this tick's reports: during a telemetry blackout the poll reports nothing,
      // and a new leader was left holding no APIs at all.
      const known = yield* Ref.get(registry).pipe(Effect.map((reg) => reg.breakers));
      const specs = yield* fleet.specs;
      const toRehydrate = specs.map((s) => s.apiId).filter((id) => !known.has(id));
      const rehydrated = yield* Effect.forEach(toRehydrate, (apiId) =>
        checkpoints.load(apiId).pipe(Effect.map((cp) => [apiId, cp] as const)),
      );
      const checkpointByApi = new Map(rehydrated);

      const events = yield* Ref.modify(registry, (reg) => {
        const seeded = Arr.reduce(toRehydrate, new Map(reg.breakers), (acc, apiId) =>
          acc.set(
            apiId,
            seedFromCheckpoint(apiId, checkpointByApi.get(apiId) ?? O.none(), cfg, now),
          ),
        );
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
            // A transition wins over the periodic snapshot.
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
          // Stamped with the lease, so readers can rank leaders (`supersedes`).
          Arr.flatMap(stepped, ({ event }) =>
            O.toArray(O.map(event, (e) => ({ ...e, data: { ...e.data, lease: token } }))),
          ) as ReadonlyArray<CircuitEvent>,
          {
            breakers: new Map(Arr.map(stepped, ({ apiId, after }) => [apiId, after] as const)),
            lastSnapshotAt: Arr.reduce(
              stepped,
              new Map(reg.lastSnapshotAt),
              (acc, { apiId, event }) =>
                O.match(event, { onNone: () => acc, onSome: () => acc.set(apiId, now) }),
            ),
          },
        ];
      });

      // Deliver the transitions (awaiting the broker's confirms) and checkpoint what
      // was confirmed under this tick's token; a failed delivery or a fenced
      // checkpoint stops publishing, and what was not checkpointed is re-derived by
      // the next leader. The first goes alone: its checkpoint proves the lease still
      // holds, so a leader deposed mid-tick publishes one stale transition, as
      // before, not one per API. The rest — at most one per API, so independent —
      // are awaited together: a burst across many APIs costs one confirm's latency,
      // not one per API, and none is started once one has failed. One window is not
      // gapless: a crash between confirm and save lets the successor reuse the
      // sequence, and its newer lease is what readers believe
      // (docs/high-availability.md).
      const checkpointOne = (e: CircuitEvent) =>
        Effect.gen(function* () {
          const after = (yield* Ref.get(registry)).breakers.get(e.data.apiId);
          const checkpoint: Checkpoint = {
            state: e.data.state,
            reason: e.data.reason,
            sequence: e.data.sequence,
            changedAt: now,
            openBackoffMs: after?.openBackoffMs ?? cfg.openMs,
          };
          return yield* checkpoints.save(e.data.apiId, token, checkpoint).pipe(
            Effect.as(false),
            Effect.catchTag("CheckpointFenced", (err) =>
              Effect.as(
                Effect.all(
                  [
                    Effect.logWarning(
                      `lost leadership publishing ${e.data.apiId}: ` +
                      `token ${formatToken(err.attempted)} superseded by ` +
                      O.match(err.current, {
                        onNone: () => "an unreadable one",
                        onSome: formatToken,
                      }),
                    ),
                    Metric.update(Metric.withAttributes(Telemetry.fencingConflicts, { apiId: e.data.apiId }), 1),
                  ],
                  { discard: true },
                ),
                true,
              ),
            ),
          );
        });

      /** Checkpoints in order, stopping at the first fence. */
      type Checkpointing = {
        readonly checkpointed: ReadonlyArray<CircuitEvent>;
        readonly fenced: boolean;
      };
      const checkpointAll = (confirmed: ReadonlyArray<CircuitEvent>, from: Checkpointing) =>
        Effect.reduce(
          confirmed,
          (): Checkpointing => from,
          (acc, e): Effect.Effect<Checkpointing, CoordinationUnavailable> =>
            acc.fenced
              ? Effect.succeed(acc)
              : Effect.map(checkpointOne(e), (isFenced) =>
                  isFenced ? { ...acc, fenced: true } : { ...acc, checkpointed: Arr.append(acc.checkpointed, e) },
                ),
        );

      type Failure = { readonly e: CircuitEvent; readonly err: DeliveryFailed };
      const sequenced = events.filter((e) => e.type === SEQUENCED_EVENT);
      const [head, ...rest] = sequenced;

      const first = head === undefined
        ? { progress: { checkpointed: [], fenced: false } as Checkpointing, failures: [] as ReadonlyArray<Failure> }
        : yield* Effect.result(sink.deliver(head)).pipe(
            Effect.flatMap((outcome) =>
              Result.isFailure(outcome)
                ? Effect.succeed({
                    progress: { checkpointed: [], fenced: false } as Checkpointing,
                    failures: [{ e: head, err: outcome.failure }] as ReadonlyArray<Failure>,
                  })
                : Effect.map(checkpointAll([head], { checkpointed: [], fenced: false }), (progress) => ({
                    progress,
                    failures: [] as ReadonlyArray<Failure>,
                  })),
            ),
          );

      const { checkpointed, fenced, failures } = first.progress.fenced || first.failures.length > 0
        ? { ...first.progress, failures: first.failures }
        : yield* Effect.gen(function* () {
            const failedOnce = yield* Ref.make(false);
            const delivered = yield* Effect.forEach(
              rest,
              (e) =>
                Effect.flatMap(Ref.get(failedOnce), (failed) =>
                  // Skipped, not attempted: the leader steps down and its successor re-derives it.
                  failed
                    ? Effect.succeed(O.none<{ e: CircuitEvent; outcome: Result.Result<void, DeliveryFailed> }>())
                    : Effect.result(sink.deliver(e)).pipe(
                        Effect.tap((outcome) => (Result.isFailure(outcome) ? Ref.set(failedOnce, true) : Effect.void)),
                        Effect.map((outcome) => O.some({ e, outcome })),
                      ),
                ),
              { concurrency: DELIVERY_CONCURRENCY },
            );
            const attempted = delivered.flatMap(O.toArray);
            const confirmed = attempted.flatMap(({ e, outcome }) => (Result.isSuccess(outcome) ? [e] : []));
            const failures: ReadonlyArray<Failure> = attempted.flatMap(({ e, outcome }) =>
              Result.isFailure(outcome) ? [{ e, err: outcome.failure }] : [],
            );
            const progress = yield* checkpointAll(confirmed, first.progress);
            return { ...progress, failures };
          });

      const stopped = fenced || failures.length > 0;
      yield* fenced
        ? Effect.all([demoteAndFence, Metric.update(Telemetry.isLeader, 0)], { discard: true })
        : failures.length > 0
          ? Effect.all(
              [
                Effect.forEach(
                  failures,
                  ({ e, err }) =>
                    Effect.logWarning(
                      `${ha.instanceId}: control plane did not confirm ` +
                      `${err.apiId} sequence ${e.data.sequence} (${err.cause}) — ` +
                      `stepping down rather than checkpoint an undelivered sequence`,
                    ),
                  { discard: true },
                ),
                releaseAndHoldOff(tickNow),
                demoteAndFence,
                Metric.update(Telemetry.isLeader, 0),
              ],
              { discard: true },
            )
          : Effect.void;

      // A leader that stopped publishes no snapshots either: it has stepped down.
      const publishable: ReadonlyArray<CircuitEvent> = stopped
        ? checkpointed
        : [...checkpointed, ...events.filter((e) => e.type !== SEQUENCED_EVENT)];

      // Every tick, so a dashboard mid-dwell does not look frozen.
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

      // Sequenced events were delivered above; snapshots are forked, unawaited.
      yield* Effect.forEach(publishable, (e) => bus.publish(e), {
        discard: true,
      });
      yield* Effect.forEach(
        publishable.filter((e) => e.type !== SEQUENCED_EVENT),
        (e) => Effect.forkChild(sink.deliver(e)),
        { discard: true },
      );

      // Only the leader drains the outbox, or every entry is delivered twice.
      yield* Effect.forkChild(sink.drainOutbox);
      return publishable;
    },
  );

  /**
   * A tick that cannot reach the coordinator is skipped, and the instance stands
   * down. It must be a typed failure: a defect would end `Effect.repeat`.
   */
  const tick: Effect.Effect<ReadonlyArray<CircuitEvent>> = attemptTick.pipe(
    Effect.catchTag("CoordinationUnavailable", (err) =>
      Effect.gen(function* () {
        yield* demoteAndFence;
        yield* Metric.update(Telemetry.coordinationErrors, 1);
        // Set explicitly: an instance that never reached Redis had no series, and
        // `max(is_leader) == 0` cannot fire on an absent metric.
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

  /** Best effort; `release` only removes a lease this instance still holds. */
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

  // A Schedule, so TestClock can drive it; `ensuring` ties the lease to the loop.
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
