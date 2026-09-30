import {
  Clock,
  Duration,
  Effect,
  Match,
  Metric,
  Option as O,
  Ref,
  Result,
  Schedule,
  Semaphore,
} from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  CONTROL_EXCHANGE_OPTIONS,
  controlQueueFor,
  controlQueueOptions,
  deadLetterQueueFor,
  deadLetterQueueOptions,
  floorQueueFor,
  floorQueueOptions,
  decodeElectionTrigger,
  encodeElectionTrigger,
  parkedQueueFor,
  parkedQueueOptions,
  probeTriggerQueueFor,
  redriveTriggerQueueFor,
  routingKeyFor,
  sacQueueOptions,
  workQueueFor,
  WORK_CONTENT_TYPE,
  WORK_MESSAGE_TYPE,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import { decodeCircuitEvent, State, STATE_CODE } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";
import { initialContract, observe } from "./Contract.ts";
import { makeApplier, persist } from "./ControlEvents.ts";
import { makeWorkCalls } from "./WorkCalls.ts";
import { makeRedrive } from "./Redrive.ts";
import { desired, initialState, isCurrent, isFloor, plan, reduce, SILENCE_MS } from "./DaemonState.ts";
import { position } from "./DaemonPolicy.ts";
import * as Limiter from "./Limiter.ts";
import * as Tally from "./Tally.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Consumer, Settlement } from "@egress/rmq/Client.ts";

import type { ContractState } from "./Contract.ts";
import type { Action, Command, DaemonState } from "./DaemonState.ts";

/**
 * One competing-consumer daemon. It knows its own identity and nothing about the
 * rest of the fleet; it learns the circuit from `circuit.control` and decides from
 * its own hash position whether to work (ADR 013). Backpressure is settlement
 * timing: a delivery settles when its call resolves, and prefetch bounds how many
 * are held (ADR 011). Retries travel in headers (ADR 016).
 */

type DaemonConfig = {
  readonly apiId: string;
  /** Its whole identity: names its control queue and fixes its hash position. Must be per replica. */
  readonly instanceId: string;
  /** The single egress address, exactly as a real client would be given it. */
  readonly egressAddr: string;
  /** The route on that address for this API — /payments for payments-provider. */
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
  /** Replay `<apiId>.work.dead` onto the work queue when the circuit closes. */
  readonly redriveOnClose: boolean;
  /** Messages moved per redrive pass, so a large backlog is recovered in bounded bites. */
  readonly redriveMax: number;
  /** Learn the concurrent-call limit from 429s (Limiter.ts). `None`: it stays at `maxInFlight`. */
  readonly limit: O.Option<Limiter.LimiterConfig>;
};

/** How often the daemon publishes counters, advances its ramp, and says it is alive. */
const FLUSH_INTERVAL = Duration.seconds(1);
/** The clock that advances the ramp, notices silence and lapses the floor lease. */
const TICK_INTERVAL = Duration.seconds(1);
const HEARTBEAT_INTERVAL = Duration.seconds(15);
/** The floor's sweep of the dead-letter queue, for dead letters no transition will redrive. */
const SWEEP_INTERVAL = Duration.seconds(30);

/** Before a failed trigger is requeued, so a failure that persists doesn't spin on redelivery. */
const TRIGGER_RETRY_HOLD = Duration.seconds(1);

/** What a retry declares about its body, the same as the producer's publish. */
const WORK_FORMAT = { contentType: WORK_CONTENT_TYPE, type: WORK_MESSAGE_TYPE };

export const runDaemon = Effect.fnUntraced(function* (cfg: DaemonConfig) {
  const control = yield* Rmq;
  const label = `${cfg.apiId}/${cfg.instanceId}`;
  // A label: Prometheus's `instance` is only an IP for a DNS-discovered fleet.
  const attrs = { apiId: cfg.apiId, daemon: cfg.instanceId };

  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);
  const parkedQueue = parkedQueueFor(cfg.apiId);
  const probeQueue = probeTriggerQueueFor(cfg.apiId);
  const redriveQueue = redriveTriggerQueueFor(cfg.apiId);
  const controlQueue = controlQueueFor(cfg.apiId, cfg.instanceId);

  const exchange = yield* control.declareTopicExchange(
    CONTROL_EXCHANGE,
    CONTROL_EXCHANGE_OPTIONS,
  );
  // Before the queues that point at it, so an early rejection has somewhere to land.
  yield* control.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* control.declareQueue(parkedQueue, parkedQueueOptions());
  yield* control.declareQueue(workQueue, workQueueOptions(cfg.apiId));
  yield* control.declareQueue(probeQueue, sacQueueOptions(cfg.apiId));
  yield* control.declareQueue(redriveQueue, sacQueueOptions(cfg.apiId));
  const controlQ = yield* control.declareQueue(controlQueue, controlQueueOptions(cfg.apiId));
  yield* control.bind(routingKeyFor(cfg.apiId), exchange, controlQ);

  /** The floor (ADR 013): its queue is bound to `circuit.control`, so every event re-elects one daemon. */
  const floorQ = yield* control.declareQueue(floorQueueFor(cfg.apiId), floorQueueOptions());
  yield* control.bind(routingKeyFor(cfg.apiId), exchange, floorQ);

  /**
   * Everything decided, in one value (DaemonState.ts): the circuit, how much it
   * is worth, the ramp, the floor lease, the elections' marks. Written only
   * through `dispatch`.
   */
  const startedAt = yield* Clock.currentTimeMillis;
  const state = yield* Ref.make<DaemonState>(initialState(startedAt));

  /** The churning channels — the "actual" side `plan` compares the desired shape against. */
  const workConsumer = yield* Ref.make(O.none<Consumer>());
  const probeConsumer = yield* Ref.make(O.none<Consumer>());
  const redriveConsumer = yield* Ref.make(O.none<Consumer>());
  /** Claimed atomically so two redrive passes never both start. */
  const redriveRunning = yield* Ref.make(false);

  const counts = Tally.zero();
  const contract = yield* Ref.make<ContractState>(initialContract);

  const selfPosition = position(cfg.instanceId);
  const selfIn = (s: DaemonState, now: number) => ({ position: selfPosition, isFloor: isFloor(s, now) });

  const workPublisher = yield* control.publisherToQueue(workQueue, WORK_FORMAT);
  const deadPublisher = yield* control.publisherToQueue(deadQueue);
  const parkedPublisher = yield* control.publisherToQueue(parkedQueue);

  /**
   * Every AMQP callback runs its effects through these: a bare `Effect.run*` builds
   * a runtime with the no-op tracer and the default logger.
   */
  const services = yield* Effect.context<never>();
  const runInContext = Effect.runPromiseWith(services);
  const forkInContext = Effect.runForkWith(services);

  const work = yield* makeWorkCalls({
    cfg,
    rmq: control,
    counts,
    attrs,
    queues: { work: workQueue, dead: deadQueue },
    publishers: { work: workPublisher, dead: deadPublisher, parked: parkedPublisher },
    services,
  });

  /**
   * Control events fan out to every daemon, so a version skew would flood the
   * dead-letter queue. The first few are kept; the metric counts all.
   */
  const UNDECODABLE_SAMPLE = 20;

  const sampleUnreadable = (what: string): Settlement => {
    const seen = ++counts.undecodable;
    forkInContext(
      seen <= UNDECODABLE_SAMPLE
        ? Effect.logWarning(`${label}: ${what}, dead-lettered`)
        : seen === UNDECODABLE_SAMPLE + 1
          ? Effect.logWarning(
            `${label}: ${UNDECODABLE_SAMPLE} unreadable messages already preserved on ` +
              `${deadQueue} — accepting further ones rather than flooding it; ` +
              `egress_daemon_undecodable_total still counts them all`,
          )
          : Effect.void,
    );
    return seen <= UNDECODABLE_SAMPLE ? "discard" : "accept";
  };

  /**
   * The one serialization point. Whatever reconciles — a control event, the clock,
   * a floor election — holds it from its transition to the end of its reconcile.
   * The probe opens its channel under it; a redrive pass opens and closes its own
   * outside it and only records the channel under it, so a reconcile in between
   * misses it for at most a tick (the pass also stops on `isClosed`). Long work
   * runs outside it: a redrive's passes, a trigger's publish, a probe's call.
   */
  const transitions = yield* Semaphore.make(1);

  /** One command in, one atomic transition out. */
  const dispatch = (command: Command) =>
    Ref.modify(state, (prior) => {
      const transition = reduce(prior, command, cfg.redriveOnClose);
      return [{ prior, ...transition }, transition.next] as const;
    });

  const startWork = Effect.gen(function* () {
    const consumer = yield* control.consume(
      workQueue,
      (body, delivery) => work.callEgress(body, delivery),
      { prefetch: cfg.maxInFlight },
    );
    yield* Ref.set(workConsumer, O.some(consumer));
  });

  /** Retire a consumer and its channel; anything held unacked returns to the queue. */
  const retire = (ref: Ref.Ref<O.Option<Consumer>>) =>
    Ref.getAndSet(ref, O.none<Consumer>()).pipe(
      Effect.flatMap(
        O.match({ onNone: () => Effect.void, onSome: control.closeConsumer }),
      ),
    );

  /**
   * Make the channels match the state. `desired` and `plan` decide; this only acts.
   * Called with `transitions` held; cheap when nothing is to change.
   */
  const reconcile = Effect.gen(function* () {
    const have = {
      work: O.isSome(yield* Ref.get(workConsumer)),
      probe: O.isSome(yield* Ref.get(probeConsumer)),
      redrive: O.isSome(yield* Ref.get(redriveConsumer)),
    };
    const now = yield* Clock.currentTimeMillis;
    const current = yield* Ref.get(state);
    const actions = plan(desired(current, selfIn(current, now)), have);
    yield* Effect.all(
      [
        actions.startWork ? startWork : Effect.void,
        actions.stopWork ? retire(workConsumer) : Effect.void,
        actions.stopProbe ? retire(probeConsumer) : Effect.void,
        actions.stopRedrive ? retire(redriveConsumer) : Effect.void,
      ],
      { discard: true },
    );
  });

  /** One message, one call, on a channel that exists only for this probe. */
  const probeOnce = transitions.withPermit(
    Effect.flatMap(
      Ref.get(probeConsumer),
      O.match({
        // Already probing: one message per election.
        onSome: () => Effect.void,
        onNone: () =>
          Effect.gen(function* () {
            // Cancel, not close: the channel must outlive the cancel to settle the message.
            let self: Consumer | null = null;
            let taken = false;
            const consumer = yield* control.consume(
              workQueue,
              (body, delivery): Settlement | Promise<Settlement> =>
                O.match(O.filter(O.fromNullOr(self), () => !taken), {
                  // Released, never acked: returning nothing acks, and extra deliveries were lost that way.
                  onNone: (): Settlement => "release",
                  onSome: (mine) => {
                    taken = true;
                    forkInContext(control.cancelConsumer(mine));
                    return work.callEgress(body, delivery);
                  },
                }),
              { prefetch: 1 },
            );
            self = consumer;

            counts.probed++;
            yield* Ref.set(probeConsumer, O.some(consumer));
            yield* Effect.log(`${label}: elected prober, taking one message`);
          }),
      }),
    ),
  );

  const redriveOnce = makeRedrive({
    label,
    enabled: cfg.redriveOnClose,
    rmq: control,
    workQueue,
    deadQueue,
    parkedQueue,
    maxPerPass: cfg.redriveMax,
    // Only a CLOSED the control plane said, and recently: a pass stops once it is not.
    isClosed: Ref.get(state).pipe(Effect.map((s) => s.control === "heard" && s.circuit === State.CLOSED)),
    consumer: redriveConsumer,
    gate: transitions,
    running: redriveRunning,
  });

  // Published by every daemon, so a trigger still arrives when some are down.
  const trigger = yield* control.publisherToQueue(probeQueue);
  const redriveTrigger = yield* control.publisherToQueue(redriveQueue);

  const perform = Match.typeTags<Action>()({
    PublishProbeTrigger: ({ sequence }) =>
      control.send(trigger, encodeElectionTrigger({ sequence })),
    PublishRedriveTrigger: ({ sequence }) =>
      control.send(redriveTrigger, encodeElectionTrigger({ sequence })),
    Probe: () => probeOnce,
    Redrive: () => redriveOnce,
  });

  const performAll = (actions: ReadonlyArray<Action>) =>
    Effect.forEach(actions, perform, { discard: true });

  const describe = Effect.gen(function* () {
    const current = yield* Ref.get(state);
    const { gaps, duplicates } = yield* Ref.get(contract);
    const floor = isFloor(current, yield* Clock.currentTimeMillis);
    const active = O.isSome(yield* Ref.get(workConsumer));
    return (
      `${current.control === "heard" ? current.circuit : current.control.toUpperCase()} ` +
      `target=${Math.round(current.policy.fraction * 100)}%${current.policy.floor ? "+floor" : ""} ` +
      `self=${active ? "ACTIVE" : "idle"}${floor ? " (floor)" : ""} ` +
      `calls ok=${counts.ok} failed=${counts.failed} inFlight=${work.inFlight()} ` +
      `control=${[...counts.byType.values()].reduce((a, b) => a + b, 0)} ` +
      `gaps=${gaps} dup=${duplicates}`
    );
  });

  /** Order matters: apply, reconcile, log, and only then publish the triggers. */
  const applyInOrder = makeApplier(transitions, {
    dispatch: (event: CircuitEvent) =>
      Effect.flatMap(Clock.currentTimeMillis, (at) =>
        dispatch({
          _tag: "CircuitChanged",
          type: event.type,
          lease: O.fromUndefinedOr(event.data.lease),
          state: event.data.state,
          sequence: event.data.sequence,
          at,
        }).pipe(
          // The one way out of `unheard` or `silent` is an applied event, so it is said here.
          Effect.tap(({ prior, next }) =>
            prior.control !== "heard" && next.control === "heard"
              ? Effect.log(`${label}: control plane heard${prior.control === "silent" ? " again" : ""}`)
              : Effect.void,
          ),
        ),
      ),
    settle: (event) =>
      persist(reconcile).pipe(
        Effect.andThen(describe),
        Effect.flatMap((d) => Effect.log(`${label}: seq=${event.data.sequence} (${event.data.reason}) ${d}`)),
      ),
    isCurrent: (next) => Effect.map(Ref.get(state), (now) => isCurrent(now, next.applied)),
    publish: (action) => persist(perform(action)),
    superseded: (action) => Effect.logDebug(`${label}: ${action._tag} superseded by a newer event, not published`),
  });

  const applyEvent = (event: CircuitEvent) => {
    const { data, type } = event;
    return Ref.modify(contract, (before) => {
      const after = observe(before, type, O.fromUndefinedOr(data.lease), data.sequence);
      return [after.gaps > before.gaps ? O.some(before) : O.none<ContractState>(), after] as const;
    }).pipe(
      Effect.flatMap(
        O.match({
          onNone: () => Effect.void,
          onSome: (before) =>
            Effect.logWarning(
              `${label}: sequence gap — expected ${O.getOrElse(O.map(before.last, (l) => l.sequence + 1), () => 0)}, got ${data.sequence} (${type}, ${data.state}, ${data.reason})`,
            ),
        }),
      ),
      Effect.andThen(applyInOrder(event)),
      Effect.flatMap((applied) =>
        applied
          ? Effect.void
          : Effect.sync(() => void counts.stale++).pipe(
              Effect.andThen(
                Effect.logWarning(`${label}: seq=${data.sequence} (${data.reason}, ${data.state}) is stale, ignored`),
              ),
            ),
      ),
    );
  };

  yield* control.consume(controlQueue, (body) =>
    Result.match(decodeCircuitEvent(body), {
      onFailure: (why) =>
        sampleUnreadable(
          why === "malformed-json"
            ? "control message that is not JSON"
            : "control message that does not match the published schema",
        ),
      onSuccess: (event) => {
        if (event.data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
        Tally.observed(counts, event.type);

        // Accepted even on failure: the next snapshot re-applies the state, and every daemon publishes
        // the same triggers. Dead-lettering would only pile up one copy per daemon, with nothing to replay it into.
        return runInContext(
          applyEvent(event).pipe(
            Effect.as<Settlement>("accept"),
            Effect.catchCause((cause) =>
              Effect.logError(
                `${label}: seq=${event.data.sequence} (${event.data.reason}) not applied in full; the next snapshot re-applies it`,
                cause,
              ).pipe(Effect.as<Settlement>("accept")),
            ),
          ),
        );
      },
    }),
  );

  /** A clock-driven command, and the reconcile its state may call for. */
  const onClock = (command: (at: number) => Command) =>
    transitions.withPermit(
      Effect.gen(function* () {
        const at = yield* Clock.currentTimeMillis;
        const { prior, next } = yield* dispatch(command(at));
        // Always: the floor lease lapses with time, not with a transition.
        yield* reconcile;
        return { prior, next };
      }),
    );

  // Nothing is read from the body: the delivery *is* the election result.
  yield* control.consume(floorQueueFor(cfg.apiId), () =>
    runInContext(
      onClock((at) => ({ _tag: "FloorElected", at })).pipe(
        Effect.asVoid,
        Effect.catchCause((cause) => Effect.logError(`${label}: acting on the floor election failed`, cause)),
      ),
    ),
  );

  /**
   * Settled only after the action runs. On failure the sequence is un-marked and
   * the trigger requeued after a pause; if the daemon dies, SAC hands the unacked
   * trigger to the next. A redrive holds it at most `REDRIVE_MAX_HOLD`, inside the
   * broker's `consumer_timeout` (Redrive.test.ts checks the two).
   */
  const onTrigger =
    (what: "probe" | "redrive", command: (sequence: number) => Command) =>
    (body: string): Settlement | Promise<Settlement> =>
      Result.match(decodeElectionTrigger(body), {
        onFailure: (why) =>
          sampleUnreadable(
            why === "malformed-json"
              ? `${what} trigger that is not JSON`
              : `${what} trigger that does not match the schema`,
          ),
        onSuccess: ({ sequence }) =>
          runInContext(
            dispatch(command(sequence)).pipe(
              Effect.flatMap(({ actions }) => performAll(actions)),
              Effect.as<Settlement>("accept"),
              Effect.catchCause((cause) =>
                Effect.logError(`${label}: ${what} for seq=${sequence} failed, retrying`, cause).pipe(
                  Effect.andThen(dispatch({ _tag: "TriggerFailed", election: what, sequence })),
                  Effect.andThen(Effect.sleep(TRIGGER_RETRY_HOLD)),
                  Effect.as<Settlement>("requeue"),
                ),
              ),
            ),
          ),
      });

  // Registered for life: SAC promotion needs candidates already waiting.
  yield* control.consume(
    probeQueue,
    onTrigger("probe", (sequence) => ({ _tag: "ProbeTriggered", sequence })),
  );

  yield* control.consume(
    redriveQueue,
    onTrigger("redrive", (sequence) => ({ _tag: "RedriveTriggered", sequence })),
  );

  // Idle until the first event (ADR 019): nothing to start, but a restart may find a stale channel ref.
  yield* transitions.withPermit(reconcile);
  yield* Effect.log(
    `${label}: up — position=${selfPosition.toFixed(3)} maxInFlight=${cfg.maxInFlight} ` +
      `limit=${O.match(cfg.limit, { onNone: () => "off", onSome: (l) => `${l.min}-${l.max} x${l.decrease}` })} ` +
      `egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} control=${controlQueue}; ` +
      `idle until the control plane is heard`,
  );

  /** Counts are flushed once a second: a fiber per metric write would be the costliest thing on the message path. */
  let published = Tally.nothing;

  /** One table for the flush and the startup zeroing. */
  const counters = [
    ["ok", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "ok" })],
    ["failed", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "failed" })],
    ["shed", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "shed" })],
    ["refused", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "refused" })],
    ["probed", Metric.withAttributes(Telemetry.probes, attrs)],
    ["undecodable", Metric.withAttributes(Telemetry.undecodable, attrs)],
    ["gaps", Metric.withAttributes(Telemetry.controlGaps, attrs)],
    ["duplicates", Metric.withAttributes(Telemetry.controlDuplicates, attrs)],
    ["stale", Metric.withAttributes(Telemetry.controlStale, attrs)],
    ["discardedFormat", Metric.withAttributes(Telemetry.discarded, { ...attrs, reason: "format" })],
    ["discardedMalformed", Metric.withAttributes(Telemetry.discarded, { ...attrs, reason: "malformed" })],
    ["discardedKeyless", Metric.withAttributes(Telemetry.discarded, { ...attrs, reason: "keyless" })],
  ] as const;

  const flush = Effect.gen(function* () {
    const current = yield* Ref.get(state);
    const now = yield* Clock.currentTimeMillis;

    yield* Effect.all(
      [
        Metric.update(Metric.withAttributes(Telemetry.circuitState, attrs), STATE_CODE[current.circuit]),
        Metric.update(Metric.withAttributes(Telemetry.targetFraction, attrs), current.policy.fraction),
        Metric.update(Metric.withAttributes(Telemetry.floorHeld, attrs), isFloor(current, now) ? 1 : 0),
        Metric.update(
          Metric.withAttributes(Telemetry.controlKnowledge, attrs),
          Telemetry.CONTROL_KNOWLEDGE_CODE[current.control],
        ),
      ],
      { discard: true },
    );

    const snapshot = Tally.snapshot(counts, yield* Ref.get(contract));
    const delta = Tally.since(published, snapshot);
    published = snapshot;

    yield* Effect.forEach(
      counters,
      ([field, metric]) =>
        delta[field] > 0 ? Metric.update(metric, delta[field]) : Effect.void,
      { discard: true },
    );
  });

  // Zeroed so a tile meant to sit at zero does not read "No data".
  yield* Effect.forEach(counters, ([, metric]) => Metric.update(metric, 0), { discard: true });

  yield* Effect.forkScoped(Effect.repeat(flush, Schedule.spaced(FLUSH_INTERVAL)));

  /** The ramp is time-gated, silence is noticed by time, and the floor lease lapses with it: something must tick. */
  const tick = onClock((at) => ({ _tag: "ClockTick", at })).pipe(
    Effect.flatMap(({ prior, next }) =>
      // A tick only ever moves into `silent`; the way back is an event (the applier logs it).
      next.control === "silent" && prior.control !== "silent"
        ? describe.pipe(
            Effect.flatMap((d) =>
              Effect.logWarning(
                `${label}: no control event for ${SILENCE_MS / 1000}s — the circuit is no longer believed; ` +
                  `falling back to ${Math.round(next.policy.fraction * 100)}% of the fleet (ADR 019) ${d}`,
              ),
            ),
          )
        : next.policy.fraction !== prior.policy.fraction || next.policy.floor !== prior.policy.floor
          ? describe.pipe(
              Effect.flatMap((d) =>
                Effect.log(
                  `${label}: ramp ${Math.round(prior.policy.fraction * 100)}% -> ` +
                    `${Math.round(next.policy.fraction * 100)}% ${d}`,
                ),
              ),
            )
          : Effect.void,
    ),
    Effect.catchCause((cause) => Effect.logError(`${label}: clock tick failed`, cause)),
  );

  yield* Effect.forkScoped(Effect.repeat(tick, Schedule.spaced(TICK_INTERVAL)));

  /** The reducer keeps this to a heard CLOSED and the floor; Redrive.ts makes it a no-op while a pass runs. */
  const sweep = Effect.gen(function* () {
    const at = yield* Clock.currentTimeMillis;
    const { actions } = yield* dispatch({ _tag: "SweepTick", at });
    yield* performAll(actions);
  }).pipe(Effect.catchCause((cause) => Effect.logError(`${label}: sweep failed`, cause)));

  yield* Effect.forkScoped(Effect.repeat(sweep, Schedule.spaced(SWEEP_INTERVAL)));

  // Without it a deaf daemon looks like one whose circuit has not moved.
  yield* Effect.repeat(
    describe.pipe(Effect.flatMap((s) => Effect.log(`${label}: heartbeat ${s}`))),
    Schedule.spaced(HEARTBEAT_INTERVAL),
  );
});
