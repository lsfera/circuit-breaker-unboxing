import { Effect, Exit, Metric, Option, Ref, Scope, Semaphore } from "effect";
import { makeRmq, Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  CONTROL_EXCHANGE_OPTIONS,
  controlQueueFor,
  controlQueueOptions,
  deadLetterQueueFor,
  deadLetterQueueOptions,
  decodeCircuitEvent,
  probeTriggerQueueFor,
  redriveTriggerQueueFor,
  routingKeyFor,
  sacQueueOptions,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import { State, STATE_CODE } from "@egress/domain/Model.ts";
import { initialContract, observe } from "./Contract.ts";
import { makeRedrive } from "./Redrive.ts";
import { desired, initialState, plan, reduce } from "./DaemonState.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Consumer, RmqConnectOptions, Settlement } from "@egress/rmq/Client.ts";
import type { ContractState } from "./Contract.ts";
import type { Action, Command, DaemonState } from "./DaemonState.ts";

/**
 * One competing-consumer daemon: one process, one index in a fleet of
 * `fleetSize`. docker-compose.yml runs N of these as N containers, so the SAC
 * elections below are contended by real separate processes rather than by
 * fibers that happen to share a runtime.
 *
 * Nothing here coordinates with the other daemons. Each one subscribes to its
 * own queue on `circuit.control`, feeds every event through the pure reducer
 * in DaemonState.ts, and starts or stops its own work consumption according to
 * whether its index falls under the agreed target. The fleet converges from
 * the same events with no leader and no shared state — the aggregator already
 * did the hard part of turning divergent replica views into one state.
 *
 * ## Two connections
 *
 * The control connection carries the control consumer, the two SAC consumers
 * and the trigger publishers, all of which live for the life of the process.
 * Everything that churns — the work consumer, opened and closed on every
 * transition, and the one-message HALF_OPEN probe — runs on a second
 * connection this module opens and destroys itself.
 *
 * This used to be damage control. On the AMQP 1.0 client, closing a consumer
 * with deliveries in flight stranded them and enough strandings stalled every
 * link on the connection: a daemon whose work consumer shared the control
 * connection went deaf to `circuit.control` after a handful of transitions
 * while still looking healthy. Under amqplib that cannot happen — each
 * consumer holds its own channel, and the integration test that used to pin
 * the stall now pins its absence.
 *
 * The split stays because it is still the cheapest way to abandon work
 * wholesale: dropping the connection is how a probe or a redrive pass returns
 * everything it was holding without walking its own deliveries. It could now
 * be collapsed onto one connection with two channels, and that is worth doing
 * on its own rather than folded into a client migration, where a regression
 * would be impossible to attribute.
 *
 * ## Egress stays transparent
 *
 * One configured address, no replica names, no admin ports. A daemon calls
 * `${egressAddr}${apiPath}` and never learns Envoy is a fleet — that topology
 * is known to exactly one thing in this repo, the aggregator's FleetSource.
 *
 * ## Backpressure is settlement timing
 *
 * The work handler returns the egress call's promise, and `@egress/rmq`
 * settles only once it resolves. AMQP 1.0 replenishes credit on settlement, so
 * a daemon holding `maxInFlight` calls open stops settling, the broker stops
 * pushing, and the backlog stays where it belongs — in the queue, visible.
 * The first version of this file accepted every message on arrival and called
 * afterwards, which turned a 50k backlog into 50k concurrent calls from one
 * daemon: precisely the herd the fleet-level policy exists to prevent.
 *
 * ## Work that fails
 *
 * A failed call requeues its message and the *broker* counts the attempts —
 * the work queue is a quorum queue carrying `x-delivery-limit`, so RabbitMQ
 * dead-letters the message itself once the budget is spent. The daemon does
 * not count and could not: this client reports `deliveryCount: 0` on every
 * delivery, and an in-process counter would be lost the moment the message
 * moved to another daemon. See WORK_DELIVERY_LIMIT in @egress/rmq.
 *
 * Preserving failed work is not recovering it, so `REDRIVE_ON_CLOSE` replays
 * `<apiId>.work.dead` onto the work queue when the circuit closes, from the
 * one daemon a second SAC election picks. Off by default: whether a
 * two-minute-old payment attempt is still worth making is a property of the
 * workload, not of the transport.
 */

export type DaemonConfig = {
  readonly apiId: string;
  /** 0-based position in the fleet. Fixed per container — see docker-compose.yml. */
  readonly index: number;
  readonly fleetSize: number;
  readonly instanceId: string;
  /** Where to open the disposable work connection — the same broker as the control one. */
  readonly connect: RmqConnectOptions;
  /** The single egress address, exactly as a real client would be given it. */
  readonly egressAddr: string;
  /** The route on that address for this API — /payments for payments-provider. */
  readonly apiPath: string;
  /**
   * Ceiling on concurrent third-party calls from this one daemon. Without it
   * a daemon draining a backlog fires one call per delivered message as fast
   * as the broker can push, which is a thundering herd of its own making —
   * the fleet-level policy would be scaling daemons down while each surviving
   * daemon hammered harder.
   */
  readonly maxInFlight: number;
  /**
   * Replay `<apiId>.work.dead` onto the work queue when the circuit closes.
   * Off by default: see the module doc — this is a statement about whether
   * this workload's messages are still worth doing later, which only the
   * workload's owner knows.
   */
  readonly redriveOnClose: boolean;
  /** Ceiling on messages moved in a single redrive pass, so a huge backlog is recovered in bounded bites rather than one burst. */
  readonly redriveMax: number;
};

export const runDaemon = (cfg: DaemonConfig) =>
  Effect.gen(function* () {
    const control = yield* Rmq;
    const label = `${cfg.apiId}/daemon-${cfg.index}`;

    const workQueue = workQueueFor(cfg.apiId);
    const deadQueue = deadLetterQueueFor(cfg.apiId);
    const probeQueue = probeTriggerQueueFor(cfg.apiId);
    const redriveQueue = redriveTriggerQueueFor(cfg.apiId);
    const controlQueue = controlQueueFor(cfg.apiId, cfg.instanceId);

    // Every daemon declares the shared topology. These are all idempotent
    // declares of the same arguments, so N daemons racing to start is fine —
    // whoever gets there first wins and the rest are no-ops.
    const exchange = yield* control.declareTopicExchange(
      CONTROL_EXCHANGE,
      CONTROL_EXCHANGE_OPTIONS,
    );
    // Declared before the queue that points at it, so a rejection in the
    // fleet's first seconds has somewhere to land. It is the only queue with
    // no dead-letter target of its own — the end of the line, and pointing it
    // at itself is a cycle. Everything else routes rejections here.
    yield* control.declareQueue(deadQueue, deadLetterQueueOptions());
    yield* control.declareQueue(workQueue, workQueueOptions(cfg.apiId));
    yield* control.declareQueue(probeQueue, sacQueueOptions(cfg.apiId));
    yield* control.declareQueue(redriveQueue, sacQueueOptions(cfg.apiId));
    const controlQ = yield* control.declareQueue(controlQueue, controlQueueOptions(cfg.apiId));
    yield* control.bind(routingKeyFor(cfg.apiId), exchange, controlQ);

    /**
     * Everything this daemon decides, in one value — see DaemonState.ts. The
     * circuit state, the policy, and the two dedupe sequences used to be four
     * separate Refs with invariants between them and nothing saying so; the two
     * sequences in particular were read-then-write across two operations, from
     * handlers that run concurrently on AMQP callbacks. One `Ref.modify` over
     * one value makes each transition atomic and each decision testable without
     * a broker.
     */
    const now = yield* Effect.clockWith((c) => c.currentTimeMillis);
    const state = yield* Ref.make<DaemonState>(initialState(cfg.fleetSize, now));

    /**
     * The connections, which are resources rather than decisions: this is the
     * "actual" side that `plan` compares the desired shape against. Kept as
     * three handles because that is what they are, and because Redrive.ts owns
     * one of them by contract.
     *
     * The work scope is non-null exactly while this daemon is pulling work.
     */
    const workScope = yield* Ref.make<Scope.Closeable | null>(null);
    const probeScope = yield* Ref.make<Scope.Closeable | null>(null);
    const redriveScope = yield* Ref.make<Scope.Closeable | null>(null);

    let inFlight = 0;
    let queued = 0;
    let ok = 0;
    let failed = 0;
    let probed = 0;
    let redriven = 0;
    /** Messages this daemon could not read at all, on any of its queues. */
    let undecodable = 0;

    /** The delivery contract, observed from this side of the broker — see Contract.ts. */
    let contract: ContractState = initialContract;
    const eventsByType = new Map<string, number>();

    /**
     * A plain concurrency gate, and the reason it can *wait* rather than
     * drop: `@egress/rmq`'s `consume` accepts a message only once the
     * handler's promise settles, so a handler parked here holds its
     * delivery unsettled and the broker's credit window stops refilling.
     * Waiting is therefore real backpressure all the way to the queue, not
     * an in-process buffer pretending to be one.
     */
    const waiting: Array<() => void> = [];
    const acquire = (): Promise<void> => {
      if (inFlight < cfg.maxInFlight) {
        inFlight++;
        return Promise.resolve();
      }
      queued++;
      return new Promise<void>((resolve) => waiting.push(resolve)).then(() => {
        queued--;
        inFlight++;
      });
    };
    const release = () => {
      inFlight--;
      waiting.shift()?.();
    };

    /**
     * One real call to the flaky third party, through the egress listener.
     * Deliberately plain async: it is awaited by the AMQP message handler,
     * and wrapping it in Effect would buy nothing here.
     */
    const callEgress = async (): Promise<Settlement> => {
      await acquire();
      try {
        const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
          signal: AbortSignal.timeout(2000),
        });
        // Read the body even though nothing wants it. An unconsumed response
        // holds its connection out of the pool until the GC gets to it, which
        // is the standard way to leak sockets at rate — and this is the
        // highest-rate call in the system. It also makes "the call finished"
        // mean the response actually arrived, rather than just its headers.
        await res.text().catch(() => {});
        if (res.ok) {
          ok++;
          return "accept";
        }
        failed++;
        return "requeue";
      } catch {
        // Connection refused / timeout once the cluster is fully ejected is
        // the expected shape of an outage, not an error to report here — the
        // aggregator is what judges the API's health, from Envoy's own view.
        //
        // `requeue`, not `discard`: the work queue is a quorum queue carrying
        // `x-delivery-limit`, so the broker counts the attempts and parks the
        // message on the dead-letter queue itself once the budget is spent
        // (WORK_DELIVERY_LIMIT in @egress/rmq/ControlPlane.ts). The daemon
        // does not count, and could not — the client reports deliveryCount 0
        // on every delivery — which is exactly why this had to be the
        // broker's job. `failed` therefore counts *attempts* now, not
        // messages: a message that fails its whole budget increments it once
        // per try, which is what a rate of failing calls should measure.
        failed++;
        return "requeue";
      } finally {
        release();
      }
    };

    /**
     * How many unreadable messages this daemon preserves before it starts
     * letting them go.
     *
     * Rejecting one is right: the evidence is worth more than the message.
     * Rejecting every one is not, and the arithmetic is unkind — control
     * events fan out to *every* daemon's own queue, so a schema mismatch
     * between publisher and fleet is not one bad message, it is every message
     * multiplied by the fleet size, all of it landing on one dead-letter
     * queue at the full event rate. A bounded sample answers the question a
     * human actually has ("what does the message look like?") without turning
     * a version skew into a second incident. `egress_daemon_undecodable_total`
     * keeps counting past the bound, so the *rate* stays visible even after
     * the samples stop.
     */
    const UNDECODABLE_SAMPLE = 20;

    /** Preserve this one if we are still sampling; otherwise let it go, loudly, once. */
    const sampleUnreadable = (what: string): Settlement => {
      undecodable++;
      if (undecodable <= UNDECODABLE_SAMPLE) {
        Effect.runFork(Effect.logWarning(`${label}: ${what}, dead-lettered`));
        return "discard";
      }
      if (undecodable === UNDECODABLE_SAMPLE + 1) {
        Effect.runFork(
          Effect.logWarning(
            `${label}: ${UNDECODABLE_SAMPLE} unreadable messages already preserved on ` +
              `${deadQueue} — accepting further ones rather than flooding it; ` +
              `egress_daemon_undecodable_total still counts them all`,
          ),
        );
      }
      return "accept";
    };

    /**
     * Serializes reconciliation against itself. Control events and probe
     * triggers both land on AMQP callbacks that fork into the runtime, so
     * without a permit two of them could each observe "no work connection" and
     * both open one.
     */
    const gate = yield* Semaphore.make(1);

    /** Open a throwaway connection and consume the work queue on it. */
    const startWork = Effect.gen(function* () {
      const scope = yield* Scope.make();
      const work = yield* Effect.provideService(makeRmq(cfg.connect), Scope.Scope, scope);
      yield* work.consume(workQueue, () => callEgress());
      yield* Ref.set(workScope, scope);
    });

    const stopScope = (ref: Ref.Ref<Scope.Closeable | null>) =>
      Ref.getAndSet(ref, null).pipe(
        Effect.flatMap((scope) => (scope === null ? Effect.void : Scope.close(scope, Exit.void))),
      );

    /**
     * Make the connections match the state, and nothing else.
     *
     * Which connections *should* exist is `desired`, and the difference between
     * that and what does exist is `plan` — both pure, both in DaemonState.ts,
     * both tested without a broker. What is left here is the part that can only
     * happen here: opening and closing sockets, under the permit.
     */
    const reconcile = gate.withPermit(
      Effect.gen(function* () {
        const have = {
          work: (yield* Ref.get(workScope)) !== null,
          probe: (yield* Ref.get(probeScope)) !== null,
          redrive: (yield* Ref.get(redriveScope)) !== null,
        };
        const actions = plan(desired(yield* Ref.get(state), cfg.index, cfg.fleetSize), have);
        if (actions.startWork) yield* startWork;
        if (actions.stopWork) yield* stopScope(workScope);
        if (actions.stopProbe) yield* stopScope(probeScope);
        if (actions.stopRedrive) yield* stopScope(redriveScope);
      }),
    );

    /**
     * Take exactly one message and make one real call, on a connection that
     * exists only for this probe. Opened only by the SAC-elected daemon;
     * closed as soon as the first message arrives, so the "one probe"
     * contract holds even with a backlog ready to deliver.
     */
    const probeOnce = gate.withPermit(
      Effect.gen(function* () {
        if ((yield* Ref.get(probeScope)) !== null) return;
        const scope = yield* Scope.make();
        const probe = yield* Effect.provideService(makeRmq(cfg.connect), Scope.Scope, scope);

        // Cancel the consumer from inside the handler — that is what stops
        // delivery at the first message — and let `reconcile` retire the
        // connection later, when the state leaves HALF_OPEN. Cancelling
        // rather than closing matters: the channel stays open long enough to
        // settle the message this probe is still holding, so the call's
        // outcome decides its fate rather than the teardown doing it.
        let self: Consumer | null = null;
        let taken = false;
        const consumer = yield* probe.consume(
          workQueue,
          () => {
            if (taken || self === null) return;
            taken = true;
            Effect.runFork(probe.closeConsumer(self));
            return callEgress();
          },
          // The one state whose contract is "exactly one call" should ask the
          // broker for exactly one message. The AMQP 1.0 client had no such
          // lever — its credit window was a fixed 1000, so a probe against a
          // deep queue was handed a thousand deliveries and stranded them all
          // on close. That is what made a throwaway connection per probe
          // necessary rather than merely tidy.
          { prefetch: 1 },
        );
        self = consumer;

        probed++;
        yield* Ref.set(probeScope, scope);
        yield* Effect.log(`${label}: elected prober, taking one message`);
      }),
    );

    /**
     * Dead-letter recovery lives in its own module — see Redrive.ts. What is
     * passed here is the coupling, made explicit: a connection it can destroy,
     * the two queue names, the circuit state it must stop on, and this
     * daemon's scope Ref and permit, because `reconcile` retires the
     * connection from the other side when the state changes.
     */
    const redriveOnce = makeRedrive({
      label,
      enabled: cfg.redriveOnClose,
      connect: cfg.connect,
      workQueue,
      deadQueue,
      maxPerPass: cfg.redriveMax,
      isClosed: Ref.get(state).pipe(Effect.map((s) => s.circuit === State.CLOSED)),
      onReplayed: () => {
        redriven++;
      },
      scope: redriveScope,
      gate,
    });

    // Published by *every* daemon on entering HALF_OPEN, not just one, so the
    // trigger still arrives when some daemons are down. SAC delivers all of
    // them to the single elected consumer, which dedupes on the circuit
    // sequence below — that is why the sequence travels in the body.
    const trigger = yield* control.publisherToQueue(probeQueue);
    const redriveTrigger = yield* control.publisherToQueue(redriveQueue);

    /**
     * One command in, one atomic transition out, and the actions the pure
     * reducer asked for.
     *
     * `Ref.modify` rather than get-then-set, which is the point: these are
     * called from AMQP callbacks that run concurrently, and the two dedupe
     * checks ("have I already probed for this sequence?") used to read and
     * write across two operations. Now the decision and the record of it are
     * the same step.
     */
    const dispatch = (command: Command) =>
      Ref.modify(state, (prior) => {
        const { next, actions } = reduce(prior, command, cfg.fleetSize, cfg.redriveOnClose);
        return [{ prior, next, actions }, next] as const;
      });

    /** The shell half of the reducer: what an Action actually does. */
    const perform = (action: Action) => {
      switch (action._tag) {
        case "PublishProbeTrigger":
          return control.send(trigger, JSON.stringify({ sequence: action.sequence }));
        case "PublishRedriveTrigger":
          return control.send(redriveTrigger, JSON.stringify({ sequence: action.sequence }));
        case "Probe":
          return probeOnce;
        case "Redrive":
          return redriveOnce;
      }
    };

    const performAll = (actions: ReadonlyArray<Action>) =>
      Effect.forEach(actions, perform, { discard: true });

    const describe = Effect.gen(function* () {
      const { circuit, policy } = yield* Ref.get(state);
      const active = (yield* Ref.get(workScope)) !== null;
      return (
        `${circuit} target=${policy.targetActive}/${cfg.fleetSize} self=${active ? "ACTIVE" : "idle"} ` +
        `calls ok=${ok} failed=${failed} inFlight=${inFlight} queued=${queued} ` +
        `control=${[...eventsByType.values()].reduce((a, b) => a + b, 0)} ` +
        `gaps=${contract.gaps} dup=${contract.duplicates}`
      );
    });

    /**
     * Order is preserved from when this was written out by hand: apply the
     * transition, make the connections match it, say so, and only then publish
     * whatever triggers the transition called for.
     */
    const applyEvent = (circuitState: State, sequence: number, reason: string) =>
      Effect.gen(function* () {
        const at = yield* Effect.clockWith((c) => c.currentTimeMillis);
        const { actions } = yield* dispatch({
          _tag: "CircuitChanged",
          state: circuitState,
          sequence,
          at,
        });
        yield* reconcile;
        yield* Effect.log(`${label}: seq=${sequence} (${reason}) ${yield* describe}`);
        yield* performAll(actions);
      });

    yield* control.consume(controlQueue, (body) => {
      const decoded = decodeCircuitEvent(body);
      if (Option.isNone(decoded)) {
        // Same stance as @egress/subscriber: an event that does not match the
        // published contract is never half-applied. It is rejected rather
        // than accepted, so it lands on the canonical dead-letter queue
        // instead of existing only as a log line nobody can act on — a
        // control message the fleet could not read is precisely the thing
        // you want to still have in your hands afterwards. Up to a point:
        // see UNDECODABLE_SAMPLE for why that point exists.
        return sampleUnreadable("undecodable control message");
      }
      const { data, type } = decoded.value;
      if (data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
      eventsByType.set(type, (eventsByType.get(type) ?? 0) + 1);

      contract = observe(contract, type, data.sequence);

      Effect.runFork(
        applyEvent(data.state, data.sequence, data.reason).pipe(
          Effect.catchCause((cause) => Effect.logError(`${label}: applying event failed`, cause)),
        ),
      );
    });

    // Registered for the whole life of the process and idle almost all of it.
    // Being *registered* is the point: SAC promotion needs candidates already
    // waiting when the active one dies, which is exactly the failover this
    // fleet gets for free from the broker instead of hand-rolling.
    yield* control.consume(probeQueue, (body) => {
      let sequence = -1;
      try {
        sequence = Number(JSON.parse(body).sequence ?? -1);
      } catch {
        return sampleUnreadable("malformed probe trigger");
      }
      // A duplicate trigger for a transition already probed produces no
      // actions, which is the reducer's job rather than this handler's.
      Effect.runFork(
        dispatch({ _tag: "ProbeTriggered", sequence }).pipe(
          Effect.flatMap(({ actions }) => performAll(actions)),
          Effect.catchCause((cause) => Effect.logError(`${label}: probe failed`, cause)),
        ),
      );
    });

    // The second SAC election, identical in shape to the prober's: every
    // daemon publishes the trigger so it still arrives when some are down,
    // the broker delivers all of them to one consumer, and that consumer
    // dedupes on the circuit sequence so one recovery means one replay.
    yield* control.consume(redriveQueue, (body) => {
      let sequence = -1;
      try {
        sequence = Number(JSON.parse(body).sequence ?? -1);
      } catch {
        return sampleUnreadable("malformed redrive trigger");
      }
      Effect.runFork(
        dispatch({ _tag: "RedriveTriggered", sequence }).pipe(
          Effect.flatMap(({ actions }) => performAll(actions)),
          Effect.catchCause((cause) => Effect.logError(`${label}: redrive failed`, cause)),
        ),
      );
    });

    // CLOSED until told otherwise: a daemon that starts mid-incident learns
    // the real state from the aggregator's next snapshot (snapshotMs), which
    // is what those periodic republishes are for.
    yield* reconcile;
    yield* Effect.log(
      `${label}: up — fleet=${cfg.fleetSize} maxInFlight=${cfg.maxInFlight} ` +
        `egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} control=${controlQueue}`,
    );

    /**
     * Metrics are published from here rather than from each call site: the
     * message path is a plain async function running a few hundred times a
     * second, and forking a fiber per metric write would be the most
     * expensive thing in it. Counters go up by the delta since the last
     * flush, which is precisely what a Prometheus counter is; gauges are
     * just set.
     */
    const attrs = { apiId: cfg.apiId };
    let flushed = { ok: 0, failed: 0, probed: 0, redriven: 0, undecodable: 0, gaps: 0, duplicates: 0 };
    let flushedEvents = new Map<string, number>();

    const flush = Effect.gen(function* () {
      const { circuit, policy } = yield* Ref.get(state);
      const active = (yield* Ref.get(workScope)) !== null;

      yield* Effect.all(
        [
          Metric.update(Metric.withAttributes(Telemetry.circuitState, attrs), STATE_CODE[circuit]),
          Metric.update(Metric.withAttributes(Telemetry.targetActive, attrs), policy.targetActive),
          Metric.update(Metric.withAttributes(Telemetry.fleetSize, attrs), cfg.fleetSize),
          Metric.update(Metric.withAttributes(Telemetry.selfActive, attrs), active ? 1 : 0),
          Metric.update(Metric.withAttributes(Telemetry.inFlight, attrs), inFlight),
          Metric.update(Metric.withAttributes(Telemetry.queued, attrs), queued),
        ],
        { discard: true },
      );

      const delta = {
        ok: ok - flushed.ok,
        failed: failed - flushed.failed,
        probed: probed - flushed.probed,
        redriven: redriven - flushed.redriven,
        undecodable: undecodable - flushed.undecodable,
        gaps: contract.gaps - flushed.gaps,
        duplicates: contract.duplicates - flushed.duplicates,
      };
      flushed = {
        ok,
        failed,
        probed,
        redriven,
        undecodable,
        gaps: contract.gaps,
        duplicates: contract.duplicates,
      };

      if (delta.ok > 0) {
        yield* Metric.update(
          Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "ok" }),
          delta.ok,
        );
      }
      if (delta.failed > 0) {
        // Published together because the gap between them is informative.
        // Every failed call rejects its message, but a rejection whose link
        // has already gone (OPEN tearing down the work connection with calls
        // still in flight) is swallowed by the client's guarded settle, and
        // the broker requeues that delivery instead of dead-lettering it —
        // so dead_lettered trailing calls{failed} slightly is work that was
        // retried rather than work that was lost. The two diverging by a
        // *lot* would mean something else, which is why both are here.
        yield* Effect.all(
          [
            Metric.update(
              Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "failed" }),
              delta.failed,
            ),
            Metric.update(Metric.withAttributes(Telemetry.deadLettered, attrs), delta.failed),
          ],
          { discard: true },
        );
      }
      if (delta.probed > 0) {
        yield* Metric.update(Metric.withAttributes(Telemetry.probes, attrs), delta.probed);
      }
      if (delta.redriven > 0) {
        yield* Metric.update(Metric.withAttributes(Telemetry.redriven, attrs), delta.redriven);
      }
      if (delta.undecodable > 0) {
        yield* Metric.update(
          Metric.withAttributes(Telemetry.undecodable, attrs),
          delta.undecodable,
        );
      }
      if (delta.gaps > 0) {
        yield* Metric.update(Metric.withAttributes(Telemetry.controlGaps, attrs), delta.gaps);
      }
      if (delta.duplicates > 0) {
        yield* Metric.update(
          Metric.withAttributes(Telemetry.controlDuplicates, attrs),
          delta.duplicates,
        );
      }
      for (const [type, count] of eventsByType) {
        const seen = count - (flushedEvents.get(type) ?? 0);
        if (seen > 0) {
          yield* Metric.update(
            Metric.withAttributes(Telemetry.controlEvents, { ...attrs, type }),
            seen,
          );
        }
      }
      flushedEvents = new Map(eventsByType);
    });

    /**
     * Zero every counter once at startup so its series exists before
     * anything has happened to it. Without this the dashboard's
     * delivery-contract tiles read "No data" until the first gap — and a
     * tile whose entire job is to sit at zero through an incident is worse
     * than useless if zero is indistinguishable from broken.
     */
    yield* Effect.all(
      [
        Metric.update(Metric.withAttributes(Telemetry.controlGaps, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.controlDuplicates, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.deadLettered, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.probes, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.redriven, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.undecodable, attrs), 0),
        Metric.update(Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "ok" }), 0),
        Metric.update(Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "failed" }), 0),
      ],
      { discard: true },
    );

    yield* Effect.forkScoped(
      Effect.forever(Effect.sleep("1 second").pipe(Effect.andThen(flush))),
    );

    /**
     * The ramp advances on a clock, so something has to look at the clock.
     *
     * Gating rungs on elapsed time fixes half the problem; the other half is
     * that `step` only ran when a control message arrived, so a quiet recovery
     * — no transitions, one snapshot every `snapshotMs` — still advanced the
     * ramp at the aggregator's pace rather than its own. This is the daemon
     * asking the question on its own schedule.
     *
     * Only while CLOSED, and only when the answer changes: every other state
     * is a level, not a ramp, and re-reconciling an unchanged target would
     * rebuild connections once a second for no reason.
     */
    const advanceRamp = Effect.gen(function* () {
      const at = yield* Effect.clockWith((c) => c.currentTimeMillis);
      const { prior, next } = yield* dispatch({ _tag: "RampTick", at });
      if (next.policy.targetActive === prior.policy.targetActive) return;
      yield* reconcile;
      yield* Effect.log(
        `${label}: ramp ${prior.policy.targetActive} -> ${next.policy.targetActive} ` +
          `${yield* describe}`,
      );
    });

    yield* Effect.forkScoped(
      Effect.forever(Effect.sleep("1 second").pipe(Effect.andThen(advanceRamp))),
    );

    // A heartbeat independent of the control plane. Without it a daemon that
    // has gone deaf is indistinguishable from one whose circuit simply has
    // not moved — which is precisely how the stranded-delivery bug above hid
    // for as long as it did. The metrics above are the same observation made
    // scrapeable; this stays because a log line is what you actually have
    // when you are looking at one container.
    yield* Effect.forever(
      Effect.sleep("15 seconds").pipe(
        Effect.andThen(describe),
        Effect.flatMap((s) => Effect.log(`${label}: heartbeat ${s}`)),
      ),
    );
  });
