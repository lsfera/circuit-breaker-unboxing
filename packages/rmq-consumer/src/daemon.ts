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
import type { Tracer } from "effect";
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
  probeTriggerQueueFor,
  redriveTriggerQueueFor,
  routingKeyFor,
  sacQueueOptions,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import { decodeCircuitEvent, State, STATE_CODE } from "@egress/domain/Model.ts";
import { initialContract, observe } from "./Contract.ts";
import { makeRedrive } from "./Redrive.ts";
import { desired, initialState, plan, reduce } from "./DaemonState.ts";
import { position } from "./DaemonPolicy.ts";
import * as Tally from "./Tally.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Consumer, Settlement } from "@egress/rmq/Client.ts";

import type { ContractState } from "./Contract.ts";
import type { Action, Command, DaemonState } from "./DaemonState.ts";

/**
 * One competing-consumer daemon. It knows its own identity and nothing about
 * the rest of the fleet — not how many there are, not where it sits among them
 * — and coordinates with the others only through the events they all receive
 * on `circuit.control`. What proportion of the fleet should be working is
 * published; which daemons those are, each decides for itself from its own
 * position. See docs/decisions/013-the-target-as-a-fraction.md.
 *
 * One AMQP connection, with two classes of channel on it. The control consumer,
 * the two SAC election consumers and the publish channel live for the process;
 * the work consumer, the one-message probe and the redrive pass churn. Closing a
 * channel requeues everything it held unacked and touches nothing else.
 *
 * Egress stays transparent: one configured address, no replica names. Envoy's
 * topology is known to exactly one thing in this repo, the aggregator's
 * FleetSource.
 *
 * Backpressure is settlement timing. The work handler returns the egress call's
 * promise and `@egress/rmq` settles only once it resolves, so with
 * `prefetch: maxInFlight` the broker holds the next delivery until this daemon
 * finishes one — see docs/decisions/011-the-ceiling-belongs-to-the-broker.md.
 *
 * Failed work is requeued and the *broker* counts the attempts
 * (`x-delivery-limit`), because an in-process counter is lost the moment a
 * message moves to another daemon. `REDRIVE_ON_CLOSE` replays the dead-letter
 * queue on recovery, from the one daemon a second election picks; off by
 * default, because whether stale work is still worth doing is a property of the
 * workload.
 */

type DaemonConfig = {
  readonly apiId: string;
  /**
   * Names this daemon's control queue and fixes its position in the hash space,
   * which is the whole of its identity now — there is no index and no fleet
   * size. Two daemons sharing an id share a position and a queue, so it should
   * be per-replica: a pod name, a container id, or the random default.
   */
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
};

/** How often the daemon publishes counters, advances its ramp, and says it is alive. */
const FLUSH_INTERVAL = Duration.seconds(1);
const RAMP_INTERVAL = Duration.seconds(1);
const HEARTBEAT_INTERVAL = Duration.seconds(15);

export const runDaemon = (cfg: DaemonConfig) =>
  Effect.gen(function* () {
    const control = yield* Rmq;
    const label = `${cfg.apiId}/${cfg.instanceId}`;

    const workQueue = workQueueFor(cfg.apiId);
    const deadQueue = deadLetterQueueFor(cfg.apiId);
    const probeQueue = probeTriggerQueueFor(cfg.apiId);
    const redriveQueue = redriveTriggerQueueFor(cfg.apiId);
    const controlQueue = controlQueueFor(cfg.apiId, cfg.instanceId);

    // Idempotent declares of identical arguments, so N daemons racing to start is fine.
    const exchange = yield* control.declareTopicExchange(
      CONTROL_EXCHANGE,
      CONTROL_EXCHANGE_OPTIONS,
    );
    // Before the queues that point at it, so an early rejection has somewhere to land.
    yield* control.declareQueue(deadQueue, deadLetterQueueOptions());
    yield* control.declareQueue(workQueue, workQueueOptions(cfg.apiId));
    yield* control.declareQueue(probeQueue, sacQueueOptions(cfg.apiId));
    yield* control.declareQueue(redriveQueue, sacQueueOptions(cfg.apiId));
    const controlQ = yield* control.declareQueue(controlQueue, controlQueueOptions(cfg.apiId));
    yield* control.bind(routingKeyFor(cfg.apiId), exchange, controlQ);

    /**
     * The floor. One single-active-consumer queue per API, bound to the same
     * exchange, so every published event also lands in exactly one daemon's lap
     * — and that daemon runs whether or not its own position falls under the
     * fraction. Without it a small fleet can select nobody: measured at 3.2% for
     * five daemons at half, and a DEGRADED fleet that stops is indistinguishable
     * from an OPEN one. See ADR 013.
     *
     * A lease rather than a flag, because SAC promotes silently: a daemon that
     * dies holding the floor is replaced by the broker, and the replacement
     * learns it holds the floor from the next event. The lease is what stops
     * the dead one's claim outliving it.
     */
    const floorQ = yield* control.declareQueue(floorQueueFor(cfg.apiId), floorQueueOptions());
    yield* control.bind(routingKeyFor(cfg.apiId), exchange, floorQ);

    /**
     * Everything this daemon decides, in one value — see DaemonState.ts. One
     * `Ref.modify` over one value keeps each transition atomic against the
     * concurrent AMQP callbacks that drive it.
     */
    const now = yield* Clock.currentTimeMillis;
    const state = yield* Ref.make<DaemonState>(initialState(now));

    /** The churning channels — the "actual" side `plan` compares the desired shape against. */
    const workConsumer = yield* Ref.make(O.none<Consumer>());
    const probeConsumer = yield* Ref.make(O.none<Consumer>());
    const redriveConsumer = yield* Ref.make(O.none<Consumer>());

    /**
     * Calls open right now — a graph, not a decision. What *bounds* it is the
     * work consumer's prefetch; see below.
     */
    let inFlight = 0;

    /** Everything counted for the metrics registry — see Tally.ts. */
    const counts = Tally.zero();

    /** The delivery contract, observed from this side of the broker — see Contract.ts. */
    let contract: ContractState = initialContract;

    /**
     * This daemon's fixed position in the hash space, and its claim on the
     * floor. `Date.now()` for the same reason Redrive.ts uses it: the claim is
     * refreshed from an AMQP callback, which has no fiber to read a Clock in,
     * and it is read from the metric flush and the log line, which are sync.
     *
     * The lease has to outlast the gap between published events. The aggregator
     * republishes a snapshot every `snapshotMs` — 15s by default — so a minute
     * is four of them, and a floor that lapses only does so because the control
     * plane has gone quiet for far longer than the fleet's own heartbeat.
     */
    const FLOOR_LEASE_MS = 60_000;
    const selfPosition = position(cfg.instanceId);
    let floorUntil = 0;
    const floorHeld = () => Date.now() < floorUntil;
    const self = () => ({ position: selfPosition, isFloor: floorHeld() });

    /**
     * One call to the third party through the egress listener. Plain async: it is
     * awaited by the AMQP handler, and Effect would buy nothing here.
     */
    const rawCall = async (): Promise<Settlement> => {
      inFlight++;
      try {
        const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
          signal: AbortSignal.timeout(2000),
        });
        // Drain the body even though nothing wants it: an unconsumed response holds
        // its connection out of the pool, which at this rate leaks sockets.
        await res.text().catch(() => {});
        if (res.ok) {
          counts.ok++;
          return "accept";
        }
        counts.failed++;
        return "requeue";
      } catch {
        // Connection refused or timeout is the expected shape of an outage, not an
        // error to report: the aggregator judges the API's health from Envoy's view.
        // `requeue`, not `discard` — the broker counts attempts and parks the message
        // itself at `x-delivery-limit`. `failed` therefore counts attempts, not messages.
        counts.failed++;
        return "requeue";
      } finally {
        inFlight--;
      }
    };

    /**
     * Captured so work started from an AMQP callback can still reach them.
     * The bare `Effect.run*` entry points build a fresh runtime with *default*
     * services: a span goes to the no-op tracer and a log line to the default
     * logger, neither of which is the one this process configured. Measured —
     * a bare `runFork(logWarning(...))` never reaches a provided `Logger`.
     *
     * Every callback below starts its work through one of these two.
     */
    const services = yield* Effect.context<never>();
    const runInContext = Effect.runPromiseWith(services);
    const forkInContext = Effect.runForkWith(services);

    /**
     * The same call inside a span, only when the message carried a parent:
     * the parent mapped to a traced call, or the raw one. Untraced is the
     * common case and stays a plain `fetch` with no Effect runtime around it.
     */
    const callEgress = (parent: O.Option<Tracer.ExternalSpan>): Promise<Settlement> =>
      O.map(parent, (span) =>
        runInContext(
          Effect.promise(rawCall).pipe(
            Effect.tap((outcome) =>
              Effect.annotateCurrentSpan({ "egress.settlement": outcome }),
            ),
            Effect.withSpan("work.call", {
              attributes: {
                "egress.api_id": cfg.apiId,
                "egress.path": cfg.apiPath,
                "egress.daemon": cfg.instanceId,
              },
            }),
            Effect.withParentSpan(span),
          ),
        ),
      ).pipe(O.getOrElse(rawCall));

    /**
     * Unreadable messages preserved before the rest are let go. Control events fan
     * out to every daemon, so a version skew would otherwise flood one dead-letter
     * queue at the full event rate. `egress_daemon_undecodable_total` counts past
     * the bound, so the rate stays visible.
     */
    const UNDECODABLE_SAMPLE = 20;

    /** Preserve this one if we are still sampling; otherwise let it go, loudly, once. */
    const sampleUnreadable = (what: string): Settlement => {
      counts.undecodable++;
      if (counts.undecodable <= UNDECODABLE_SAMPLE) {
        forkInContext(Effect.logWarning(`${label}: ${what}, dead-lettered`));
        return "discard";
      }
      if (counts.undecodable === UNDECODABLE_SAMPLE + 1) {
        forkInContext(
          Effect.logWarning(
            `${label}: ${UNDECODABLE_SAMPLE} unreadable messages already preserved on ` +
              `${deadQueue} — accepting further ones rather than flooding it; ` +
              `egress_daemon_undecodable_total still counts them all`,
          ),
        );
      }
      return "accept";
    };

    /** Serializes reconciliation: two callbacks could otherwise both see "no work consumer". */
    const gate = yield* Semaphore.make(1);

    /** Open a work-queue consumer on its own channel. */
    const startWork = Effect.gen(function* () {
      const consumer = yield* control.consume(
        workQueue,
        (_body, delivery) => callEgress(delivery.parent),
        // The whole concurrency limit, expressed once, where it can actually
        // stop the flow: the broker will not push a `maxInFlight + 1`th
        // delivery until this daemon settles one.
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

/** Make the channels match the state. `desired` and `plan` decide; this only acts. */
    const reconcile = gate.withPermit(
      Effect.gen(function* () {
        const have = {
          work: O.isSome(yield* Ref.get(workConsumer)),
          probe: O.isSome(yield* Ref.get(probeConsumer)),
          redrive: O.isSome(yield* Ref.get(redriveConsumer)),
        };
        const actions = plan(desired(yield* Ref.get(state), self()), have);
        if (actions.startWork) yield* startWork;
        if (actions.stopWork) yield* retire(workConsumer);
        if (actions.stopProbe) yield* retire(probeConsumer);
        if (actions.stopRedrive) yield* retire(redriveConsumer);
      }),
    );

    /** One message, one call, on a channel that exists only for this probe. */
    const probeOnce = gate.withPermit(
      Effect.gen(function* () {
        if (O.isSome(yield* Ref.get(probeConsumer))) return;

        // Cancel from inside the handler to stop at the first message, and cancel
        // rather than close: the channel must outlive it long enough to settle.
        let self: Consumer | null = null;
        let taken = false;
        const consumer = yield* control.consume(
          workQueue,
          (_body, delivery) => {
            if (taken || self === null) return;
            taken = true;
            forkInContext(control.cancelConsumer(self));
            return callEgress(delivery.parent);
          },
          // The state whose contract is "exactly one call" asks for exactly one message.
          { prefetch: 1 },
        );
        self = consumer;

        counts.probed++;
        yield* Ref.set(probeConsumer, O.some(consumer));
        yield* Effect.log(`${label}: elected prober, taking one message`);
      }),
    );

    /** Dead-letter recovery — see Redrive.ts. The options are the coupling, made explicit. */
    const redriveOnce = makeRedrive({
      label,
      enabled: cfg.redriveOnClose,
      rmq: control,
      workQueue,
      deadQueue,
      maxPerPass: cfg.redriveMax,
      isClosed: Ref.get(state).pipe(Effect.map((s) => s.circuit === State.CLOSED)),
      onReplayed: () => {
        counts.redriven++;
      },
      consumer: redriveConsumer,
      gate,
    });

    // Published by every daemon, so a trigger still arrives when some are down.
    const trigger = yield* control.publisherToQueue(probeQueue);
    const redriveTrigger = yield* control.publisherToQueue(redriveQueue);

    /** One command in, one atomic transition out. `Ref.modify` because callers run concurrently. */
    const dispatch = (command: Command) =>
      Ref.modify(state, (prior) => {
        const { next, actions } = reduce(prior, command, cfg.redriveOnClose);
        return [{ prior, next, actions }, next] as const;
      });

    /** The shell half of the reducer: what an Action actually does. */
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
      const { circuit, policy } = yield* Ref.get(state);
      const active = O.isSome(yield* Ref.get(workConsumer));
      return (
        `${circuit} target=${Math.round(policy.fraction * 100)}%${policy.floor ? "+floor" : ""} ` +
        `self=${active ? "ACTIVE" : "idle"}${floorHeld() ? " (floor)" : ""} ` +
        `calls ok=${counts.ok} failed=${counts.failed} inFlight=${inFlight} ` +
        `control=${[...counts.byType.values()].reduce((a, b) => a + b, 0)} ` +
        `gaps=${contract.gaps} dup=${contract.duplicates}`
      );
    });

    /** Order matters: apply, reconcile, log, and only then publish the triggers. */
    const applyEvent = (circuitState: State, sequence: number, reason: string) =>
      Effect.gen(function* () {
        const at = yield* Clock.currentTimeMillis;
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

    yield* control.consume(controlQueue, (body) =>
      Result.match(decodeCircuitEvent(body), {
        // Never half-applied: rejected, so it lands on the dead-letter queue rather
        // than existing only as a log line. Bounded by UNDECODABLE_SAMPLE.
        onFailure: (why) =>
          sampleUnreadable(
            why === "malformed-json"
              ? "control message that is not JSON"
              : "control message that does not match the published schema",
          ),
        onSuccess: ({ data, type }) => {
          if (data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
          Tally.observed(counts, type);

          contract = observe(contract, type, data.sequence);

          forkInContext(
            applyEvent(data.state, data.sequence, data.reason).pipe(
              Effect.catchCause((cause) =>
                Effect.logError(`${label}: applying event failed`, cause),
              ),
            ),
          );
        },
      }),
    );

    // Nothing is read from the body: the delivery *is* the election result.
    yield* control.consume(floorQueueFor(cfg.apiId), () => {
      floorUntil = Date.now() + FLOOR_LEASE_MS;
    });

    /**
     * Both elections read their trigger the same way, through `ElectionTrigger`.
     * A duplicate for a transition already acted on produces no actions — that is
     * the reducer's job, not this handler's.
     */
    const onTrigger =
      (what: string, command: (sequence: number) => Command) => (body: string) => {
        Result.match(decodeElectionTrigger(body), {
          onFailure: (why) =>
            sampleUnreadable(
              why === "malformed-json"
                ? `${what} trigger that is not JSON`
                : `${what} trigger that does not match the schema`,
            ),
          onSuccess: ({ sequence }) =>
            forkInContext(
              dispatch(command(sequence)).pipe(
                Effect.flatMap(({ actions }) => performAll(actions)),
                Effect.catchCause((cause) => Effect.logError(`${label}: ${what} failed`, cause)),
              ),
            ),
        });
      };

    // Registered for the life of the process and idle almost all of it: SAC promotion
    // needs candidates already waiting when the active one dies.
    yield* control.consume(
      probeQueue,
      onTrigger("probe", (sequence) => ({ _tag: "ProbeTriggered", sequence })),
    );

    // The second election, identical in shape: one recovery means one replay.
    yield* control.consume(
      redriveQueue,
      onTrigger("redrive", (sequence) => ({ _tag: "RedriveTriggered", sequence })),
    );

    // CLOSED until told otherwise: a daemon starting mid-incident learns the real
    // state from the aggregator's next snapshot.
    yield* reconcile;
    yield* Effect.log(
      `${label}: up — position=${selfPosition.toFixed(3)} maxInFlight=${cfg.maxInFlight} ` +
        `egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} control=${controlQueue}`,
    );

    /**
     * Published from here rather than each call site: the message path is a plain
     * async function running a few hundred times a second, and a fiber per metric
     * write would be the most expensive thing in it.
     */
    // The instance id is a label, not only a log prefix. The fleet is one
    // scaled service discovered by DNS, so Prometheus's own `instance` is an
    // IP address — without this there is no way to tell from a dashboard which
    // daemon holds the floor, or which one stopped hearing the control plane.
    // It is also the container's hostname, which is what makes a daemon
    // identified here killable by name.
    const attrs = { apiId: cfg.apiId, daemon: cfg.instanceId };

    /** Advanced from the same snapshot the delta came from, never by re-reading — see Tally.ts. */
    let published = Tally.nothing;

    /**
     * Every counter this daemon publishes, and which field of a delta feeds it.
     * One table, two readers — the flush below and the zeroing at startup — so
     * a new counter is one edit and cannot be added to one of them only.
     */
    const counters = [
      ["ok", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "ok" })],
      ["failed", Metric.withAttributes(Telemetry.calls, { ...attrs, outcome: "failed" })],
      // Fed by the same field as calls{failed}, because the gap between the two is
      // informative: a rejection whose link has already gone is requeued rather
      // than dead-lettered, so dead_lettered trailing calls{failed} slightly is
      // work that was retried rather than work that was lost.
      ["failed", Metric.withAttributes(Telemetry.deadLettered, attrs)],
      ["probed", Metric.withAttributes(Telemetry.probes, attrs)],
      ["redriven", Metric.withAttributes(Telemetry.redriven, attrs)],
      ["undecodable", Metric.withAttributes(Telemetry.undecodable, attrs)],
      ["gaps", Metric.withAttributes(Telemetry.controlGaps, attrs)],
      ["duplicates", Metric.withAttributes(Telemetry.controlDuplicates, attrs)],
    ] as const;

    const flush = Effect.gen(function* () {
      const { circuit, policy } = yield* Ref.get(state);
      const active = O.isSome(yield* Ref.get(workConsumer));

      // Gauges are whatever it is now, so these are read live.
      yield* Effect.all(
        [
          Metric.update(Metric.withAttributes(Telemetry.circuitState, attrs), STATE_CODE[circuit]),
          Metric.update(Metric.withAttributes(Telemetry.targetFraction, attrs), policy.fraction),
          Metric.update(Metric.withAttributes(Telemetry.floorHeld, attrs), floorHeld() ? 1 : 0),
          Metric.update(Metric.withAttributes(Telemetry.selfActive, attrs), active ? 1 : 0),
          Metric.update(Metric.withAttributes(Telemetry.inFlight, attrs), inFlight),
        ],
        { discard: true },
      );

      // One reading, both uses, so everything below may suspend freely.
      const current = Tally.snapshot(counts, contract);
      const delta = Tally.since(published, current);
      published = current;

      yield* Effect.forEach(
        counters,
        ([field, metric]) =>
          delta[field] > 0 ? Metric.update(metric, delta[field]) : Effect.void,
        { discard: true },
      );

      yield* Effect.forEach(
        delta.byType,
        ([type, seen]) =>
          Metric.update(Metric.withAttributes(Telemetry.controlEvents, { ...attrs, type }), seen),
        { discard: true },
      );
    });

    /**
     * Zero every counter at startup so its series exists before anything happens to
     * it: a tile whose job is to sit at zero is useless if zero reads as "No data".
     */
    yield* Effect.forEach(counters, ([, metric]) => Metric.update(metric, 0), { discard: true });

    /**
     * A `Schedule`, not `forever(sleep >> act)` — the same reason the
     * aggregator's tick loop is one. A schedule is a value: `TestClock` can
     * drive it, and interrupting it is closing a scope rather than remembering
     * a handle. It also runs the first pass immediately, so the gauges carry
     * real numbers from startup instead of whatever the zeroing left for a
     * second.
     */
    yield* Effect.forkScoped(Effect.repeat(flush, Schedule.spaced(FLUSH_INTERVAL)));

    /**
     * The ramp advances on a clock, so something must look at the clock — otherwise
     * it would advance at whatever pace control messages happen to arrive. Only
     * while CLOSED, and only when the target actually changes.
     */
    const advanceRamp = Effect.gen(function* () {
      const at = yield* Clock.currentTimeMillis;
      const { prior, next } = yield* dispatch({ _tag: "RampTick", at });
      if (next.policy.fraction === prior.policy.fraction && next.policy.floor === prior.policy.floor) {
        return;
      }
      yield* reconcile;
      yield* Effect.log(
        `${label}: ramp ${Math.round(prior.policy.fraction * 100)}% -> ` +
          `${Math.round(next.policy.fraction * 100)}% ${yield* describe}`,
      );
    });

    yield* Effect.forkScoped(Effect.repeat(advanceRamp, Schedule.spaced(RAMP_INTERVAL)));

    // Independent of the control plane: without it, a daemon that has gone deaf
    // looks exactly like one whose circuit has not moved.
    yield* Effect.repeat(
      describe.pipe(Effect.flatMap((s) => Effect.log(`${label}: heartbeat ${s}`))),
      Schedule.spaced(HEARTBEAT_INTERVAL),
    );
  });
