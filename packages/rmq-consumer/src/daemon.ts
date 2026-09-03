import { Effect, Exit, Option, Ref, Scope, Semaphore } from "effect";
import { makeRmq, Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  controlQueueFor,
  decodeCircuitEvent,
  probeTriggerQueueFor,
  routingKeyFor,
  workQueueFor,
} from "@egress/rmq/ControlPlane.ts";
import { State } from "@egress/domain/Model.ts";
import { activeIndices, initial, step } from "./DaemonPolicy.ts";
import type { Consumer, RmqConnectOptions } from "@egress/rmq/Client.ts";
import type { DaemonPolicyState } from "./DaemonPolicy.ts";

/**
 * One competing-consumer daemon: one process, one index in a fleet of
 * `fleetSize`. This is deliberately *not* a fleet simulator —
 * docker-compose.yml runs N of these as N separate containers, the same way
 * it runs envoy-00/01/02 and aggregator/aggregator-2, so the SAC election
 * below is contended by real separate processes rather than by fibers that
 * happen to share a runtime.
 *
 * Nothing here coordinates with the other daemons directly. Each one:
 *
 *  - subscribes to its *own* queue on `circuit.control`, bound to only its
 *    API's routing key, so every daemon sees the same event stream;
 *  - feeds each event through the pure `DaemonPolicy.step` to get a target
 *    active count, and starts or stops its own work consumption according to
 *    whether its index falls under that target (`activeIndices`);
 *  - registers on the SAC `probe-trigger` queue and waits, doing nothing
 *    until RabbitMQ promotes it.
 *
 * The whole fleet therefore converges on the same target from the same
 * events with no leader, no gossip and no shared state — the aggregator
 * already did the hard part of turning divergent replica views into one
 * agreed state, and this side just reads it.
 *
 * ## Two connections, on purpose
 *
 * The control plane runs on the connection from the `Rmq` layer, and that
 * connection *only ever opens links* — the control consumer, the SAC
 * consumer and the trigger publisher are created at startup and live for the
 * life of the process.
 *
 * Everything that churns — the work consumer, opened and closed on every
 * transition, and the one-message HALF_OPEN probe — runs on a second
 * connection this module opens and destroys itself. That is not tidiness.
 * Closing a consumer while the broker still has deliveries in flight for it
 * strands those deliveries, and enough of them stall every link on that
 * connection (see Client.ts's module doc for the measurement). A daemon
 * whose work consumer shared the control connection went deaf to
 * `circuit.control` after a handful of transitions and sat there looking
 * healthy — the failure this split exists to make impossible. Throwing the
 * work connection away is what returns the stranded capacity.
 *
 * ## Egress stays transparent
 *
 * Same invariant as infra/traffic-generator.mjs: one configured address, no
 * replica names, no admin ports. A daemon calls `${egressAddr}${apiPath}`
 * and never learns Envoy is a fleet — that topology is known to exactly one
 * thing in this repo, the aggregator's FleetSource.
 *
 * ## Backpressure, and where it actually comes from
 *
 * The work handler returns the egress call's promise, and `@egress/rmq`
 * accepts the message only once that settles. That is the whole flow-control
 * story: AMQP 1.0 replenishes credit on settlement, so a daemon holding
 * `maxInFlight` calls open stops settling, the broker stops pushing, and the
 * backlog stays where it belongs — in the queue, visible, rather than in a
 * process-local buffer or in a burst of concurrent requests at a service
 * that is already struggling.
 *
 * The first version of this file accepted every message on arrival and fired
 * the call afterwards. It looked fine and it was not: draining a 50k backlog
 * meant tens of thousands of concurrent calls from a single daemon, which is
 * precisely the herd the fleet-level policy is scaling daemons down to
 * avoid.
 *
 * A failed call still accepts. Rejecting would requeue the message straight
 * back into the outage, and the same argument applies — so this is
 * at-most-once with respect to failures, which a real deployment would
 * revisit (dead-letter, or a bounded redelivery budget) but which is right
 * for a demo about what the *fleet* does when a third party degrades.
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
};

export const runDaemon = (cfg: DaemonConfig) =>
  Effect.gen(function* () {
    const control = yield* Rmq;
    const label = `${cfg.apiId}/daemon-${cfg.index}`;

    const workQueue = workQueueFor(cfg.apiId);
    const probeQueue = probeTriggerQueueFor(cfg.apiId);
    const controlQueue = controlQueueFor(cfg.apiId, cfg.instanceId);

    // Every daemon declares the shared topology. These are all idempotent
    // declares of the same arguments, so N daemons racing to start is fine —
    // whoever gets there first wins and the rest are no-ops.
    const exchange = yield* control.declareTopicExchange(CONTROL_EXCHANGE);
    yield* control.declareQueue(workQueue);
    yield* control.declareQueue(probeQueue, { "x-single-active-consumer": true });
    const controlQ = yield* control.declareQueue(controlQueue);
    yield* control.bind(routingKeyFor(cfg.apiId), exchange, controlQ);

    const circuit = yield* Ref.make<State>(State.CLOSED);
    const policy = yield* Ref.make<DaemonPolicyState>(initial(cfg.fleetSize));
    /** The disposable work connection's scope — non-null exactly while this daemon is pulling work. */
    const workScope = yield* Ref.make<Scope.Closeable | null>(null);
    const probeScope = yield* Ref.make<Scope.Closeable | null>(null);
    /** Highest circuit sequence this daemon has already probed for — see the trigger handler. */
    const probedSequence = yield* Ref.make(-1);

    let inFlight = 0;
    let queued = 0;
    let ok = 0;
    let failed = 0;

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
    const callEgress = async () => {
      await acquire();
      try {
        const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
          signal: AbortSignal.timeout(2000),
        });
        if (res.ok) ok++;
        else failed++;
      } catch {
        // Connection refused / timeout once the cluster is fully ejected is
        // the expected shape of an outage, not an error to report here — the
        // aggregator is what judges the API's health, from Envoy's own view.
        failed++;
      } finally {
        release();
      }
    };

    /**
     * Reconciliation is serialized against itself: control events and probe
     * triggers both land on AMQP callbacks that fork into the runtime, so
     * without a permit two of them could each observe "no work connection"
     * and both open one.
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

    const reconcile = gate.withPermit(
      Effect.gen(function* () {
        const state = yield* Ref.get(circuit);
        const { targetActive } = yield* Ref.get(policy);

        // HALF_OPEN is the one state where a daemon must not act on its own
        // index. targetActive is 1, so index 0 would otherwise self-activate
        // and race whichever daemon SAC actually elected — two probes for a
        // state whose entire contract is "exactly one call". The prober is
        // chosen by the broker, below, not here.
        const shouldWork =
          state !== State.HALF_OPEN && activeIndices(targetActive, cfg.fleetSize).has(cfg.index);

        const running = (yield* Ref.get(workScope)) !== null;
        if (shouldWork && !running) yield* startWork;
        if (!shouldWork && running) yield* stopScope(workScope);

        // A probe connection only ever belongs to HALF_OPEN. Leaving the
        // state for any reason retires it, so it can never overlap the work
        // consumption CLOSED is about to start.
        if (state !== State.HALF_OPEN) yield* stopScope(probeScope);
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

        // Two closes, and the order is not interchangeable. Closing the
        // *consumer* from inside the handler is what stops delivery at the
        // first message; closing the *connection* is what returns the
        // deliveries that closing stranded. The connection close has to
        // happen outside the handler — tearing the connection down while
        // rhea is mid-way through a batch of transfer frames leaves the
        // remaining frames addressing a link that no longer exists, and it
        // throws `transfer after detach` from inside a socket callback. So
        // the connection is retired by `reconcile` when the state leaves
        // HALF_OPEN, never from here.
        let self: Consumer | null = null;
        let taken = false;
        const consumer = yield* probe.consume(workQueue, () => {
          if (taken || self === null) return;
          taken = true;
          Effect.runFork(probe.closeConsumer(self));
          return callEgress();
        });
        self = consumer;

        yield* Ref.set(probeScope, scope);
        yield* Effect.log(`${label}: elected prober, taking one message`);
      }),
    );

    // Published by *every* daemon on entering HALF_OPEN, not just one, so the
    // trigger still arrives when some daemons are down. SAC delivers all of
    // them to the single elected consumer, which dedupes on the circuit
    // sequence below — that is why the sequence travels in the body.
    const trigger = yield* control.publisherToQueue(probeQueue);

    const describe = Effect.gen(function* () {
      const state = yield* Ref.get(circuit);
      const { targetActive } = yield* Ref.get(policy);
      const active = (yield* Ref.get(workScope)) !== null;
      return (
        `${state} target=${targetActive}/${cfg.fleetSize} self=${active ? "ACTIVE" : "idle"} ` +
        `calls ok=${ok} failed=${failed} inFlight=${inFlight} queued=${queued}`
      );
    });

    const applyEvent = (state: State, sequence: number, reason: string) =>
      Effect.gen(function* () {
        const prior = yield* Ref.get(policy);
        yield* Ref.set(circuit, state);
        yield* Ref.set(policy, step(prior, state, cfg.fleetSize));
        yield* reconcile;
        yield* Effect.log(`${label}: seq=${sequence} (${reason}) ${yield* describe}`);
        if (state === State.HALF_OPEN) {
          yield* control.send(trigger, JSON.stringify({ sequence }));
        }
      });

    yield* control.consume(controlQueue, (body) => {
      const decoded = decodeCircuitEvent(body);
      if (Option.isNone(decoded)) {
        // Same stance as @egress/subscriber: an event that does not match the
        // published contract is dropped loudly, never half-applied.
        Effect.runFork(Effect.logWarning(`${label}: undecodable control message, dropped`));
        return;
      }
      const { data } = decoded.value;
      if (data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
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
        return;
      }
      Effect.runFork(
        Ref.get(probedSequence).pipe(
          Effect.flatMap((last) =>
            sequence <= last
              ? Effect.void // a duplicate trigger for a transition already probed
              : Ref.set(probedSequence, sequence).pipe(Effect.andThen(probeOnce)),
          ),
          Effect.catchCause((cause) => Effect.logError(`${label}: probe failed`, cause)),
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

    // A heartbeat independent of the control plane. Without it a daemon that
    // has gone deaf is indistinguishable from one whose circuit simply has
    // not moved — which is precisely how the stranded-delivery bug above hid
    // for as long as it did.
    yield* Effect.forever(
      Effect.sleep("15 seconds").pipe(
        Effect.andThen(describe),
        Effect.flatMap((s) => Effect.log(`${label}: heartbeat ${s}`)),
      ),
    );
  });
