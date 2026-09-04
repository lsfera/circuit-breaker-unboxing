import { Effect, Exit, Metric, Option, Ref, Scope, Semaphore } from "effect";
import { makeRmq, Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  controlQueueFor,
  deadLetterArgs,
  deadLetterQueueFor,
  decodeCircuitEvent,
  probeTriggerQueueFor,
  redriveTriggerQueueFor,
  routingKeyFor,
  sacQueueArgs,
  workQueueArgs,
  workQueueFor,
} from "@egress/rmq/ControlPlane.ts";
import { randomUUID } from "node:crypto";
import { State, STATE_CODE } from "@egress/domain/Model.ts";
import { activeIndices, initial, step } from "./DaemonPolicy.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Consumer, RmqConnectOptions, Settlement } from "@egress/rmq/Client.ts";
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
 * ## What happens to work that fails
 *
 * A failed call rejects its message, and the work queue is declared with a
 * dead-letter exchange, so it lands on `<apiId>.work.dead` where it can be
 * counted, inspected and replayed. That is the whole point: this repo proves
 * a delivery contract for control events, and it would be a strange kind of
 * rigour to prove that while silently dropping the payload work — which is
 * what accepting a failed message did.
 *
 * Preserving failed work is not the same as recovering it, though, and a
 * dead-letter queue nobody drains is just a slower way of losing things. So
 * there is an opt-in redrive (`REDRIVE_ON_CLOSE`): on the transition back to
 * `CLOSED`, one daemon — elected by the broker on a second SAC queue, the
 * same mechanism as the prober — replays the dead-lettered messages onto the
 * work queue and stops as soon as the queue is drained, a cap is reached, or
 * the circuit leaves `CLOSED` again. It is off by default because replaying
 * work is a policy decision about *this* workload, not a property of the
 * transport: whether a two-minute-old payment attempt should be retried at
 * all is the sort of question a queue cannot answer for you.
 *
 * There is deliberately no retry. The client's `requeue` sends
 * `modified{delivery_failed: false}`, and RabbitMQ only increments AMQP
 * 1.0's `delivery-count` for a delivery marked *failed* — so a released
 * message comes back indistinguishable from a new one, forever, and a
 * redelivery budget that survives the message moving to another daemon
 * cannot be expressed. Verified against a real broker, and pinned by
 * `@egress/rmq`'s integration tests so that a client release which fixes it
 * turns the test red. One attempt then dead-letter is the honest policy
 * given that, not a shortcut around it.
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
    const exchange = yield* control.declareTopicExchange(CONTROL_EXCHANGE);
    // The dead-letter queue is declared before the queue that points at it,
    // so a rejection during the first seconds of the fleet's life has
    // somewhere to land rather than being discarded by the broker.
    // The dead-letter queue is the only one without a dead-letter target of
    // its own: it is the end of the line, and pointing it at itself is a
    // cycle. Everything else routes rejections to it — see deadLetterArgs.
    yield* control.declareQueue(deadQueue);
    yield* control.declareQueue(workQueue, workQueueArgs(cfg.apiId));
    yield* control.declareQueue(probeQueue, sacQueueArgs(cfg.apiId));
    yield* control.declareQueue(redriveQueue, sacQueueArgs(cfg.apiId));
    const controlQ = yield* control.declareQueue(controlQueue, deadLetterArgs(cfg.apiId));
    yield* control.bind(routingKeyFor(cfg.apiId), exchange, controlQ);

    const circuit = yield* Ref.make<State>(State.CLOSED);
    const policy = yield* Ref.make<DaemonPolicyState>(initial(cfg.fleetSize));
    /** The disposable work connection's scope — non-null exactly while this daemon is pulling work. */
    const workScope = yield* Ref.make<Scope.Closeable | null>(null);
    const probeScope = yield* Ref.make<Scope.Closeable | null>(null);
    const redriveScope = yield* Ref.make<Scope.Closeable | null>(null);
    /** Highest circuit sequence this daemon has already probed for — see the trigger handler. */
    const probedSequence = yield* Ref.make(-1);
    /** Same idea for the redrive election: one replay per recovery, not one per trigger message. */
    const redrivenSequence = yield* Ref.make(-1);

    let inFlight = 0;
    let queued = 0;
    let ok = 0;
    let failed = 0;
    let probed = 0;
    let redriven = 0;
    /** Messages this daemon could not read at all, on any of its queues, and rejected onto the canonical dead-letter queue. */
    let undecodable = 0;

    /**
     * The delivery contract, observed from this side of the broker.
     *
     * `-1` until the first state_changed arrives: a daemon that starts
     * mid-incident legitimately joins the sequence part-way through, and
     * calling that a gap would make the metric lie on every restart.
     */
    let lastSequence = -1;
    let gaps = 0;
    let duplicates = 0;
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
        if (res.ok) {
          ok++;
          return "accept";
        }
        failed++;
        return "discard";
      } catch {
        // Connection refused / timeout once the cluster is fully ejected is
        // the expected shape of an outage, not an error to report here — the
        // aggregator is what judges the API's health, from Envoy's own view.
        // The message still goes to the dead-letter queue rather than being
        // accepted, so the work is recoverable even though the call is lost.
        failed++;
        return "discard";
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

        // A redrive belongs to CLOSED and nothing else. Leaving that state
        // for any reason retires it immediately: replaying a backlog into an
        // upstream that has just started failing again is the one thing this
        // whole design exists to prevent.
        if (state !== State.CLOSED) yield* stopScope(redriveScope);
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

        probed++;
        yield* Ref.set(probeScope, scope);
        yield* Effect.log(`${label}: elected prober, taking one message`);
      }),
    );

    /**
     * One bounded pass: move at most `redriveMax` messages from the
     * dead-letter queue back onto the work queue, then stop. Returns why it
     * stopped, which is what tells the caller whether another pass is worth
     * running.
     *
     * Three properties worth stating, because each is a decision:
     *
     *  - **Its own connection**, like the probe, and for the same measured
     *    reason: this closes a consumer with a backlog behind it, which is
     *    what strands deliveries and eventually stalls every link on a shared
     *    connection. The connection is retired from outside the handler.
     *  - **Publish, then accept.** A crash between the two redelivers a
     *    message that was already replayed, which is a duplicate; accepting
     *    first would lose it. Duplicates are recoverable and losses are not,
     *    and the whole point of this queue is that the work still matters.
     *  - **It stops on its own.** Whichever comes first: the cap, the queue
     *    running dry, the circuit leaving CLOSED, or a hard deadline. A
     *    redrive that cannot end is a worse failure mode than a queue that
     *    does not drain.
     */
    /** Stamped onto anything the redrive moves within the dead-letter queue, so its origin survives the republish that loses the broker's own annotations. */
    const ORIGIN_PROPERTY = "x-egress-origin-queue";
    const ORIGIN_REASON_PROPERTY = "x-egress-origin-reason";
    const ORIGIN_PASS_PROPERTY = "x-egress-redrive-pass";

    const redrivePass = Effect.gen(function* () {
      const scope = yield* Scope.make();
      const conn = yield* Effect.provideService(makeRmq(cfg.connect), Scope.Scope, scope);
      const into = yield* conn.publisherToQueue(workQueue);
      const back = yield* conn.publisherToQueue(deadQueue);

      const passId = randomUUID();
      let moved = 0;
      let parked = 0;
      let cycled = false;
      let lastReplayAt = Date.now();
      yield* conn.consume(deadQueue, async (body, delivery): Promise<Settlement> => {
        // One canonical dead-letter queue means this one holds more than
        // failed work: a control event that would not decode lands here too,
        // and replaying *that* onto the work queue would be nonsense. The
        // broker records where each message was dead-lettered from, so the
        // filter is exact rather than a guess at the body's shape.

        // Our own stamp, from *this* pass: the queue has come the whole way
        // round and everything left is stuff this pass will not replay.
        // Without that signal the pass re-parks the same handful of messages
        // tail to tail as fast as the broker can deliver them — measured at
        // 17,703 republishes of two messages in 2.5 seconds before an idle
        // timer eventually noticed. A stamp from an *older* pass means only
        // "something already decided this is not work", and must be moved on
        // rather than ending the lap: otherwise one parked message sitting at
        // the head makes every later redrive give up before replaying
        // anything, which is the opposite of self-healing.
        if (delivery.properties[ORIGIN_PASS_PROPERTY] === passId) {
          cycled = true;
          return "requeue";
        }

        // Where it came from: the broker's annotation on first sight, our own
        // stamp once an earlier pass moved it, and "unknown" for anything
        // published straight onto this queue by something else. Only work is
        // ever replayed, so anything unattributable is kept, not guessed at.
        const originQueue =
          delivery.deadLetter?.queue ?? delivery.properties[ORIGIN_PROPERTY] ?? "unknown";
        const originReason =
          delivery.deadLetter?.reason ?? delivery.properties[ORIGIN_REASON_PROPERTY] ?? "unknown";

        if (originQueue !== workQueue) {
          // Moved to the tail rather than released, because releasing puts it
          // straight back at the head and starves everything behind it, and
          // stamped on the way so the provenance the annotations carried is
          // not lost with them.
          parked++;
          try {
            await Effect.runPromise(
              conn.send(back, body, {
                [ORIGIN_PROPERTY]: originQueue,
                [ORIGIN_REASON_PROPERTY]: originReason,
                [ORIGIN_PASS_PROPERTY]: passId,
              }),
            );
            return "accept";
          } catch {
            return "requeue";
          }
        }

        // Reserve the slot *before* awaiting. The broker delivers with a
        // credit window in the hundreds, so a check-then-await-then-increment
        // lets every in-flight handler pass the same check and overshoot the
        // cap by an order of magnitude — measured at 5739 against a cap of
        // 5000 before this was reordered.
        if (moved >= cfg.redriveMax) return "requeue";
        moved++;
        try {
          await Effect.runPromise(conn.send(into, body));
        } catch {
          // The work queue is unreachable; leave the message where it is
          // rather than accepting it into nothing.
          moved--;
          return "requeue";
        }
        redriven++;
        lastReplayAt = Date.now();
        return "accept";
      });

      yield* gate.withPermit(Ref.set(redriveScope, scope));

      const deadline = Date.now() + 60_000;
      let reason = "deadline";
      while (true) {
        yield* Effect.sleep("200 millis");
        if ((yield* Ref.get(circuit)) !== State.CLOSED) {
          reason = "circuit reopened";
          break;
        }
        if (moved >= cfg.redriveMax) {
          reason = "cap reached";
          break;
        }
        if (cycled) {
          reason = "came full circle";
          break;
        }
        // Idle is measured on *replays* rather than on deliveries, so a pass
        // that is only being handed things it will not replay still ends.
        if (Date.now() - lastReplayAt > 2000) {
          reason = parked > 0 ? "nothing left to replay" : "drained";
          break;
        }
        if (Date.now() > deadline) break;
      }

      // Close *this pass's* scope, and only clear the Ref if it still points
      // at it. Closing whatever the Ref happens to hold is not the same
      // thing: reconcile retires the scope on any state change, so a pass
      // whose scope had already been retired and replaced by a newer one
      // would tear down the newer pass's live connection on its way out.
      yield* gate.withPermit(Ref.update(redriveScope, (s) => (s === scope ? null : s)));
      yield* Scope.close(scope, Exit.void);

      if (parked > 0) {
        yield* Effect.logWarning(
          `${label}: left ${parked} non-work message(s) on ${deadQueue} — ` +
            `dead-lettered from somewhere other than ${workQueue}, so not replayed as work`,
        );
      }
      return { moved, parked, reason };
    });
    /**
     * Replay the dead-letter queue, in bounded passes, until it is empty or
     * something says stop. Runs on exactly one daemon — the broker elects it
     * on a second SAC queue, below — and only while the circuit is CLOSED.
     *
     * Passes rather than one long drain because each pass is a fresh
     * connection it can afford to destroy, and because `redriveMax` is there
     * to keep any single burst onto the work queue bounded. Looping until
     * drained is what makes this actually self-healing: a backlog larger than
     * the cap would otherwise need one outage per 5,000 messages to recover.
     */
    const REDRIVE_MAX_PASSES = 20;
    const redriveOnce = Effect.gen(function* () {
      if (!cfg.redriveOnClose) return;
      if ((yield* gate.withPermit(Ref.get(redriveScope))) !== null) return;

      yield* Effect.log(`${label}: redriving ${deadQueue} (max ${cfg.redriveMax} per pass)`);
      let total = 0;
      for (let pass = 1; pass <= REDRIVE_MAX_PASSES; pass++) {
        const { moved, reason } = yield* redrivePass;
        total += moved;
        // A pass that replayed nothing means whatever is left is not work,
        // so more passes would only cycle it.
        if (reason !== "cap reached" || moved === 0) {
          yield* Effect.log(`${label}: redrive finished — ${total} replayed (${reason})`);
          return;
        }
      }
      yield* Effect.log(
        `${label}: redrive stopped after ${REDRIVE_MAX_PASSES} passes — ${total} replayed; ` +
          `whatever is left will be picked up by the next recovery`,
      );
    });

    // Published by *every* daemon on entering HALF_OPEN, not just one, so the
    // trigger still arrives when some daemons are down. SAC delivers all of
    // them to the single elected consumer, which dedupes on the circuit
    // sequence below — that is why the sequence travels in the body.
    const trigger = yield* control.publisherToQueue(probeQueue);
    const redriveTrigger = yield* control.publisherToQueue(redriveQueue);

    const describe = Effect.gen(function* () {
      const state = yield* Ref.get(circuit);
      const { targetActive } = yield* Ref.get(policy);
      const active = (yield* Ref.get(workScope)) !== null;
      return (
        `${state} target=${targetActive}/${cfg.fleetSize} self=${active ? "ACTIVE" : "idle"} ` +
        `calls ok=${ok} failed=${failed} inFlight=${inFlight} queued=${queued} ` +
        `control=${[...eventsByType.values()].reduce((a, b) => a + b, 0)} gaps=${gaps} dup=${duplicates}`
      );
    });

    const applyEvent = (state: State, sequence: number, reason: string) =>
      Effect.gen(function* () {
        const prior = yield* Ref.get(policy);
        const priorState = yield* Ref.get(circuit);
        yield* Ref.set(circuit, state);
        yield* Ref.set(policy, step(prior, state, cfg.fleetSize));
        yield* reconcile;
        yield* Effect.log(`${label}: seq=${sequence} (${reason}) ${yield* describe}`);
        if (state === State.HALF_OPEN) {
          yield* control.send(trigger, JSON.stringify({ sequence }));
        }
        // Only on the actual transition back into CLOSED — the aggregator's
        // periodic snapshots repeat the current state, and a redrive per
        // snapshot would replay the queue every fifteen seconds forever.
        if (cfg.redriveOnClose && state === State.CLOSED && priorState !== State.CLOSED) {
          yield* control.send(redriveTrigger, JSON.stringify({ sequence }));
        }
      });

    yield* control.consume(controlQueue, (body) => {
      const decoded = decodeCircuitEvent(body);
      if (Option.isNone(decoded)) {
        // Same stance as @egress/subscriber: an event that does not match the
        // published contract is never half-applied. It is rejected rather
        // than accepted, so it lands on the canonical dead-letter queue
        // instead of existing only as a log line nobody can act on — a
        // control message the fleet could not read is precisely the thing
        // you want to still have in your hands afterwards.
        undecodable++;
        Effect.runFork(
          Effect.logWarning(`${label}: undecodable control message, dead-lettered`),
        );
        return "discard";
      }
      const { data, type } = decoded.value;
      if (data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
      eventsByType.set(type, (eventsByType.get(type) ?? 0) + 1);

      // Snapshots deliberately repeat the current sequence, so only
      // state_changed carries the gapless guarantee — the same rule
      // @egress/aggregator's own Integrity tracker applies to the webhook
      // stream. Checking it here proves it a second time, over a different
      // transport, from a process the publisher does not control.
      if (type === "egress.circuit.state_changed") {
        if (lastSequence >= 0) {
          if (data.sequence <= lastSequence) duplicates++;
          else if (data.sequence > lastSequence + 1) gaps++;
        }
        lastSequence = Math.max(lastSequence, data.sequence);
      }

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
        undecodable++;
        return "discard";
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

    // The second SAC election, identical in shape to the prober's: every
    // daemon publishes the trigger so it still arrives when some are down,
    // the broker delivers all of them to one consumer, and that consumer
    // dedupes on the circuit sequence so one recovery means one replay.
    yield* control.consume(redriveQueue, (body) => {
      let sequence = -1;
      try {
        sequence = Number(JSON.parse(body).sequence ?? -1);
      } catch {
        undecodable++;
        return "discard";
      }
      Effect.runFork(
        Ref.get(redrivenSequence).pipe(
          Effect.flatMap((last) =>
            sequence <= last
              ? Effect.void
              : Ref.set(redrivenSequence, sequence).pipe(Effect.andThen(redriveOnce)),
          ),
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
      const state = yield* Ref.get(circuit);
      const { targetActive } = yield* Ref.get(policy);
      const active = (yield* Ref.get(workScope)) !== null;

      yield* Effect.all(
        [
          Metric.update(Metric.withAttributes(Telemetry.circuitState, attrs), STATE_CODE[state]),
          Metric.update(Metric.withAttributes(Telemetry.targetActive, attrs), targetActive),
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
        gaps: gaps - flushed.gaps,
        duplicates: duplicates - flushed.duplicates,
      };
      flushed = { ok, failed, probed, redriven, undecodable, gaps, duplicates };

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
