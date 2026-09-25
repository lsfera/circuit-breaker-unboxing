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
  ATTEMPTS_HEADER,
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
  decodeWorkMessage,
  IDEMPOTENCY_KEY_HTTP_HEADER,
  ORIGIN_QUEUE_HEADER,
  ORIGIN_REASON_HEADER,
  parkedQueueFor,
  parkedQueueOptions,
  readsWorkFormat,
  probeTriggerQueueFor,
  REDRIVE_COUNT_HEADER,
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
import { makeRedrive } from "./Redrive.ts";
import { desired, initialState, plan, reduce } from "./DaemonState.ts";
import { position } from "./DaemonPolicy.ts";
import * as Attempts from "./Attempts.ts";
import * as Limiter from "./Limiter.ts";
import * as Tally from "./Tally.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Consumer, DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

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
const RAMP_INTERVAL = Duration.seconds(1);
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

  /**
   * The floor (ADR 013). A lease, not a flag: SAC promotes silently, so a
   * replacement learns it holds the floor from the next event, and the lease
   * stops a dead holder's claim outliving it.
   */
  const floorQ = yield* control.declareQueue(floorQueueFor(cfg.apiId), floorQueueOptions());
  yield* control.bind(routingKeyFor(cfg.apiId), exchange, floorQ);

  /** Everything decided, in one value (DaemonState.ts); `Ref.modify` keeps each transition atomic. */
  const now = yield* Clock.currentTimeMillis;
  const state = yield* Ref.make<DaemonState>(initialState(now));

  /** The churning channels — the "actual" side `plan` compares the desired shape against. */
  const workConsumer = yield* Ref.make(O.none<Consumer>());
  const probeConsumer = yield* Ref.make(O.none<Consumer>());
  const redriveConsumer = yield* Ref.make(O.none<Consumer>());
  /** Claimed atomically so two redrive passes never both start. */
  const redriveRunning = yield* Ref.make(false);

  /** A graph, not a limit: prefetch bounds it. */
  let inFlight = 0;

  const counts = Tally.zero();

  let contract: ContractState = initialContract;

  /**
   * `Date.now()`: refreshed from an AMQP callback and read synchronously. A minute
   * is four snapshot intervals, so it lapses only when the control plane is silent.
   */
  const FLOOR_LEASE_MS = 60_000;
  const selfPosition = position(cfg.instanceId);
  let floorUntil = 0;
  const floorHeld = () => Date.now() < floorUntil;
  const self = () => ({ position: selfPosition, isFloor: floorHeld() });

  const workPublisher = yield* control.publisherToQueue(workQueue, WORK_FORMAT);
  const deadPublisher = yield* control.publisherToQueue(deadQueue);
  const parkedPublisher = yield* control.publisherToQueue(parkedQueue);

  /** Jitter before releasing a 429, so a fleet shed together does not return together. Not tuned. */
  const SHED_BACKOFF_MIN_MS = 100;
  const SHED_BACKOFF_MAX_MS = 400;

  /**
   * Every AMQP callback runs its effects through these: a bare `Effect.run*` builds
   * a runtime with the no-op tracer and the default logger.
   */
  const services = yield* Effect.context<never>();
  const runInContext = Effect.runPromiseWith(services);
  const forkInContext = Effect.runForkWith(services);

  const limit = O.map(cfg.limit, (c) => new Limiter.AdaptiveLimit(c));
  const initialSlots = O.match(limit, { onNone: () => cfg.maxInFlight, onSome: (l) => l.slots });
  const slots = Semaphore.makeUnsafe(initialSlots);
  yield* Metric.update(Metric.withAttributes(Telemetry.concurrencyLimit, attrs), initialSlots);
  const resize = (to: number) =>
    Semaphore.resize(slots, to).pipe(
      Effect.andThen(Metric.update(Metric.withAttributes(Telemetry.concurrencyLimit, attrs), to)),
    );

  /** The key is the delivery's `message_id`, carried by every redelivery, retry and redrive. */
  const fetchStatus = async (key: string): Promise<number | "error"> => {
    inFlight++;
    try {
      const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
        signal: AbortSignal.timeout(2000),
        headers: { [IDEMPOTENCY_KEY_HTTP_HEADER]: key },
      });
      // An unconsumed body holds its connection out of the pool.
      await res.text().catch(() => {});
      return res.status;
    } catch {
      // The shape of an outage, not an error: the aggregator judges health from Envoy.
      return "error";
    } finally {
      inFlight--;
    }
  };

  /**
   * A delivery waiting for a slot stays unacked, so the limit only narrows what
   * prefetch allows. Moved once per round trip (Limiter.ts's `epoch`).
   */
  const limitedStatus = (key: string): Promise<number | "error"> =>
    O.match(limit, {
      onNone: () => fetchStatus(key),
      onSome: (l) =>
        runInContext(
          Semaphore.withPermit(
            slots,
            Effect.gen(function* () {
              const startedIn = l.epoch;
              const status = yield* Effect.promise(() => fetchStatus(key));
              const before = l.slots;
              yield* Effect.sync(() =>
                Match.value(Attempts.classify(status)).pipe(
                  Match.when("ok", () => l.succeeded()),
                  Match.when("shed", () => l.throttled(startedIn)),
                  Match.orElse(() => {}),
                ),
              );
              yield* Effect.when(resize(l.slots), Effect.sync(() => l.slots !== before));
              return status;
            }),
          ),
        ),
    });

  /**
   * Poison goes straight to the parked queue: through the dead-letter queue it came from the work queue, so the
   * redrive would replay it `MAX_REDRIVES` times for the same answer. `otherwise` is the settlement if the park fails.
   */
  const park = (body: string, messageId: O.Option<string>, reason: string, otherwise: Settlement): Promise<Settlement> =>
    runInContext(
      control.send(parkedPublisher, body, {
        messageId: O.getOrUndefined(messageId),
        headers: { [ORIGIN_QUEUE_HEADER]: workQueue, [ORIGIN_REASON_HEADER]: reason },
      }),
    ).then(
      (): Settlement => "accept",
      (): Settlement => otherwise,
    );

  const attempt = async (body: string, delivery: DeliveryInfo, key: string): Promise<Settlement> => {
    const status = await limitedStatus(key);
    const outcome = Attempts.classify(status);

    // Headers are only materialized when the call failed: the success path runs thousands of times a second.
    const decision = Attempts.nextAttempt(
      outcome,
      outcome === "failed" ? delivery.properties[ATTEMPTS_HEADER] : undefined,
    );
    return Match.valueTags(decision, {
      accept: async (): Promise<Settlement> => {
        counts.ok++;
        return "accept";
      },

      release: async (): Promise<Settlement> => {
        // Backpressure, not a failure. `release` does not spend x-delivery-limit;
        // `requeue` would, and a burst would dead-letter healthy work.
        counts.shed++;
        const jitter =
          SHED_BACKOFF_MIN_MS + Math.random() * (SHED_BACKOFF_MAX_MS - SHED_BACKOFF_MIN_MS);
        await new Promise((resolve) => setTimeout(resolve, jitter));
        return "release";
      },

      park: async (): Promise<Settlement> => {
        // Refused (4xx other than 408/429): a retry gets the same answer. Parked for a human.
        counts.refused++;
        return park(body, O.some(key), `refused-${status}`, "requeue");
      },

      republish: async (decision): Promise<Settlement> => {
        // Republished, never requeued: a requeue cannot carry the attempt count.
        counts.failed++;
        const toDead = decision.destination === "dead";
        const target = toDead ? deadPublisher : workPublisher;
        const destinationQueue = toDead ? deadQueue : workQueue;
        const headers: Record<string, string> = {
          [ATTEMPTS_HEADER]: String(decision.attempts),
          // Carried forward, or a redriven poison message would reset its count on every failure and never park.
          ...O.match(O.fromNullishOr(delivery.properties[REDRIVE_COUNT_HEADER]), {
            onNone: () => ({}),
            onSome: (count) => ({ [REDRIVE_COUNT_HEADER]: count }),
          }),
          // A direct publish has no x-first-death-*; the redrive attributes it by these.
          ...(toDead
            ? { [ORIGIN_QUEUE_HEADER]: workQueue, [ORIGIN_REASON_HEADER]: "attempts-exhausted" }
            : {}),
        };
        try {
          const send = control.send(target, body, { messageId: key, headers });
          await runInContext(
            O.map(delivery.parent, (span) =>
              send.pipe(
                Effect.withSpan("work.retry", {
                  attributes: {
                    "messaging.system": "rabbitmq",
                    "messaging.operation.name": "retry",
                    "messaging.destination.name": destinationQueue,
                    "egress.attempts": decision.attempts,
                  },
                }),
                Effect.withParentSpan(span),
              ),
            ).pipe(O.getOrElse(() => send)),
          );
        } catch {
          // x-delivery-limit is the backstop when a retry cannot be republished.
          return "requeue";
        }
        return "accept";
      },
    });
  };

  /** Never published by this fleet: parked unread, not retried or redriven. */
  const discard = (reason: "Format" | "Malformed" | "Keyless", body: string, delivery: DeliveryInfo): Promise<Settlement> => {
    counts[`discarded${reason}`]++;
    return park(body, delivery.messageId, `unreadable-${reason.toLowerCase()}`, "discard");
  };

  const call = (body: string, delivery: DeliveryInfo): Settlement | Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("Malformed", body, delivery),
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("Keyless", body, delivery),
              onSome: (key) => attempt(body, delivery, key),
            }),
        })
      : discard("Format", body, delivery);

  /** Traced only when the message carried a parent; the common case stays a plain call. */
  const callEgress = (body: string, delivery: DeliveryInfo): Settlement | Promise<Settlement> =>
    O.map(delivery.parent, (span) =>
      runInContext(
        Effect.promise(async () => call(body, delivery)).pipe(
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
    ).pipe(O.getOrElse(() => call(body, delivery)));

  /**
   * Control events fan out to every daemon, so a version skew would flood the
   * dead-letter queue. The first few are kept; the metric counts all.
   */
  const UNDECODABLE_SAMPLE = 20;

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

  const startWork = Effect.gen(function* () {
    const consumer = yield* control.consume(
      workQueue,
      (body, delivery) => callEgress(body, delivery),
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

      // Cancel, not close: the channel must outlive the cancel to settle the message.
      let self: Consumer | null = null;
      let taken = false;
      const consumer = yield* control.consume(
        workQueue,
        (body, delivery): Settlement | Promise<Settlement> => {
          // Released, never acked: returning nothing acks, and extra deliveries were lost that way.
          if (taken || self === null) return "release";
          taken = true;
          forkInContext(control.cancelConsumer(self));
          return callEgress(body, delivery);
        },
        { prefetch: 1 },
      );
      self = consumer;

      counts.probed++;
      yield* Ref.set(probeConsumer, O.some(consumer));
      yield* Effect.log(`${label}: elected prober, taking one message`);
    }),
  );

  const redriveOnce = makeRedrive({
    label,
    enabled: cfg.redriveOnClose,
    rmq: control,
    workQueue,
    deadQueue,
    parkedQueue,
    maxPerPass: cfg.redriveMax,
    isClosed: Ref.get(state).pipe(Effect.map((s) => s.circuit === State.CLOSED)),
    consumer: redriveConsumer,
    gate,
    running: redriveRunning,
  });

  // Published by every daemon, so a trigger still arrives when some are down.
  const trigger = yield* control.publisherToQueue(probeQueue);
  const redriveTrigger = yield* control.publisherToQueue(redriveQueue);

  /** One command in, one atomic transition out. `Ref.modify` because callers run concurrently. */
  const dispatch = (command: Command) =>
    Ref.modify(state, (prior) => {
      const transition = reduce(prior, command, cfg.redriveOnClose);
      return [{ prior, ...transition }, transition.next] as const;
    });

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
  const applyEvent = Effect.fnUntraced(function* (event: CircuitEvent) {
    const { state: circuitState, sequence, reason } = event.data;
    const at = yield* Clock.currentTimeMillis;
    const { ignored, actions } = yield* dispatch({
      _tag: "CircuitChanged",
      type: event.type,
      lease: O.fromUndefinedOr(event.data.lease),
      state: circuitState,
      sequence,
      at,
    });
    yield* ignored
      ? Effect.sync(() => void counts.stale++).pipe(
          Effect.andThen(Effect.logWarning(`${label}: seq=${sequence} (${reason}, ${circuitState}) is stale, ignored`)),
        )
      : reconcile.pipe(
          Effect.andThen(describe),
          Effect.flatMap((d) => Effect.log(`${label}: seq=${sequence} (${reason}) ${d}`)),
          Effect.andThen(performAll(actions)),
        );
  });

  yield* control.consume(controlQueue, (body) =>
    Result.match(decodeCircuitEvent(body), {
      onFailure: (why) =>
        sampleUnreadable(
          why === "malformed-json"
            ? "control message that is not JSON"
            : "control message that does not match the published schema",
        ),
      onSuccess: (event) => {
        const { data, type } = event;
        if (data.apiId !== cfg.apiId) return; // belt and braces; the binding already filters
        Tally.observed(counts, type);

        const before = contract;
        contract = observe(contract, type, O.fromUndefinedOr(data.lease), data.sequence);
        if (contract.gaps > before.gaps) {
          forkInContext(
            Effect.logWarning(
              `${label}: sequence gap — expected ${O.getOrElse(O.map(before.last, (l) => l.sequence + 1), () => 0)}, got ${data.sequence} (${type}, ${data.state}, ${data.reason})`,
            ),
          );
        }

        forkInContext(
          applyEvent(event).pipe(
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
   * Settled only after the action runs. On failure the sequence is un-marked and
   * the trigger requeued after a pause; if the daemon dies, SAC hands the unacked
   * trigger to the next. A redrive may hold it 20 minutes, inside RabbitMQ's
   * 30-minute delivery timeout.
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

  // CLOSED until the next snapshot says otherwise.
  yield* reconcile;
  yield* Effect.log(
    `${label}: up — position=${selfPosition.toFixed(3)} maxInFlight=${cfg.maxInFlight} ` +
      `limit=${O.match(cfg.limit, { onNone: () => "off", onSome: (l) => `${l.min}-${l.max} x${l.decrease}` })} ` +
      `egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} control=${controlQueue}`,
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
    const { circuit, policy } = yield* Ref.get(state);

    yield* Effect.all(
      [
        Metric.update(Metric.withAttributes(Telemetry.circuitState, attrs), STATE_CODE[circuit]),
        Metric.update(Metric.withAttributes(Telemetry.targetFraction, attrs), policy.fraction),
        Metric.update(Metric.withAttributes(Telemetry.floorHeld, attrs), floorHeld() ? 1 : 0),
      ],
      { discard: true },
    );

    const current = Tally.snapshot(counts, contract);
    const delta = Tally.since(published, current);
    published = current;

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

  /** The ramp is time-gated, so something must tick it. */
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

  /** The reducer keeps this to the floor and CLOSED; Redrive.ts makes it a no-op while a pass runs. */
  const sweep = Effect.gen(function* () {
    const { actions } = yield* dispatch({ _tag: "SweepTick", isFloor: floorHeld() });
    yield* performAll(actions);
  });

  yield* Effect.forkScoped(Effect.repeat(sweep, Schedule.spaced(SWEEP_INTERVAL)));

  // Without it a deaf daemon looks like one whose circuit has not moved.
  yield* Effect.repeat(
    describe.pipe(Effect.flatMap((s) => Effect.log(`${label}: heartbeat ${s}`))),
    Schedule.spaced(HEARTBEAT_INTERVAL),
  );
});
