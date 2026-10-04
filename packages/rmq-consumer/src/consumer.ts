import { carry, Rmq } from "@egress/rmq/Client.ts";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";
import * as Delay from "@egress/rmq/DelayedDelivery.ts";
import { deadLetterQueueFor, deadLetterQueueOptions, workQueueFor, workQueueOptions } from "@egress/rmq/WorkQueue.ts";
import {
  Array as Arr,
  Clock,
  Context,
  Effect,
  Metric,
  Option as O,
  Queue,
  Random,
  Ref,
  Result,
  Schedule,
  Semaphore
} from "effect";
import * as Breaker from "./Breaker.ts";
import { breakerFor, CurrentCaller, localPermit } from "./Dependency.ts";
import type { AnyDependency, Caller, Registration, Verdict } from "./Dependency.ts";
import * as Gate from "./Gate.ts";
import * as Limiter from "./Limiter.ts";
import { read } from "./Negotiation.ts";
import type { Negotiate, Unreadable } from "./Negotiation.ts";
import * as Permit from "./Permit.ts";
import * as Redrive from "./Redrive.ts";
import { settle } from "./Settle.ts";
import type { Settled } from "./Settle.ts";
import * as Telemetry from "./Telemetry.ts";

/**
 * One application: consumers, each draining its own `<key>.work`, and one breaker per dependency per process, gating
 * every consumer that lists it (Gate.ts). Replicas share only what the broker holds: a probe permit per dependency
 * (Permit.ts) and each consumer's redrive, run by the replica RabbitMQ elects (Redrive.ts).
 */

export type ConsumerSpec = {
  /** Names the consumer's queues: `<key>.work`, `.work.dead`, `.work.parked`, `.redrive-trigger`. */
  readonly key: string;
  readonly negotiate: Negotiate;
  /** The contract: what the negotiated parser produced, as a message, or `None`. */
  readonly decode: (input: unknown) => O.Option<unknown>;
  readonly action: (payload: unknown, metadata: DeliveryInfo) => Effect.Effect<unknown, unknown, any>;
  readonly dependencies: ReadonlyArray<AnyDependency>;
};

type ApplicationConfig = {
  /** Names the dependencies' queues (wake queues, permits) and prefixes every log line. */
  readonly name: string;
  /** Each consumer's concurrent actions, applied as its prefetch. */
  readonly maxInFlight: number;
  /** Names this replica's wake queues, so a token finds only this process. */
  readonly replicaId: string;
  /** Every dependency's breaker, except for what the dependency sets of its own. */
  readonly breaker: Breaker.BreakerConfig;
  /**
   * Adapt each consumer's concurrency to its dependencies' `throttled` answers. `None`: `throttled` is a failed
   * call like any other, and `maxInFlight` never moves.
   */
  readonly limit: O.Option<Limiter.LimiterConfig>;
  readonly consumers: ReadonlyArray<ConsumerSpec>;
};

/**
 * Longer than the client's connection-recovery budget (about five minutes), so a wake queue survives a reconnect
 * and is collected only once its replica is really gone. The queue only expires while it has no consumer.
 */
const WAKE_QUEUE_EXPIRES_MS = 600_000;

/**
 * How long a `throttled` answer keeps its concurrency slot before the message is released: the backoff the
 * dependency asked for. Jittered so replicas don't come back in lockstep.
 */
const throttleHold = Effect.flatMap(Random.nextBetween(100, 400), (ms) => Effect.sleep(ms));

/** A clock-driven redrive trigger: a message can dead-letter while every breaker stays closed. */
const REDRIVE_SWEEP = "30 seconds";

/** A breaker's state, as the Gates, the wrapped calls and the redrive read it. */
type Cell = { readonly registration: O.Option<Registration>; readonly phase: Breaker.Phase; };
type Cells = Record<string, Ref.Ref<Cell>>;

type Running = {
  readonly spec: ConsumerSpec;
  readonly gate: Gate.Gate;
  readonly triggerRedrive: Effect.Effect<void>;
};

const cellOf = (cells: Cells, d: AnyDependency) => cells[d.dependencyName]!;

export const runApplication = Effect.fnUntraced(function*(cfg: ApplicationConfig) {
  const rmq = yield* Rmq;
  yield* Delay.declare();

  const dependencies = Arr.dedupeWith(
    Arr.flatMap(cfg.consumers, (c) => c.dependencies),
    (a: AnyDependency, b: AnyDependency) => a.dependencyName === b.dependencyName
  );
  const cells: Cells = Object.fromEntries(
    yield* Effect.forEach(
      dependencies,
      (d) =>
        Effect.map(
          Ref.make<Cell>({ registration: O.none(), phase: "closed" }),
          (cell) => [d.dependencyName, cell] as const
        )
    )
  );
  const scopeOf = (d: AnyDependency) => `${cfg.name}.${d.dependencyName}`;
  const permits = yield* Effect.forEach(dependencies, (d) =>
    localPermit(
      Permit.take(scopeOf(d)).pipe(
        Effect.provideService(Rmq, rmq),
        Effect.orElseSucceed(() => O.none<Effect.Effect<void>>())
      )
    ));

  // Captured so the plain-async handlers (amqplib's callbacks, not Effect fibers) reach the application's
  // services, the broker, and each dependency's breaker.
  const captured = yield* Effect.context<Rmq>();
  const services = Arr.reduce(
    Arr.zip(dependencies, permits),
    captured as Context.Context<any>,
    (ctx, [d, takePermit]) =>
      Context.add(ctx, d.guard, {
        registration: Effect.map(Ref.get(cellOf(cells, d)), (c) => c.registration),
        takePermit
      })
  );

  const running = yield* Effect.forEach(cfg.consumers, (spec) => runConsumer(cfg, spec, cells, services));

  const superviseDependency = Effect.fnUntraced(function*(d: AnyDependency) {
    const name = d.dependencyName;
    const cell = cellOf(cells, d);
    const gates = Arr.filter(running, (r) => r.spec.dependencies.some((x) => x.dependencyName === name));
    // Concurrently: each Gate may have to drain its consumer's in-flight actions.
    const reconcile = Effect.forEach(gates, (r) => r.gate.reconcile, { concurrency: "unbounded", discard: true });

    // The open state is a message addressed to this replica, so the queue it comes back on belongs to it:
    // named by it, and collected by the broker when the replica is gone for good.
    const wakeQueue = `${scopeOf(d)}.breaker.wake.${cfg.replicaId}`;
    yield* rmq.declareQueue(wakeQueue, { args: { "x-expires": WAKE_QUEUE_EXPIRES_MS } });
    yield* Delay.receive(wakeQueue);
    const wakes = yield* Queue.unbounded<number>();
    yield* rmq.consume(
      wakeQueue,
      (_body, delivery) => {
        Queue.offerUnsafe(wakes, Number(delivery.properties.attempt));
        return "accept";
      },
      { prefetch: 1 }
    );
    yield* Permit.seed(scopeOf(d));

    const setRegistration = (registration: O.Option<Registration>) => Ref.update(cell, (c) => ({ ...c, registration }));
    const register = (registration: Registration) =>
      setRegistration(O.some(registration)).pipe(Effect.andThen(reconcile), Effect.as(registration));

    const breakerState = Metric.withAttributes(Telemetry.breakerState, { dependency: name });
    const breakerTrips = Metric.withAttributes(Telemetry.breakerTrips, { dependency: name });

    return yield* Breaker.supervise<Registration>(breakerFor(cfg.breaker, d), {
      subscribe: (report) => register({ phase: "closed", report }),
      probe: (verdict) => register({ phase: "half-open", verdict }),
      // Unregistering can only stop subscriptions, never start one: a failure here is a broken invariant.
      retire: () => setRegistration(O.none()).pipe(Effect.andThen(reconcile), Effect.orDie),
      hold: (seconds, attempt) =>
        Delay.sendDelayed(wakeQueue, seconds, "wake", { headers: { attempt: String(attempt) } }).pipe(
          Effect.provideService(Rmq, rmq),
          Effect.andThen(Effect.log(`${cfg.name}: breaker ${name} open for ${seconds}s (attempt ${attempt})`)),
          Effect.andThen(Queue.take(wakes))
        ),
      onPhase: (next) =>
        Ref.update(cell, (c) => ({ ...c, phase: next })).pipe(
          Effect.andThen(Metric.update(breakerState, Breaker.PHASE_CODE[next])),
          Effect.andThen(next === "open" ? Metric.update(breakerTrips, 1) : Effect.void),
          Effect.andThen(Effect.log(`${cfg.name}: breaker ${name} ${next}`)),
          // Closing (startup included) is when "the outage may be over" first becomes true here.
          Effect.andThen(
            next === "closed" ? Effect.forEach(gates, (r) => r.triggerRedrive, { discard: true }) : Effect.void
          )
        )
    }, d.breakerPolicy);
  });

  const describeBreaker = (d: AnyDependency) => {
    const b = breakerFor(cfg.breaker, d);
    return `${d.dependencyName}:${b.initialDelaySeconds}-${b.maxDelaySeconds}s`;
  };
  yield* Effect.log(
    `${cfg.name}: up — consumers=${
      running.map((r) => `${r.spec.key}[${r.spec.dependencies.map((d) => d.dependencyName).join(",")}]`).join(",")
    } ` +
      `maxInFlight=${cfg.maxInFlight} wake=${cfg.name}.<dependency>.breaker.wake.${cfg.replicaId} ` +
      `breakers=${dependencies.map(describeBreaker).join(",")} ` +
      `limit=${O.match(cfg.limit, { onNone: () => "off", onSome: (l) => `${l.min}-${l.max} x${l.decrease}` })}`
  );

  // Runs for the process's life: the phases repeat, and it ends only if the broker fails an operation a
  // breaker cannot do without.
  return yield* Effect.forEach(dependencies, superviseDependency, { concurrency: "unbounded", discard: true });
});

const runConsumer = Effect.fnUntraced(function*(
  cfg: ApplicationConfig,
  spec: ConsumerSpec,
  cells: Cells,
  services: Context.Context<any>
) {
  const rmq = yield* Rmq;
  const log = `${cfg.name}/${spec.key}`;
  const runInContext = Effect.runPromiseWith(services);
  const attributes = { consumer: spec.key };

  const workQueue = workQueueFor(spec.key);
  yield* rmq.declareQueue(deadLetterQueueFor(spec.key), deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(spec.key));
  // Every replica declares the parked queue, even those never elected: every process that might touch a
  // queue has to agree on its arguments.
  yield* rmq.declareQueue(Redrive.parkedQueueFor(spec.key), Redrive.parkedQueueOptions());
  const parkedPub = yield* rmq.publisherToQueue(Redrive.parkedQueueFor(spec.key));

  const isClosed = Effect.map(
    Effect.forEach(spec.dependencies, (d) => Ref.get(cellOf(cells, d))),
    Arr.every((c) => c.phase === "closed")
  );

  const redriveQueue = Redrive.redriveTriggerQueueFor(spec.key);
  yield* rmq.declareQueue(redriveQueue, Redrive.redriveTriggerQueueOptions());
  const redriveTriggerPub = yield* rmq.publisherToQueue(redriveQueue);
  const triggerRedrive = rmq.send(redriveTriggerPub, "redrive").pipe(
    Effect.catch((err) => Effect.logWarning(`${log}: redrive trigger publish failed`, err))
  );
  // Two triggers close together must not start overlapping passes; one that arrives mid-pass is dropped.
  const redriving = yield* Ref.make(false);
  const redrivePass = Ref.modify(redriving, (running) => [running, true] as const).pipe(
    Effect.flatMap((running) =>
      running
        ? Effect.void
        : Redrive.runPass({
          apiId: spec.key,
          // This replica's own view, not the fleet's: see README.md's "Limits it accepts".
          isClosed,
          onOutcome: (outcome) =>
            Metric.update(Metric.withAttributes(Telemetry.redrives, { ...attributes, outcome }), 1)
        }).pipe(Effect.ensuring(Ref.set(redriving, false)))
    ),
    Effect.catch((err) => Effect.logWarning(`${log}: redrive pass failed`, err))
  );
  // Only the consumer RabbitMQ has made active (`x-single-active-consumer`) receives anything here.
  yield* rmq.consume(redriveQueue, () => {
    Effect.runForkWith(services)(redrivePass);
    return "accept";
  });
  yield* Effect.forkChild(Effect.repeat(Effect.when(triggerRedrive, isClosed), Schedule.spaced(REDRIVE_SWEEP)));

  // How many of the `maxInFlight` prefetched messages may be in an action at once. Without a `limit` config
  // the semaphore never resizes, so it admits exactly what the consumer's own prefetch already did.
  const limit = O.map(cfg.limit, (c) => new Limiter.AdaptiveLimit(c));
  const initialSlots = O.match(limit, { onNone: () => cfg.maxInFlight, onSome: (l) => l.slots });
  const slots = yield* Semaphore.make(initialSlots);
  const concurrencyLimit = Metric.withAttributes(Telemetry.concurrencyLimit, attributes);
  yield* Metric.update(concurrencyLimit, initialSlots);
  const adapt = (change: (l: Limiter.AdaptiveLimit) => void) =>
    Effect.suspend(() =>
      O.match(limit, {
        onNone: () => Effect.void,
        onSome: (l) => {
          const before = l.slots;
          change(l);
          return l.slots === before
            ? Effect.void
            : Semaphore.resize(slots, l.slots).pipe(Effect.andThen(Metric.update(concurrencyLimit, l.slots)));
        }
      })
    );

  // A `throttled` answer keeps its slot through the hold. Released first, the slot is free again the moment
  // the answer comes back and the next waiting message spends it on another `throttled`.
  const caller: Caller = {
    consumer: spec.key,
    throttling: O.isSome(cfg.limit),
    epoch: () => O.match(limit, { onNone: () => 0, onSome: (l) => l.epoch }),
    observe: (verdict: Verdict, startedIn: number) =>
      verdict.outcome === "ok"
        ? adapt((l) => l.succeeded())
        : verdict.outcome === "throttled"
        ? adapt((l) => l.throttled(startedIn)).pipe(Effect.andThen(throttleHold))
        : Effect.void
  };

  const inFlight = yield* Ref.make(0);
  const inFlightGauge = Metric.withAttributes(Telemetry.inFlight, attributes);
  const setInFlight = (delta: 1 | -1) =>
    Effect.flatMap(Ref.updateAndGet(inFlight, (n) => n + delta), (n) => Metric.update(inFlightGauge, n));

  // Logged at most once a second: a publisher that starts sending `gzip` by mistake, or a dependency that
  // starts refusing every message, would otherwise empty the queue into the parked queue without a trace.
  // The counters carry the volume.
  const lastLoggedAt = yield* Ref.make(0);
  const warnAtMostOncePerSecond = (message: () => string) =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) => Ref.modify(lastLoggedAt, (last) => (now - last >= 1000 ? [true, now] : [false, last]))),
      Effect.flatMap((due) => (due ? Effect.logWarning(message()) : Effect.void))
    );

  const declared = (o: O.Option<string>) => O.getOrElse(o, () => "none");

  /**
   * Poison goes straight to `work.parked`, never through the dead-letter queue: the redrive would replay it
   * `MAX_REDRIVES` times for the same answer. If the park itself fails, dead-lettering keeps it, and the
   * redrive parks it in the end.
   */
  const park = (body: Uint8Array, delivery: DeliveryInfo, reason: string): Effect.Effect<Settlement> =>
    rmq
      .send(parkedPub, body, carry(delivery, { [Redrive.PARKED_REASON_HEADER]: reason }))
      .pipe(
        Effect.as<Settlement>("accept"),
        Effect.orElseSucceed((): Settlement => "discard")
      );

  // A body this consumer cannot read, or that is not a message of its contract, was never published for it:
  // park it unread rather than spend the delivery budget on something no retry can fix.
  const unreadable = (reason: Unreadable, body: Uint8Array, delivery: DeliveryInfo) =>
    Metric.update(Metric.withAttributes(Telemetry.discarded, { ...attributes, reason }), 1).pipe(
      Effect.andThen(
        warnAtMostOncePerSecond(
          () =>
            `${log}: parking a ${reason} delivery — message_id ${declared(delivery.messageId)}, ` +
            `type ${declared(delivery.type)}, content-type ${declared(delivery.contentType)}, ` +
            `content-encoding ${declared(delivery.contentEncoding)}`
        )
      ),
      Effect.andThen(park(body, delivery, `unreadable-${reason}`))
    );

  const dispose = (body: Uint8Array, delivery: DeliveryInfo, settled: Settled): Effect.Effect<Settlement> =>
    settled.disposition === "park"
      ? warnAtMostOncePerSecond(() => `${log}: parking message_id ${declared(delivery.messageId)}: ${settled.reason}`)
        .pipe(
          Effect.andThen(park(body, delivery, settled.reason))
        )
      : settled.reason === "defect" || settled.reason === "unwrapped-error"
      ? warnAtMostOncePerSecond(
        () => `${log}: the action failed outside any dependency (${settled.reason}); requeueing`
      ).pipe(Effect.as(settled.disposition))
      : Effect.succeed(settled.disposition);

  // A span only for a delivery that carries a trace: the producer's sampler decides, never this process.
  const traced = (delivery: DeliveryInfo) => <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    O.match(delivery.parent, {
      onNone: () => effect,
      onSome: (parent) =>
        Effect.withSpan(effect, "work.process", {
          parent,
          attributes: {
            "messaging.system": "rabbitmq",
            "messaging.operation.name": "process",
            "messaging.destination.name": workQueue,
            "messaging.message.id": O.getOrUndefined(delivery.messageId)
          }
        })
    });

  const act = (payload: unknown, body: Uint8Array, delivery: DeliveryInfo) =>
    Semaphore.withPermit(
      slots,
      setInFlight(1).pipe(
        Effect.andThen(Effect.exit(Effect.suspend(() => spec.action(payload, delivery)).pipe(traced(delivery)))),
        Effect.ensuring(setInFlight(-1))
      )
    ).pipe(
      Effect.provideService(CurrentCaller, caller),
      Effect.flatMap((exit) => {
        // A defect is logged in full: `settle` only sees that there was one.
        const settled = settle(exit);
        return settled.reason === "defect"
          ? Effect.logError(`${log}: the action died`, exit).pipe(Effect.andThen(dispose(body, delivery, settled)))
          : dispose(body, delivery, settled);
      })
    );

  const readBody = read(spec.negotiate, spec.decode);
  const handle = (body: Uint8Array, delivery: DeliveryInfo): Effect.Effect<Settlement, never, any> =>
    Result.match(readBody(body, delivery), {
      onFailure: (reason) => unreadable(reason, body, delivery),
      onSuccess: (payload) => act(payload, body, delivery)
    });

  const gate = yield* Gate.make(
    Effect.forEach(spec.dependencies, (d) => Effect.map(Ref.get(cellOf(cells, d)), (c) => c.registration)),
    {
      // A probe is one message: prefetch 1 is the whole mechanism.
      subscribe: (mode) =>
        rmq.consume(workQueue, (body, delivery) => runInContext(handle(body, delivery)), {
          prefetch: mode === "probe" ? 1 : cfg.maxInFlight
        }),
      retire: rmq.drainConsumer
    }
  );

  return { spec, gate, triggerRedrive } satisfies Running;
});
