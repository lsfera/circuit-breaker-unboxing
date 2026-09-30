import { Duration, Effect, Option as O, Ref, Schedule } from "effect";
import {
  MAX_REDRIVES,
  WORK_CONTENT_TYPE,
  WORK_MESSAGE_TYPE,
  ORIGIN_QUEUE_HEADER,
  ORIGIN_REASON_HEADER,
  REDRIVE_COUNT_HEADER,
} from "@egress/rmq/ControlPlane.ts";
import type { Consumer, DeliveryInfo, RmqError, RmqService, Settlement } from "@egress/rmq/Client.ts";
import type { Semaphore } from "effect/Semaphore";

/**
 * Where a redriven work message goes next. A header that does not parse counts
 * as 0: trusting it risks a poison message that redrives for ever.
 */
export type RedriveDecision =
  | { readonly destination: "work"; readonly count: number }
  | { readonly destination: "parked" };

export const nextRedrive = (header: string | undefined): RedriveDecision => {
  const parsed = header === undefined ? 0 : Number(header);
  const count = (Number.isFinite(parsed) ? parsed : 0) + 1;
  return count > MAX_REDRIVES ? { destination: "parked" } : { destination: "work", count };
};

/**
 * Bounded passes that replay work from the dead-letter queue and park anything
 * that is not work. One daemon runs it (elected, or the floor's sweep): five
 * replaying one backlog would be a fivefold burst at a recovering upstream.
 */
type RedriveOptions = {
  readonly label: string;
  readonly enabled: boolean;
  /** The daemon's one connection. A pass opens a channel on it and closes that. */
  readonly rmq: RmqService;
  readonly workQueue: string;
  readonly deadQueue: string;
  /** Where a message goes once MAX_REDRIVES is exhausted — see ControlPlane.ts. */
  readonly parkedQueue: string;
  /** Ceiling on messages moved in a single pass, so a huge backlog is recovered in bounded bites. */
  readonly maxPerPass: number;
  /** Read fresh each time round: a pass must stop as soon as the circuit leaves CLOSED. */
  readonly isClosed: Effect.Effect<boolean>;
  /** Shared with `reconcile`, which retires the channel when the state changes. */
  readonly consumer: Ref.Ref<O.Option<Consumer>>;
  readonly gate: Semaphore;
  /**
   * Claimed with `Ref.modify`, not inferred from `consumer`, which is only set
   * after `consume` returns — two callers in that gap would both open a consumer.
   */
  readonly running: Ref.Ref<boolean>;
};

/** A pass is bounded three ways: how often it looks, how long it tolerates no replays, and how long it may run at all. */
const PASS_POLL = Duration.millis(200);
const PASS_IDLE_MS = 2000;
const PASS_DEADLINE = Duration.seconds(60);
/** How a finished pass waits for its last publishes to be confirmed before closing the channel. */
const SETTLE_POLL = Duration.millis(20);
const SETTLE_DEADLINE = Duration.seconds(10);

/** Passes until drained, capped, so a backlog larger than one pass needs no second outage. */
const REDRIVE_MAX_PASSES = 20;

/**
 * The longest one redrive can hold its election trigger unacked: every pass
 * running to its deadline and then to its settle deadline, about 23 minutes. It
 * must fit inside the broker's `consumer_timeout`, or the broker closes the
 * channel mid-redrive and the trigger is redelivered (Redrive.test.ts).
 */
export const REDRIVE_MAX_HOLD_MS =
  REDRIVE_MAX_PASSES * (Duration.toMillis(PASS_DEADLINE) + Duration.toMillis(SETTLE_DEADLINE));

// Every message a pass does not move is handed back with `release`, never `requeue`:
// nothing about it failed, and a counted return is a step towards a delivery limit.
export const makeRedrive = (opts: RedriveOptions) => {
  /** The idempotency key: a republish must carry it, or a replay reaches the third party as new work. */
  const sameId = (delivery: DeliveryInfo) => O.getOrUndefined(delivery.messageId);

  const redrivePass = Effect.gen(function* () {
    const conn = opts.rmq;
    const into = yield* conn.publisherToQueue(opts.workQueue, {
      contentType: WORK_CONTENT_TYPE,
      type: WORK_MESSAGE_TYPE,
    });
    const toParked = yield* conn.publisherToQueue(opts.parkedQueue);

    // The handler is a plain callback; a bare `runPromise` builds a fresh runtime per message.
    const services = yield* Effect.context<never>();
    const runInContext = Effect.runPromiseWith(services);

    let moved = 0;
    let strays = 0;
    let poisoned = 0;
    let lastReplayAt = Date.now();
    /** Reserved before the publish is awaited, and handed back if it fails. */
    const reserve = (decision: RedriveDecision, delta: 1 | -1) => {
      moved += decision.destination === "work" ? delta : 0;
      poisoned += decision.destination === "parked" ? delta : 0;
    };

    /** Publishes not yet settled: the pass must not close its channel under them. */
    let inFlight = 0;

    /** Publish, then accept: a crash in between is a duplicate, the reverse a loss. */
    const settleAfter = (send: Effect.Effect<void, RmqError>, onFailure: () => void): Promise<Settlement> => {
      inFlight++;
      return runInContext(send).then(
        (): Settlement => {
          lastReplayAt = Date.now();
          return "accept";
        },
        (): Settlement => {
          onFailure();
          return "release";
        },
      ).finally(() => {
        inFlight--;
      });
    };

    // Parked, not returned to this queue, where it would be lapped by every pass for good.
    const parkStray = (body: string, delivery: DeliveryInfo, originQueue: string, originReason: string) => {
      strays++;
      return settleAfter(
        conn.send(toParked, body, {
          messageId: sameId(delivery),
          headers: { [ORIGIN_QUEUE_HEADER]: originQueue, [ORIGIN_REASON_HEADER]: originReason },
        }),
        () => {},
      );
    };

    const replay = (body: string, delivery: DeliveryInfo, originReason: string) => {
      const decision = nextRedrive(delivery.properties[REDRIVE_COUNT_HEADER]);
      reserve(decision, 1);
      const destinationQueue = decision.destination === "work" ? opts.workQueue : opts.parkedQueue;
      // ATTEMPTS_HEADER is dropped: a redrive is a fresh outage with a fresh budget.
      const send = conn.send(decision.destination === "work" ? into : toParked, body, {
        messageId: sameId(delivery),
        headers: decision.destination === "work" ? { [REDRIVE_COUNT_HEADER]: String(decision.count) } : {},
      });
      // The `traceparent` survives dead-lettering, so a replay rejoins its trace.
      const traced = O.match(delivery.parent, {
        onNone: () => send,
        onSome: (span) =>
          send.pipe(
            Effect.withSpan("work.redrive", {
              attributes: {
                "messaging.system": "rabbitmq",
                "messaging.operation.name": "redrive",
                "messaging.destination.name": destinationQueue,
                "egress.origin_reason": originReason,
                "egress.redrive_destination": decision.destination,
              },
            }),
            Effect.withParentSpan(span),
          ),
      });
      return settleAfter(traced, () => reserve(decision, -1));
    };

    const consumer = yield* conn.consume(opts.deadQueue, (body, delivery): Settlement | Promise<Settlement> => {
      // The broker's annotation, else our stamp from an earlier move, else unknown.
      // Only work from the work queue is replayed; anything else is parked.
      const { queue: originQueue, reason: originReason } = O.getOrElse(delivery.deadLetter, () => ({
        queue: delivery.properties[ORIGIN_QUEUE_HEADER] ?? "unknown",
        reason: delivery.properties[ORIGIN_REASON_HEADER] ?? "unknown",
      }));
      return originQueue !== opts.workQueue
        ? parkStray(body, delivery, originQueue, originReason)
        // The slot is reserved synchronously, before anything is awaited: with a prefetch
        // of 100, check-await-increment overshot the cap (5,739 against 5,000).
        : moved + poisoned >= opts.maxPerPass
          ? "release"
          : replay(body, delivery, originReason);
    });

    yield* opts.gate.withPermit(Ref.set(opts.consumer, O.some(consumer)));

    const when = (ended: boolean, reason: () => string): O.Option<string> =>
      ended ? O.some(reason()) : O.none();

    const finished = Effect.map(opts.isClosed, (closed) =>
      // Order is priority.
      O.firstSomeOf([
        when(!closed, () => "circuit reopened"),
        when(moved + poisoned >= opts.maxPerPass, () => "cap reached"),
        // Idle counts moves, not deliveries, so a pass handed only things it
        // releases still ends.
        when(Date.now() - lastReplayAt > PASS_IDLE_MS, () => "drained"),
      ]),
    );

    const reason = yield* finished.pipe(
      Effect.repeat({ schedule: Schedule.spaced(PASS_POLL), until: O.isSome }),
      Effect.map(O.getOrElse(() => "deadline")),
      Effect.timeoutOrElse({ duration: PASS_DEADLINE, orElse: () => Effect.succeed("deadline") }),
    );

    // The cap counts reservations, so a pass reaches it with its last publishes still
    // awaiting confirms. Closing then dropped their acks: each went back to the dead
    // queue and was replayed again, every capped pass (measured: 5 messages, 23 replays).
    // Cancel, let what was taken settle, then close.
    yield* conn.cancelConsumer(consumer);
    yield* Effect.sync(() => inFlight).pipe(
      Effect.repeat({ schedule: Schedule.spaced(SETTLE_POLL), until: (n) => n === 0 }),
      Effect.timeoutOrElse({
        duration: SETTLE_DEADLINE,
        orElse: () =>
          Effect.logWarning(`${opts.label}: ${inFlight} redrive publish(es) still unconfirmed; closing anyway, they may be replayed twice`),
      }),
    );

    // Clear the Ref only if it still holds this pass's consumer: reconcile may
    // already have replaced it with a newer pass's.
    yield* opts.gate.withPermit(
      Ref.update(opts.consumer, O.filter((held) => held !== consumer)),
    );
    yield* conn.closeConsumer(consumer);

    yield* strays > 0
      ? Effect.logWarning(
          `${opts.label}: parked ${strays} non-work message(s) on ${opts.parkedQueue} — ` +
          `dead-lettered from somewhere other than ${opts.workQueue}, so not replayed as work`,
        )
      : Effect.void;
    yield* poisoned > 0
      ? Effect.logWarning(
          `${opts.label}: parked ${poisoned} message(s) on ${opts.parkedQueue} — ` +
          `redriven ${MAX_REDRIVES} time(s) without succeeding`,
        )
      : Effect.void;
    return { moved, strays, poisoned, reason };
  });
  const passes = Effect.gen(function* () {
    // Debug: the sweep runs every 30s and usually finds nothing.
    yield* Effect.logDebug(
      `${opts.label}: redriving ${opts.deadQueue} (max ${opts.maxPerPass} per pass)`,
    );

    const state = yield* Ref.make({
      pass: 0,
      total: 0,
      totalParked: 0,
      finished: O.none<string>(),
    });

    yield* Effect.repeat(
      redrivePass.pipe(
        Effect.flatMap(({ moved, strays, poisoned, reason }) =>
          Ref.updateAndGet(state, (prior) => ({
            pass: prior.pass + 1,
            total: prior.total + moved,
            totalParked: prior.totalParked + poisoned + strays,
            // Another pass only if this one stopped at the cap having moved something.
            finished:
              reason !== "cap reached" || moved + poisoned + strays === 0
                ? O.some(reason)
                : O.none<string>(),
          })),
        ),
      ),
      { until: (s) => O.isSome(s.finished) || s.pass >= REDRIVE_MAX_PASSES },
    );

    const { total, totalParked, finished } = yield* Ref.get(state);
    const quiet = total === 0 && totalParked === 0;
    const outcome =
      `${total} replayed` + (totalParked > 0 ? `, ${totalParked} parked` : "");
    yield* O.match(finished, {
      onSome: (reason) =>
        quiet
          ? Effect.logDebug(`${opts.label}: redrive found nothing to move (${reason})`)
          : Effect.log(`${opts.label}: redrive finished — ${outcome} (${reason})`),
      onNone: () =>
        Effect.log(
          `${opts.label}: redrive stopped after ${REDRIVE_MAX_PASSES} passes — ${outcome}; ` +
          `whatever is left will be picked up by the next recovery`,
        ),
    });
  });

  /**
   * A sweep landing while another pass runs (up to PASS_DEADLINE) is a no-op.
   * `ensuring` releases the claim on interruption too.
   */
  const redriveOnce = opts.enabled
    ? Ref.modify(opts.running, (running) => [!running, true] as const).pipe(
        Effect.flatMap((claimed) =>
          claimed ? passes.pipe(Effect.ensuring(Ref.set(opts.running, false))) : Effect.void,
        ),
      )
    : Effect.void;

  return redriveOnce;
};
