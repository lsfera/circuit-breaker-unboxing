import { Duration, Effect, Option as O, Ref, Schedule } from "effect";
import { randomUUID } from "node:crypto";
import {
  IDEMPOTENCY_KEY_HEADER,
  MAX_REDRIVES,
  ORIGIN_QUEUE_HEADER,
  ORIGIN_REASON_HEADER,
  REDRIVE_COUNT_HEADER,
} from "@egress/rmq/ControlPlane.ts";
import type { Consumer, RmqService, Settlement } from "@egress/rmq/Client.ts";
import type { Semaphore } from "effect/Semaphore";

/**
 * Where a redriven work message goes next, and what it carries there — pulled
 * out because it is the one decision in this file with no broker in it.
 *
 * `header` is `delivery.properties[REDRIVE_COUNT_HEADER]` as the broker hands
 * it back: absent on a message that has never been redriven, a digit string
 * on one that has. Anything that doesn't parse as a redrive count (there is
 * no way to publish one except this function) is treated as 0 rather than
 * trusted — the failure mode of trusting it is a poison message that redrives
 * forever, which is the one thing MAX_REDRIVES exists to prevent.
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
 * Recovering the dead-letter queue: bounded passes that replay work back onto the
 * work queue and leave anything that is not work where a human can find it.
 *
 * Elected to one daemon by the broker — five replaying the same backlog would
 * make a recovery a fivefold burst at an upstream that has just come back. The
 * options are the coupling written down, including the daemon's consumer Ref and
 * permit, because `reconcile` retires the channel from the other side.
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
   * Whether a pass is currently running, claimed with `Ref.modify` rather than
   * `consumer`: `consumer` is only set once `conn.consume` has actually
   * returned a channel, and that is itself an async round trip. Two callers
   * that both check "is a pass running" while the first is still awaiting that
   * round trip would both see none and both open a consumer on the same
   * dead-letter queue. `Ref.modify` reads and marks the claim in one
   * synchronous step, so there is no gap between them to race into — see the
   * comment on `redriveOnce` below.
   */
  readonly running: Ref.Ref<boolean>;
};

/** A pass is bounded three ways: how often it looks, how long it tolerates no replays, and how long it may run at all. */
const PASS_POLL = Duration.millis(200);
const PASS_IDLE_MS = 2000;
const PASS_DEADLINE = Duration.seconds(60);

// Every message a pass does not move is handed back with `release`, never `requeue`:
// nothing about it failed, and a counted return is a step towards a delivery limit.
export const makeRedrive = (opts: RedriveOptions) => {
  /** Marks a lap of this pass, local to it — ORIGIN_QUEUE_HEADER/ORIGIN_REASON_HEADER (ControlPlane.ts) are the ones shared with daemon.ts's own dead-lettering. */
  const ORIGIN_PASS_PROPERTY = "x-egress-redrive-pass";

  /** The idempotency key as a header, or `undefined` — a republish must carry it when the original delivery did, and nothing extra when it did not. */
  const idempotencyHeader = (key: O.Option<string>): Record<string, string> | undefined =>
    O.match(key, { onNone: () => undefined, onSome: (v) => ({ [IDEMPOTENCY_KEY_HEADER]: v }) });

  const redrivePass = Effect.gen(function* () {
    const conn = opts.rmq;
    const into = yield* conn.publisherToQueue(opts.workQueue);
    const back = yield* conn.publisherToQueue(opts.deadQueue);
    const toParked = yield* conn.publisherToQueue(opts.parkedQueue);

    /**
     * Captured for the same reason daemon.ts captures it: the handler below is a
     * plain AMQP callback, and `Effect.runPromise` would build a fresh runtime
     * with default services for every message a pass moves — thousands of them.
     */
    const services = yield* Effect.context<never>();
    const runInContext = Effect.runPromiseWith(services);

    const passId = randomUUID();
    let moved = 0;
    let parked = 0;
    let poisoned = 0;
    let cycled = false;
    let lastReplayAt = Date.now();
    const consumer = yield* conn.consume(opts.deadQueue, async (body, delivery): Promise<Settlement> => {
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
        return "release";
      }

      // Where it came from: the broker's annotation while it still has one, our
      // own stamp once an earlier pass moved it and the annotation was lost, and
      // "unknown" for anything published straight onto this queue by something
      // else. Only work is ever replayed, so anything unattributable is kept,
      // not guessed at. One fold, because both fields come or go together.
      const { queue: originQueue, reason: originReason } = O.getOrElse(
        delivery.deadLetter,
        () => ({
          queue: delivery.properties[ORIGIN_QUEUE_HEADER] ?? "unknown",
          reason: delivery.properties[ORIGIN_REASON_HEADER] ?? "unknown",
        }),
      );

      if (originQueue !== opts.workQueue) {
        // Moved to the tail rather than released, because releasing puts it
        // straight back at the head and starves everything behind it, and
        // stamped on the way so the provenance the annotations carried is
        // not lost with them.
        parked++;
        try {
          await runInContext(
            conn.send(back, body, {
              [ORIGIN_QUEUE_HEADER]: originQueue,
              [ORIGIN_REASON_HEADER]: originReason,
              [ORIGIN_PASS_PROPERTY]: passId,
              ...idempotencyHeader(delivery.idempotencyKey),
            }),
          );
          return "accept";
        } catch {
          return "release";
        }
      }

      // Reserve the slot *before* awaiting. The consumer's prefetch lets the
      // broker keep a hundred deliveries in flight, so check-then-await-then-increment
      // lets every in-flight handler pass the same check and overshoot the
      // cap by an order of magnitude — measured at 5739 against a cap of
      // 5000 before this was reordered. Poison and replayed messages share
      // one cap: both leave the dead-letter queue in this branch, and the cap
      // bounds the burst either puts on the queue it lands on.
      if (moved + poisoned >= opts.maxPerPass) return "release";

      // A message redriven MAX_REDRIVES times without succeeding is poison,
      // not unlucky: WORK_DELIVERY_LIMIT already spent three attempts per
      // redrive, so this is five outages' worth of the third party rejecting
      // it. Parking it is what keeps the periodic sweep from redriving it
      // forever — see nextRedrive.
      const decision = nextRedrive(delivery.properties[REDRIVE_COUNT_HEADER]);
      if (decision.destination === "work") moved++;
      else poisoned++;
      // Publish, then accept — never the reverse. A crash between the two
      // redelivers a message that was already replayed, which is a duplicate;
      // accepting first would lose it outright. Duplicates are recoverable and
      // losses are not, and the premise of this queue is that the work matters.
      try {
        // A replayed message rejoins the trace that produced it. The
        // `traceparent` survives being dead-lettered — RabbitMQ keeps
        // application headers — and republishing the body alone was throwing it
        // away, so the one message worth following, the one that failed and was
        // retried, arrived looking like a brand new one with no history.
        //
        // Only a message that carried a parent pays for a span, same as the
        // daemon's egress call: an untraced replay takes the plain path.
        // Both destinations carry the trace the same way — a parked message is
        // still the tail of the same story, now waiting on a human instead of
        // another attempt.
        const destinationQueue =
          decision.destination === "work" ? opts.workQueue : opts.parkedQueue;
        // ATTEMPTS_HEADER is deliberately not carried forward: a redrive is a
        // fresh outage, and starting it with the prior outage's call count
        // would dead-letter the message on its first failure this time round.
        const send = conn.send(decision.destination === "work" ? into : toParked, body, {
          ...idempotencyHeader(delivery.idempotencyKey),
          ...(decision.destination === "work"
            ? { [REDRIVE_COUNT_HEADER]: String(decision.count) }
            : {}),
        });
        await runInContext(
          O.map(delivery.parent, (span) =>
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
          ).pipe(O.getOrElse(() => send)),
        );
      } catch {
        // The destination is unreachable; leave the message where it is
        // rather than accepting it into nothing.
        if (decision.destination === "work") moved--;
        else poisoned--;
        return "release";
      }
      // `moved`/`poisoned` already carry this for the pass-end log lines; the
      // metrics registry reads queue-flow numbers from RabbitMQ itself now
      // (rabbitmq_detailed_queue_*, rabbitmq_global_messages_dead_lettered_*),
      // not from an application counter.
      lastReplayAt = Date.now();
      return "accept";
    });

    yield* opts.gate.withPermit(Ref.set(opts.consumer, O.some(consumer)));

    /**
     * Every way a pass ends, as one total function polled on a schedule — the
     * deadline is the timeout around it rather than a sixth branch, so no
     * `reason` can be reached without saying which one it was.
     */
    const when = (ended: boolean, reason: () => string): O.Option<string> =>
      ended ? O.some(reason()) : O.none();

    const finished = Effect.map(opts.isClosed, (closed) =>
      // The order is the priority, and `firstSomeOf` is what makes it data
      // rather than the order four `if`s happen to be written in.
      O.firstSomeOf([
        when(!closed, () => "circuit reopened"),
        when(moved + poisoned >= opts.maxPerPass, () => "cap reached"),
        when(cycled, () => "came full circle"),
        // Idle is measured on *replays and parkings* rather than on
        // deliveries, so a pass that is only being handed things it will not
        // replay still ends. On `Date.now()` at both ends deliberately: what
        // is being measured is how long the broker has gone without handing
        // over work.
        when(Date.now() - lastReplayAt > PASS_IDLE_MS, () =>
          parked > 0 ? "nothing left to replay" : "drained",
        ),
      ]),
    );

    const reason = yield* finished.pipe(
      Effect.repeat({ schedule: Schedule.spaced(PASS_POLL), until: O.isSome }),
      Effect.map(O.getOrElse(() => "deadline")),
      Effect.timeoutOrElse({ duration: PASS_DEADLINE, orElse: () => Effect.succeed("deadline") }),
    );

    // Close *this pass's* channel, and only clear the Ref if it still points
    // at it. Closing whatever the Ref happens to hold is not the same thing:
    // reconcile retires the consumer on any state change, so a pass whose
    // channel had already been retired and replaced by a newer one would tear
    // down the newer pass's live consumer on its way out.
    yield* opts.gate.withPermit(
      Ref.update(opts.consumer, O.filter((held) => held !== consumer)),
    );
    yield* conn.closeConsumer(consumer);

    yield* parked > 0
      ? Effect.logWarning(
          `${opts.label}: left ${parked} non-work message(s) on ${opts.deadQueue} — ` +
          `dead-lettered from somewhere other than ${opts.workQueue}, so not replayed as work`,
        )
      : Effect.void;
    yield* poisoned > 0
      ? Effect.logWarning(
          `${opts.label}: parked ${poisoned} message(s) on ${opts.parkedQueue} — ` +
          `redriven ${MAX_REDRIVES} time(s) without succeeding`,
        )
      : Effect.void;
    return { moved, parked, poisoned, reason };
  });
  /**
   * Replay in bounded passes until empty or told to stop, only while CLOSED.
   *
   * Passes rather than one drain: each is a fresh channel it can afford to
   * destroy, and `redriveMax` bounds any single burst onto the work queue.
   * Looping until drained is what keeps a backlog larger than the cap from
   * needing one outage per cap to recover.
   */
  const REDRIVE_MAX_PASSES = 20;
  const passes = Effect.gen(function* () {
    // Debug rather than info: this runs every REDRIVE_SWEEP_MS from the sweep,
    // not only on a recovery, and most runs find nothing — see daemon.ts. The
    // finish line below carries the outcome at info level whenever one moved.
    yield* Effect.logDebug(
      `${opts.label}: redriving ${opts.deadQueue} (max ${opts.maxPerPass} per pass)`,
    );

    /**
     * The loop is `repeat` with an `until`, and the state it threads is a Ref
     * rather than a mutable local: `finished` carries the reason the run ended,
     * and its absence after the cap is what tells the two log lines apart.
     */
    const state = yield* Ref.make({
      pass: 0,
      total: 0,
      totalParked: 0,
      finished: O.none<string>(),
    });

    yield* Effect.repeat(
      redrivePass.pipe(
        Effect.flatMap(({ moved, poisoned, reason }) =>
          Ref.updateAndGet(state, (prior) => ({
            pass: prior.pass + 1,
            total: prior.total + moved,
            totalParked: prior.totalParked + poisoned,
            // A pass that moved nothing — replayed or parked — means whatever
            // is left is not work, so more passes would only cycle it.
            finished:
              reason !== "cap reached" || moved + poisoned === 0
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
   * `running` is claimed atomically before `passes` starts and released once
   * it ends, however it ends — `Effect.ensuring` covers the interrupt case
   * too, which matters here: this fiber can be cancelled mid-pass by the
   * daemon shutting down.
   *
   * This is what makes a sweep that lands while an election-triggered pass is
   * still going — or a sweep that lands before the previous sweep's pass has
   * finished, since a pass may legitimately run up to PASS_DEADLINE while the
   * sweep repeats every REDRIVE_SWEEP_MS — a no-op instead of a second
   * consumer on the same dead-letter queue. `consumer` alone cannot do this
   * job; see the comment on `running` in RedriveOptions.
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
