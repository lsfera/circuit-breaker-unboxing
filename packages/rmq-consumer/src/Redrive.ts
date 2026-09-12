import { Duration, Effect, Option as O, Ref, Schedule } from "effect";
import { randomUUID } from "node:crypto";
import type { Consumer, RmqService, Settlement } from "@egress/rmq/Client.ts";
import type { Semaphore } from "effect/Semaphore";

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
  /** Ceiling on messages moved in a single pass, so a huge backlog is recovered in bounded bites. */
  readonly maxPerPass: number;
  /** Read fresh each time round: a pass must stop as soon as the circuit leaves CLOSED. */
  readonly isClosed: Effect.Effect<boolean>;
  /** Bumped per replayed message, for the daemon's metrics flush. */
  readonly onReplayed: () => void;
  /** Shared with `reconcile`, which retires the channel when the state changes. */
  readonly consumer: Ref.Ref<O.Option<Consumer>>;
  readonly gate: Semaphore;
};

/** A pass is bounded three ways: how often it looks, how long it tolerates no replays, and how long it may run at all. */
const PASS_POLL = Duration.millis(200);
const PASS_IDLE_MS = 2000;
const PASS_DEADLINE = Duration.seconds(60);

export const makeRedrive = (opts: RedriveOptions) => {
  /** Stamped onto anything the redrive moves within the dead-letter queue, so its origin survives the republish that loses the broker's own annotations. */
  const ORIGIN_PROPERTY = "x-egress-origin-queue";
  const ORIGIN_REASON_PROPERTY = "x-egress-origin-reason";
  const ORIGIN_PASS_PROPERTY = "x-egress-redrive-pass";

  const redrivePass = Effect.gen(function* () {
    const conn = opts.rmq;
    const into = yield* conn.publisherToQueue(opts.workQueue);
    const back = yield* conn.publisherToQueue(opts.deadQueue);

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
        return "requeue";
      }

      // Where it came from: the broker's annotation while it still has one, our
      // own stamp once an earlier pass moved it and the annotation was lost, and
      // "unknown" for anything published straight onto this queue by something
      // else. Only work is ever replayed, so anything unattributable is kept,
      // not guessed at. One fold, because both fields come or go together.
      const { queue: originQueue, reason: originReason } = O.getOrElse(
        delivery.deadLetter,
        () => ({
          queue: delivery.properties[ORIGIN_PROPERTY] ?? "unknown",
          reason: delivery.properties[ORIGIN_REASON_PROPERTY] ?? "unknown",
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

      // Reserve the slot *before* awaiting. The consumer's prefetch lets the
      // broker keep a hundred deliveries in flight, so check-then-await-then-increment
      // lets every in-flight handler pass the same check and overshoot the
      // cap by an order of magnitude — measured at 5739 against a cap of
      // 5000 before this was reordered.
      if (moved >= opts.maxPerPass) return "requeue";
      moved++;
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
        const send = conn.send(into, body);
        await runInContext(
          O.map(delivery.parent, (span) =>
            send.pipe(
              Effect.withSpan("work.redrive", {
                attributes: {
                  "messaging.system": "rabbitmq",
                  "messaging.operation.name": "redrive",
                  "messaging.destination.name": opts.workQueue,
                  "egress.origin_reason": originReason,
                },
              }),
              Effect.withParentSpan(span),
            ),
          ).pipe(O.getOrElse(() => send)),
        );
      } catch {
        // The work queue is unreachable; leave the message where it is
        // rather than accepting it into nothing.
        moved--;
        return "requeue";
      }
      opts.onReplayed();
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
        when(moved >= opts.maxPerPass, () => "cap reached"),
        when(cycled, () => "came full circle"),
        // Idle is measured on *replays* rather than on deliveries, so a pass
        // that is only being handed things it will not replay still ends. On
        // `Date.now()` at both ends deliberately: what is being measured is how
        // long the broker has gone without handing over work.
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
    return { moved, parked, reason };
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
    yield* Effect.log(`${opts.label}: redriving ${opts.deadQueue} (max ${opts.maxPerPass} per pass)`);

    /**
     * The loop is `repeat` with an `until`, and the state it threads is a Ref
     * rather than a mutable local: `finished` carries the reason the run ended,
     * and its absence after the cap is what tells the two log lines apart.
     */
    const state = yield* Ref.make({
      pass: 0,
      total: 0,
      finished: O.none<string>(),
    });

    yield* Effect.repeat(
      redrivePass.pipe(
        Effect.flatMap(({ moved, reason }) =>
          Ref.updateAndGet(state, (prior) => ({
            pass: prior.pass + 1,
            total: prior.total + moved,
            // A pass that replayed nothing means whatever is left is not work,
            // so more passes would only cycle it.
            finished:
              reason !== "cap reached" || moved === 0 ? O.some(reason) : O.none<string>(),
          })),
        ),
      ),
      { until: (s) => O.isSome(s.finished) || s.pass >= REDRIVE_MAX_PASSES },
    );

    const { total, finished } = yield* Ref.get(state);
    yield* O.match(finished, {
      onSome: (reason) =>
        Effect.log(`${opts.label}: redrive finished — ${total} replayed (${reason})`),
      onNone: () =>
        Effect.log(
          `${opts.label}: redrive stopped after ${REDRIVE_MAX_PASSES} passes — ${total} replayed; ` +
          `whatever is left will be picked up by the next recovery`,
        ),
    });
  });

  /** Disabled, or a pass already holds the channel: either way there is nothing to start. */
  const redriveOnce = Effect.when(
    passes,
    opts.enabled
      ? opts.gate.withPermit(Ref.get(opts.consumer)).pipe(Effect.map(O.isNone))
      : Effect.succeed(false),
  ).pipe(Effect.asVoid);

  return redriveOnce;
};
