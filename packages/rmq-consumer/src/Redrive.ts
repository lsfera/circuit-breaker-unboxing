import { Effect, Ref } from "effect";
import { randomUUID } from "node:crypto";
import type { Consumer, RmqService, Settlement } from "@egress/rmq/Client.ts";
import type { Semaphore } from "effect/Semaphore";

/**
 * Recovering the dead-letter queue: bounded passes that replay work back onto
 * the work queue, and leave anything that is not work where a human can find
 * it.
 *
 * Its own module because daemon.ts had grown to eight hundred lines holding
 * eight separate concerns, and this is the largest and most self-contained of
 * them. The options below are not ceremony — they are the coupling, written
 * down: a redrive needs the daemon's connection, the two queue names, the
 * circuit state (it must stop the moment the circuit reopens), and the
 * daemon's own consumer Ref and permit, because `reconcile` retires the
 * channel from the other side when the state changes.
 *
 * Elected to exactly one daemon by the broker — see the SAC redrive-trigger
 * queue in daemon.ts. Five daemons replaying the same backlog would turn a
 * recovery into a fivefold burst at a third party that has just come back.
 */
export type RedriveOptions = {
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
  readonly consumer: Ref.Ref<Consumer | null>;
  readonly gate: Semaphore;
};

export const makeRedrive = (opts: RedriveOptions) => {
  /** Stamped onto anything the redrive moves within the dead-letter queue, so its origin survives the republish that loses the broker's own annotations. */
  const ORIGIN_PROPERTY = "x-egress-origin-queue";
  const ORIGIN_REASON_PROPERTY = "x-egress-origin-reason";
  const ORIGIN_PASS_PROPERTY = "x-egress-redrive-pass";

  const redrivePass = Effect.gen(function* () {
    const conn = opts.rmq;
    const into = yield* conn.publisherToQueue(opts.workQueue);
    const back = yield* conn.publisherToQueue(opts.deadQueue);

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

      // Where it came from: the broker's annotation on first sight, our own
      // stamp once an earlier pass moved it, and "unknown" for anything
      // published straight onto this queue by something else. Only work is
      // ever replayed, so anything unattributable is kept, not guessed at.
      const originQueue =
        delivery.deadLetter?.queue ?? delivery.properties[ORIGIN_PROPERTY] ?? "unknown";
      const originReason =
        delivery.deadLetter?.reason ?? delivery.properties[ORIGIN_REASON_PROPERTY] ?? "unknown";

      if (originQueue !== opts.workQueue) {
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
      if (moved >= opts.maxPerPass) return "requeue";
      moved++;
      // Publish, then accept — never the reverse. A crash between the two
      // redelivers a message that was already replayed, which is a duplicate;
      // accepting first would lose it outright. Duplicates are recoverable and
      // losses are not, and the premise of this queue is that the work matters.
      try {
        await Effect.runPromise(conn.send(into, body));
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

    yield* opts.gate.withPermit(Ref.set(opts.consumer, consumer));

    const deadline = Date.now() + 60_000;
    let reason = "deadline";
    while (true) {
      yield* Effect.sleep("200 millis");
      if (!(yield* opts.isClosed)) {
        reason = "circuit reopened";
        break;
      }
      if (moved >= opts.maxPerPass) {
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

    // Close *this pass's* channel, and only clear the Ref if it still points
    // at it. Closing whatever the Ref happens to hold is not the same thing:
    // reconcile retires the consumer on any state change, so a pass whose
    // channel had already been retired and replaced by a newer one would tear
    // down the newer pass's live consumer on its way out.
    yield* opts.gate.withPermit(Ref.update(opts.consumer, (c) => (c === consumer ? null : c)));
    yield* conn.closeConsumer(consumer);

    if (parked > 0) {
      yield* Effect.logWarning(
        `${opts.label}: left ${parked} non-work message(s) on ${opts.deadQueue} — ` +
          `dead-lettered from somewhere other than ${opts.workQueue}, so not replayed as work`,
      );
    }
    return { moved, parked, reason };
  });
  /**
   * Replay the dead-letter queue, in bounded passes, until it is empty or
   * something says stop. Runs on exactly one daemon — the broker elects it
   * on a second SAC queue, below — and only while the circuit is CLOSED.
   *
   * Passes rather than one long drain because each pass is a fresh channel it
   * can afford to destroy — closing it hands back everything it was holding —
   * and because `redriveMax` is there to keep any single burst onto the work
   * queue bounded. Looping until
   * drained is what makes this actually self-healing: a backlog larger than
   * the cap would otherwise need one outage per 5,000 messages to recover.
   */
  const REDRIVE_MAX_PASSES = 20;
  const redriveOnce = Effect.gen(function* () {
    if (!opts.enabled) return;
    if ((yield* opts.gate.withPermit(Ref.get(opts.consumer))) !== null) return;

    yield* Effect.log(`${opts.label}: redriving ${opts.deadQueue} (max ${opts.maxPerPass} per pass)`);
    let total = 0;
    for (let pass = 1; pass <= REDRIVE_MAX_PASSES; pass++) {
      const { moved, reason } = yield* redrivePass;
      total += moved;
      // A pass that replayed nothing means whatever is left is not work,
      // so more passes would only cycle it.
      if (reason !== "cap reached" || moved === 0) {
        yield* Effect.log(`${opts.label}: redrive finished — ${total} replayed (${reason})`);
        return;
      }
    }
    yield* Effect.log(
      `${opts.label}: redrive stopped after ${REDRIVE_MAX_PASSES} passes — ${total} replayed; ` +
        `whatever is left will be picked up by the next recovery`,
    );
  });

  return redriveOnce;
};
