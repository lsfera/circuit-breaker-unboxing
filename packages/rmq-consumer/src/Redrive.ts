import { carry, Rmq } from "@egress/rmq/Client.ts";
import type { GotMessage } from "@egress/rmq/Client.ts";
import { deadLetterQueueFor, workQueueFor } from "@egress/rmq/WorkQueue.ts";
import { Effect, Match, Option as O, Schema } from "effect";

/**
 * Replays `<api>.work.dead` onto the work queue once the third party is back, and parks what has been redriven
 * too often to be anything but poison. Who runs a pass (the single-active-consumer election) and when (a close,
 * startup, a sweep) is `consumer.ts`'s business.
 */

/**
 * The redrive election: `x-single-active-consumer` delivers to one bound consumer and holds the rest as backups,
 * promoting one if the active one disconnects. Nothing is published on it but the trigger itself.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

export const redriveTriggerQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-single-active-consumer": true },
  durable: true
});

/**
 * Poison, parked for a human: what the consumer cannot read, what the third party refused, and what the redrive
 * gave up on after `MAX_REDRIVES`. Terminal, like the dead-letter queue.
 */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

/** Why a message was parked: `refused-<status>`, `unreadable-<format|malformed|keyless>`, or `redriven-too-often`. */
export const PARKED_REASON_HEADER = "x-egress-parked-reason";

export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true
});

/** Stamped on a redriven message; absent means it has never been redriven. */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/**
 * Redrives before a message is treated as poison rather than unlucky. Each one grants a fresh
 * `WORK_DELIVERY_LIMIT`, so this bounds outages survived, not attempts.
 */
const MAX_REDRIVES = 5;

type RedriveDecision =
  | { readonly destination: "work"; readonly count: number; }
  | { readonly destination: "parked"; };

const decodeCount = Schema.decodeUnknownOption(
  Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))
);

/**
 * `header` is `REDRIVE_COUNT_HEADER` as the broker hands it back. Anything but a whole count counts as 0 rather
 * than being trusted: trusting it is how a poison message would redrive forever.
 */
export const nextRedrive = (header: string | undefined): RedriveDecision => {
  const count = O.getOrElse(decodeCount(header), () => 0) + 1;
  return count > MAX_REDRIVES ? { destination: "parked" } : { destination: "work", count };
};

/** One pass moves at most this many, so a huge backlog does not monopolise the elected replica. */
const MAX_PER_PASS = 200;

export type RedriveOutcome = "moved" | "parked";

type RedriveOptions = {
  readonly apiId: string;
  /** Re-read before every message: a pass stops the moment this replica's breaker leaves closed. */
  readonly isClosed: Effect.Effect<boolean>;
  readonly onOutcome: (outcome: RedriveOutcome) => Effect.Effect<void>;
};

/**
 * `basic.get`, not `consume`: "replay what is there, stop when empty" needs no idle timer. One channel for the
 * pass (`pull`), not one per message. Publish, then ack: a crash in between duplicates (the idempotency key rides
 * along as `message_id`), where the reverse order would lose the message. Whatever a pass does not reach stays
 * where it was.
 */
export const runPass = Effect.fnUntraced(function*(opts: RedriveOptions) {
  const rmq = yield* Rmq;
  const deadQueue = deadLetterQueueFor(opts.apiId);
  const next = yield* rmq.pull(deadQueue);
  const workPub = yield* rmq.publisherToQueue(workQueueFor(opts.apiId));
  const parkedPub = yield* rmq.publisherToQueue(parkedQueueFor(opts.apiId));

  const move = (got: GotMessage) => {
    const [publish, outcome] = Match.value(nextRedrive(got.properties[REDRIVE_COUNT_HEADER])).pipe(
      Match.discriminatorsExhaustive("destination")({
        work: ({ count }) =>
          [
            rmq.send(workPub, got.body, carry(got, { [REDRIVE_COUNT_HEADER]: String(count) })),
            "moved" as const
          ] as const,
        parked: () =>
          [
            rmq.send(parkedPub, got.body, carry(got, { [PARKED_REASON_HEADER]: "redriven-too-often" })),
            "parked" as const
          ] as const
      })
    );
    // A move that fails or is interrupted hands the message back, rather than leave it unacked on the pass's
    // channel, invisible to the rest of the pass, until the channel closes.
    return publish.pipe(
      Effect.onError(() => got.nack),
      Effect.andThen(got.ack),
      Effect.andThen(opts.onOutcome(outcome))
    );
  };

  /** One message, if the breaker is still closed and the queue still has one; `false` ends the pass. */
  const step = opts.isClosed.pipe(
    Effect.flatMap((closed) => (closed ? next : Effect.succeedNone)),
    Effect.flatMap(
      O.match({
        onNone: () => Effect.succeed(false),
        onSome: (got) => move(got).pipe(Effect.as(true))
      })
    )
  );

  yield* step.pipe(Effect.repeat({ while: (more) => more, times: MAX_PER_PASS - 1 }));
}, Effect.scoped);
