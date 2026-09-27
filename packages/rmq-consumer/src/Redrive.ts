import { Effect, Option as O, Schema } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import type { GotMessage, RmqError } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  MAX_REDRIVES,
  parkedQueueFor,
  PARKED_REASON_HEADER,
  REDRIVE_COUNT_HEADER,
  workQueueFor,
} from "@egress/rmq/WorkQueue.ts";

/**
 * Replays `<api>.work.dead` onto the work queue once the third party is back, and parks what has been redriven
 * too often to be anything but poison. Who runs a pass (the single-active-consumer election) and when (a close,
 * startup, a sweep) is `consumer.ts`'s business.
 */

export type RedriveDecision =
  | { readonly destination: "work"; readonly count: number }
  | { readonly destination: "parked" };

const decodeCount = Schema.decodeUnknownOption(
  Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
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

export type RedriveOptions = {
  readonly apiId: string;
  /** Re-read before every message: a pass stops the moment this replica's breaker leaves closed. */
  readonly isClosed: Effect.Effect<boolean>;
  readonly onOutcome: (outcome: RedriveOutcome) => Effect.Effect<void>;
};

/**
 * `get`, not `consume`: "replay what is there, stop when empty" needs no idle timer. Publish, then ack: a crash
 * in between duplicates (the idempotency key rides along as `message_id`), where the reverse order would lose
 * the message. Whatever a pass does not reach stays where it was.
 */
export const runPass = Effect.fnUntraced(function* (opts: RedriveOptions) {
  const rmq = yield* Rmq;
  const deadQueue = deadLetterQueueFor(opts.apiId);
  const workPub = yield* rmq.publisherToQueue(workQueueFor(opts.apiId));
  const parkedPub = yield* rmq.publisherToQueue(parkedQueueFor(opts.apiId));

  const move = (got: GotMessage) => {
    const messageId = O.getOrUndefined(got.messageId);
    const decision = nextRedrive(got.properties[REDRIVE_COUNT_HEADER]);
    const [publish, outcome] =
      decision.destination === "work"
        ? [rmq.send(workPub, got.body, { messageId, headers: { [REDRIVE_COUNT_HEADER]: String(decision.count) } }), "moved" as const]
        : [rmq.send(parkedPub, got.body, { messageId, headers: { [PARKED_REASON_HEADER]: "redriven-too-often" } }), "parked" as const];
    return publish.pipe(
      Effect.andThen(got.ack),
      Effect.andThen(opts.onOutcome(outcome)),
    );
  };

  const next = (left: number): Effect.Effect<void, RmqError> =>
    left === 0
      ? Effect.void
      : opts.isClosed.pipe(
          Effect.flatMap((closed) => (closed ? rmq.get(deadQueue) : Effect.succeed(O.none<GotMessage>()))),
          Effect.flatMap(
            O.match({
              onNone: () => Effect.void,
              onSome: (got) => move(got).pipe(Effect.andThen(next(left - 1))),
            }),
          ),
        );

  yield* next(MAX_PER_PASS);
});
