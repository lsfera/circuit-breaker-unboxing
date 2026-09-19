import { Effect, Option as O } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  MAX_REDRIVES,
  parkedQueueFor,
  REDRIVE_COUNT_HEADER,
  workQueueFor,
} from "@egress/rmq/ControlPlane.ts";

/**
 * Recovering `<api>.work.dead` once the third party is back: bounded passes
 * that replay dead-lettered work onto the work queue, and park anything that
 * has failed too many times to be an outage rather than poison. Election
 * (exactly one replica ever runs a pass) and the trigger that starts one are
 * `consumer.ts`'s job — this module is broker-agnostic of who's in charge,
 * same split `Breaker.ts` has from `consumer.ts`'s `onStateChange`. A pass
 * is triggered either by a breaker transition into Closed or by a fixed
 * clock — a message can dead-letter while the breaker never leaves Closed at
 * all, so a transition-only trigger can leave it unread for a long time.
 */

/**
 * Where a redriven message goes next, and what it carries there — pulled out
 * because it's the one decision here with no broker in it.
 *
 * `header` is `delivery.properties[REDRIVE_COUNT_HEADER]` as the broker hands
 * it back: absent on a message that has never been redriven, a digit string
 * on one that has. Anything that doesn't parse as a redrive count (there is
 * no way to publish one except this module) is treated as 0 rather than
 * trusted — the failure mode of trusting it is a poison message that
 * redrives forever, which is the one thing `MAX_REDRIVES` exists to prevent.
 */
export type RedriveDecision =
  | { readonly destination: "work"; readonly count: number }
  | { readonly destination: "parked" };

export const nextRedrive = (header: string | undefined): RedriveDecision => {
  const parsed = header === undefined ? 0 : Number(header);
  const count = (Number.isFinite(parsed) ? parsed : 0) + 1;
  return count > MAX_REDRIVES ? { destination: "parked" } : { destination: "work", count };
};

/** One pass moves at most this many messages, so the elected redriver's single active-consumer slot can't be hogged by a huge backlog. */
const MAX_PER_PASS = 200;

export type RedriveOutcome = "moved" | "parked";

export type RedriveOptions = {
  readonly apiId: string;
  /** Re-read on every message, not just once at the start — a pass must stop the instant this replica's own breaker reopens. */
  readonly isClosed: Effect.Effect<boolean>;
  /** Called once per message actually moved or parked, for telemetry — kept out of this function's own return so a pass can report incrementally rather than only at the end. */
  readonly onOutcome: (outcome: RedriveOutcome) => void;
};

/**
 * Drains `<api>.work.dead` with `rmq.get` — a non-blocking single fetch
 * already used for article 3's probe permit, and a natural fit here too:
 * "replay what's there, stop when it's empty" needs no idle timer the way a
 * long-lived `consume` subscription would.
 *
 * Publish-then-ack, never the reverse: a crash between the two redelivers
 * the original, which is a duplicate; acking first would lose the message
 * outright if the publish never lands. Anything not moved this call (the
 * gate closed, the cap hit, the queue empty) is left exactly where it was —
 * `get`'s own message is simply never acked, so RabbitMQ requeues it.
 */
export const runPass = Effect.fn(function* (opts: RedriveOptions) {
  const rmq = yield* Rmq;
  const deadQueue = deadLetterQueueFor(opts.apiId);
  const workPub = yield* rmq.publisherToQueue(workQueueFor(opts.apiId));
  const parkedPub = yield* rmq.publisherToQueue(parkedQueueFor(opts.apiId));

  for (let processed = 0; processed < MAX_PER_PASS; processed++) {
    const closed = yield* opts.isClosed;
    if (!closed) return;

    const got = yield* rmq.get(deadQueue);
    if (O.isNone(got)) return;

    const decision = nextRedrive(got.value.properties[REDRIVE_COUNT_HEADER]);
    if (decision.destination === "work") {
      yield* rmq.send(workPub, got.value.body, { [REDRIVE_COUNT_HEADER]: String(decision.count) });
      yield* got.value.ack;
      opts.onOutcome("moved");
    } else {
      yield* rmq.send(parkedPub, got.value.body);
      yield* got.value.ack;
      opts.onOutcome("parked");
    }
  }
});
