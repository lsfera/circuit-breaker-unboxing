import { Match, Predicate } from "effect";
import { WORK_DELIVERY_LIMIT } from "@egress/rmq/ControlPlane.ts";

/**
 * What to do about one call to the third party, with no broker and no fetch
 * in it — the decision half of the daemon's work handler, pulled out for the
 * same reason DaemonState.ts's `reduce` is: it is the one part worth testing
 * without a broker.
 *
 * A failed call is retried by *republishing* a new message, never by
 * `requeue`, so the attempt count can travel with it: a broker requeue hands
 * back the original message with no way to add a header. `ATTEMPTS_HEADER`
 * (ControlPlane.ts) bounds it at `WORK_DELIVERY_LIMIT` attempts. The
 * idempotency key needs nothing of this: it is the `message_id`, which a
 * requeue and a republish both carry. See ADR 016.
 */

/** What the call itself did, already reduced from an HTTP status or a thrown error. */
export type CallOutcome = "ok" | "shed" | "refused" | "failed";

/**
 * 4xx statuses that say "try again", unlike the rest of the 4xx range: a
 * timeout on the third party's side, and a 429, which is backpressure.
 */
const TRY_AGAIN = new Set([408, 429]);

/**
 * An HTTP status, or `"error"` when no response came back (refused, reset,
 * timed out). A 4xx other than 408 and 429 is the third party refusing this
 * request: retrying it sends the same request and gets the same answer.
 */
export const classify = (status: number | "error"): CallOutcome =>
  Match.value(status).pipe(
    Match.when(Predicate.isString, (): CallOutcome => "failed"),
    Match.when(429, (): CallOutcome => "shed"),
    Match.when((n) => n >= 200 && n < 300, (): CallOutcome => "ok"),
    Match.when((n) => n >= 400 && n < 500 && !TRY_AGAIN.has(n), (): CallOutcome => "refused"),
    Match.orElse((): CallOutcome => "failed"),
  );

export type AttemptDecision =
  | { readonly _tag: "accept" }
  /** A 429 never reached the third party — hold and hand back uncounted, see Client.ts's `release`. */
  | { readonly _tag: "release" }
  /** Refused by the third party: parked for a human, never retried or redriven. */
  | { readonly _tag: "park" }
  | {
      readonly _tag: "republish";
      readonly destination: "work" | "dead";
      readonly attempts: number;
    };

/**
 * `attemptsHeader` is `delivery.properties[ATTEMPTS_HEADER]` as the broker
 * hands it back: absent on a message never retried this way, a digit string
 * otherwise. Anything that doesn't parse — there is no way to publish one
 * except this function — is treated as 0 rather than trusted, the same
 * stance `Redrive.ts`'s `nextRedrive` takes on its own count header.
 */
export const nextAttempt = (
  outcome: CallOutcome,
  attemptsHeader: string | undefined,
): AttemptDecision =>
  Match.value(outcome).pipe(
    Match.when("ok", (): AttemptDecision => ({ _tag: "accept" })),
    Match.when("shed", (): AttemptDecision => ({ _tag: "release" })),
    Match.when("refused", (): AttemptDecision => ({ _tag: "park" })),
    Match.when("failed", (): AttemptDecision => {
      const parsed = Number(attemptsHeader ?? 0);
      const attempts = (Number.isFinite(parsed) ? parsed : 0) + 1;
      return {
        _tag: "republish",
        destination: attempts < WORK_DELIVERY_LIMIT ? "work" : "dead",
        attempts,
      };
    }),
    Match.exhaustive,
  );
