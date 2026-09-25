import { Match, Predicate } from "effect";
import { WORK_DELIVERY_LIMIT } from "@egress/rmq/ControlPlane.ts";

/**
 * The pure decision about one call. A failure is republished with
 * `ATTEMPTS_HEADER` + 1, never requeued, since a requeue cannot carry a header
 * (ADR 016); the key is the `message_id`, which both carry.
 */

/** What the call itself did, already reduced from an HTTP status or a thrown error. */
export type CallOutcome = "ok" | "shed" | "refused" | "failed";

/** 4xx that mean "try again": a timeout, and backpressure. */
const TRY_AGAIN = new Set([408, 429]);

/** Another 4xx is a refusal: retrying sends the same request for the same answer. */
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

/** An unparseable header counts as 0 rather than being trusted. */
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
