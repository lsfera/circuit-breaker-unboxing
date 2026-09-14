import { WORK_DELIVERY_LIMIT } from "@egress/rmq/ControlPlane.ts";

/**
 * What to do about one call to the third party, with no broker and no fetch
 * in it — the decision half of the daemon's work handler, pulled out for the
 * same reason DaemonState.ts's `reduce` is: it is the one part worth testing
 * without a broker.
 *
 * A key only protects the third party if every retry of the same attempt
 * carries the same one, and a broker requeue hands back the original message
 * with no way to add a header to it — so a failed call is retried by
 * *republishing* a new message with the key carried forward, never by
 * `requeue`. `ATTEMPTS_HEADER` (ControlPlane.ts) is what makes that
 * republishing bounded: three attempts, same as the old broker-enforced
 * `WORK_DELIVERY_LIMIT`, now spent by the daemon rather than by
 * redeliveries.
 */

/** What the call itself did, already reduced from an HTTP status or a thrown error. */
export type CallOutcome = "ok" | "shed" | "failed";

export type AttemptDecision =
  | { readonly _tag: "accept" }
  /** A 429 never reached the third party — hold and hand back uncounted, see Client.ts's `release`. */
  | { readonly _tag: "release" }
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
): AttemptDecision => {
  if (outcome === "ok") return { _tag: "accept" };
  if (outcome === "shed") return { _tag: "release" };
  const parsed = attemptsHeader === undefined ? 0 : Number(attemptsHeader);
  const attempts = (Number.isFinite(parsed) ? parsed : 0) + 1;
  return {
    _tag: "republish",
    destination: attempts < WORK_DELIVERY_LIMIT ? "work" : "dead",
    attempts,
  };
};
