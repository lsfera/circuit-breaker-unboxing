import { Cause, Exit, Match, Option as O } from "effect";
import type { Settlement } from "@egress/rmq/Client.ts";
import type { Outcome } from "./Breaker.ts";
import { Halted, Rejected } from "./Dependency.ts";
import type { Role } from "./Dependency.ts";

/**
 * How a message settles once its action has run: a total function of the action's Exit, testable without a
 * broker, breaker or dependency.
 */

/** A settlement, or `park`: publish to `work.parked`, then accept. */
export type Disposition = Settlement | "park";

/**
 * Whether a dependency's answer is accepted, handed back to the broker or parked. `client_error` skips the delivery
 * budget and the release/requeue split entirely: it is parked at once, because a retry or a redrive gets the same
 * answer.
 *
 * A `failed` is charged to the message (`requeue` counts toward the queue's delivery budget) only when it stands
 * alone. One that follows another failure at the same dependency (`streak` above 1) or is a probe is evidence about
 * the dependency, not the message, and is `release`d with no strike: the breaker needs `consecutiveFailures` calls
 * to open and its consumer takes a round trip to stop, and in that window the same few messages are redelivered
 * again and again, so charging them would dead-letter healthy messages. A message that fails between successes (a
 * poison message on a healthy dependency) is still charged, and still parked.
 *
 * `throttled` is `release`d too: the dependency answered "not right now", which says nothing about the message.
 */
export const decide = (outcome: Outcome, role: Role = "work", streak = 1): Disposition =>
  Match.value(outcome).pipe(
    Match.withReturnType<Disposition>(),
    Match.when("ok", () => "accept"),
    Match.when("client_error", () => "park"),
    Match.when("throttled", () => "release"),
    Match.when("failed", () => (role === "probe" || streak > 1 ? "release" : "requeue")),
    Match.exhaustive,
  );

/** `reason` is the parked message's `x-egress-parked-reason`, and names the outcome in a log line. */
export type Settled = { readonly disposition: Disposition; readonly reason: string };

const halted = (h: Halted): Settled =>
  Match.value(h.stop).pipe(
    Match.withReturnType<Settled>(),
    // `open` and `no-permit` made no call: nothing was learned about the message.
    Match.when(Match.is("open", "no-permit"), () => ({ disposition: "release", reason: h.stop })),
    Match.orElse((outcome) => ({ disposition: decide(outcome, h.role, h.streak), reason: `refused-${h.dependency}-${h.reason}` })),
  );

/**
 * Anything the action failed with outside a dependency — an error it did not wrap, a defect — is a fault of the
 * application's own code, not of a dependency: charged to the message (`requeue`), reported to no breaker.
 */
export const settle = (exit: Exit.Exit<unknown, unknown>): Settled =>
  Exit.match(exit, {
    onSuccess: (): Settled => ({ disposition: "accept", reason: "ok" }),
    onFailure: (cause) =>
      O.match(Cause.findErrorOption(cause), {
        onNone: (): Settled => ({ disposition: "requeue", reason: "defect" }),
        onSome: (error) =>
          Match.value(error).pipe(
            Match.withReturnType<Settled>(),
            Match.when(Match.instanceOf(Halted), halted),
            Match.when(Match.instanceOf(Rejected), (r) => ({ disposition: "park", reason: `rejected-${r.reason}` })),
            Match.orElse(() => ({ disposition: "requeue", reason: "unwrapped-error" })),
          ),
      }),
  });
