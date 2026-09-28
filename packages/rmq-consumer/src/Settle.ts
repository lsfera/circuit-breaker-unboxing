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
 * `client_error` is parked at once: a retry or redrive gets the same answer. A `failed` is charged to the message
 * (`requeue`, counted toward the delivery limit) only when it stands alone. A probe's, or one in a streak, is
 * evidence about the dependency and is `release`d; charged, the messages redelivered while the breaker opens would
 * be dead-lettered healthy. `throttled` is `release`d: "not now" says nothing about the message.
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
