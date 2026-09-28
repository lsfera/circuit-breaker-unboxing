import { Effect, Option as O } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import type { RmqError } from "@egress/rmq/Client.ts";

/**
 * The fleet's probe permit: one token in a queue of at most one ready message, so one probe reaches the dependency
 * at a time. The limit counts only ready messages, so the token goes back as a publish then an ack of the held
 * one, never a requeue: a duplicate is refused on its next return, and a crash between leaves two tokens, not none.
 */

/** `scope` is `<application>.<dependency>`: one permit per dependency, shared by the fleet. */
export const permitQueueFor = (scope: string): string => `${scope}.probe-permit`;

/** Classic, not quorum: a quorum queue's length limit is enforced loosely and let two seeds in. */
const PERMIT_QUEUE_OPTIONS = { args: { "x-max-length": 1, "x-overflow": "reject-publish" } } as const;

/** Publish a token, and treat the broker refusing it (one is already there) as success. */
const offer = Effect.fnUntraced(function* (scope: string) {
  const rmq = yield* Rmq;
  yield* rmq.send(yield* rmq.publisherToQueue(permitQueueFor(scope)), "permit");
}, (effect) => Effect.ignore(effect));

/** Every replica seeds at startup; all but the first are refused. */
export const seed = Effect.fnUntraced(function* (scope: string) {
  const rmq = yield* Rmq;
  yield* rmq.declareQueue(permitQueueFor(scope), PERMIT_QUEUE_OPTIONS);
  yield* offer(scope);
});

/** The permit if it is free, as the effect that hands it back; `None` if another replica holds it. */
export const take = Effect.fnUntraced(function* (
  scope: string,
): Effect.fn.Return<O.Option<Effect.Effect<void>>, RmqError, Rmq> {
  const rmq = yield* Rmq;
  const got = yield* rmq.get(permitQueueFor(scope));
  return O.map(got, (token) => offer(scope).pipe(Effect.andThen(token.ack), Effect.provideService(Rmq, rmq)));
});
