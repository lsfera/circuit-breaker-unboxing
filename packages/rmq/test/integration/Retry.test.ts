import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import { broker, skipIfNoDocker, startBroker, stopBroker, waitFor } from "./harness.ts";
import { Rmq } from "../../src/Client.ts";
import {
  ATTEMPTS_HEADER,
  deadLetterQueueFor,
  deadLetterQueueOptions,
  IDEMPOTENCY_KEY_HEADER,
  ORIGIN_QUEUE_HEADER,
  ORIGIN_REASON_HEADER,
  workQueueFor,
  workQueueOptions,
} from "../../src/ControlPlane.ts";

/**
 * NOT RUN as part of `pnpm run check` or this change — see the task report.
 * Exercised only by hand against a real broker (`docker` is off-limits while
 * the chaos run in progress uses the live stack).
 *
 * The daemon now retries a failed call by *republishing* rather than by a
 * broker requeue — see packages/rmq-consumer/src/Attempts.ts — and once
 * WORK_DELIVERY_LIMIT is spent it publishes straight onto the dead-letter
 * queue itself, stamping ORIGIN_QUEUE_HEADER/ORIGIN_REASON_HEADER by hand.
 * That message never passes through the broker's own dead-lettering, so it
 * carries no `x-first-death-*` at all — `delivery.deadLetter` reads `None`.
 * Redrive.ts falls back to the ORIGIN_* properties for exactly this case, and
 * if that fallback ever breaks, this is what would silently start parking
 * every message the daemon itself exhausts instead of redriving it as work.
 */

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>,
  );

test("a message the daemon dead-letters itself carries no broker death annotation, only the stamped origin", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `attempts-exhausted-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);
  const key = "22222222-2222-2222-2222-222222222222";

  const seen = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const toDead = yield* rmq.publisherToQueue(dead);

      // What Attempts.ts's "republish" -> "dead" branch does: publish
      // straight onto the dead-letter queue, never through a broker reject.
      yield* rmq.send(toDead, "poison-work", {
        [IDEMPOTENCY_KEY_HEADER]: key,
        [ATTEMPTS_HEADER]: "3",
        [ORIGIN_QUEUE_HEADER]: work,
        [ORIGIN_REASON_HEADER]: "attempts-exhausted",
      });

      const seen: Array<{
        deadLetter: boolean;
        originQueue: string | undefined;
        originReason: string | undefined;
        key: O.Option<string>;
      }> = [];
      yield* rmq.consume(dead, (_body, delivery) => {
        seen.push({
          deadLetter: O.isSome(delivery.deadLetter),
          originQueue: delivery.properties[ORIGIN_QUEUE_HEADER],
          originReason: delivery.properties[ORIGIN_REASON_HEADER],
          key: delivery.idempotencyKey,
        });
        return "accept" as const;
      });
      yield* waitFor(() => seen.length >= 1);
      return seen;
    }),
  );

  assert.equal(seen.length, 1);
  assert.equal(
    seen[0]!.deadLetter,
    false,
    "a message published straight onto the queue carries no x-first-death-*",
  );
  assert.equal(
    seen[0]!.originQueue,
    work,
    "Redrive.ts's fallback reads this as the work queue to redrive back onto",
  );
  assert.equal(seen[0]!.originReason, "attempts-exhausted");
  assert.equal(O.getOrUndefined(seen[0]!.key), key, "the key must survive the daemon's own dead-lettering");
});
