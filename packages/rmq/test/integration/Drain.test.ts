import { Effect } from "effect";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { Rmq } from "../../src/Client.ts";
import { broker, skipIfNoDocker, startBroker, stopBroker, waitFor } from "./harness.ts";

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

/**
 * What a breaker opening does to a consumer that is mid-call: `closeConsumer` hands its unacked deliveries back,
 * so calls that had already finished would run again; `drainConsumer` lets them settle first and then leaves.
 */
test("draining a consumer settles what it holds, redelivers none of it, and takes nothing more", async (t) => {
  if (skipIfNoDocker(t)) return;

  const seen: string[] = [];
  const redelivered: string[] = [];
  const depth = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue("drain.work");
      const pub = yield* rmq.publisherToQueue("drain.work");
      yield* Effect.forEach(["a", "b", "c", "d", "e"], (m) => rmq.send(pub, m));
      const consumer = yield* rmq.consume(
        "drain.work",
        async (bytes, delivery) => {
          const body = bytes.toString();
          seen.push(body);
          delivery.deliveryCount > 0 && redelivered.push(body);
          await new Promise((r) => setTimeout(r, 300));
        },
        { prefetch: 2 }
      );
      yield* waitFor(() => seen.length >= 1);
      yield* rmq.drainConsumer(consumer);
      const held = seen.length;
      yield* Effect.sleep(700);
      return { held, after: seen.length };
    })
  );

  assert.equal(depth.after, depth.held, "nothing is delivered after the drain");
  const remaining = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      const left: string[] = [];
      yield* rmq.consume("drain.work", (body) => void left.push(body.toString()), { prefetch: 10 });
      yield* waitFor(() => left.length >= 5 - seen.length);
      yield* Effect.sleep(300);
      return left;
    })
  );
  assert.equal(
    remaining.length + seen.length,
    5,
    "every message is either settled by the drained consumer or still queued"
  );
  assert.deepEqual(redelivered, [], "and none it finished came back");
});
