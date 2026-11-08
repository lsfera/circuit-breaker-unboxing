import { Effect } from "effect";
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import { Rmq } from "../../src/Client.ts";
import * as DelayedQueue from "../../src/DelayedQueue.ts";
import { broker, skipIfNoDocker, startBroker, stopBroker, text, waitFor } from "./harness.ts";

/**
 * A quorum queue's own delayed delivery (RabbitMQ 4.4) against a real broker: that it is held until due, not before,
 * and that the version the client reports is the one the consumer chooses it by. Real time, as in
 * `DelayedDelivery.test.ts`.
 */

beforeAll(startBroker);
afterAll(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

test("the broker reports a version that supports delayed queues", async (t) => {
  if (skipIfNoDocker(t)) return;
  const version = await run(Effect.gen(function*() {
    return (yield* Rmq).brokerVersion;
  }));
  assert.ok(DelayedQueue.supportedBy(version), `broker version ${JSON.stringify(version)}`);
});

test("a delayed message reaches only its queue, no earlier than asked", async (t) => {
  if (skipIfNoDocker(t)) return;

  const arrivals: Array<{ body: string; queue: string; at: number; }> = [];
  const sentAt = Date.now();

  await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* Effect.forEach(["dq.a", "dq.b"], (queue) =>
        Effect.gen(function*() {
          yield* DelayedQueue.declare(queue);
          yield* rmq.consume(queue, (body) => void arrivals.push({ body: text(body), queue, at: Date.now() - sentAt }));
        }));
      yield* Effect.forEach(
        [1, 3],
        (s) => DelayedQueue.sendDelayed("dq.a", s, `a:${s}`, { headers: { attempt: "7" } })
      );
      yield* DelayedQueue.sendDelayed("dq.b", 2, "b:2");
      yield* waitFor(() => arrivals.length >= 3, 15_000);
    })
  );

  assert.equal(arrivals.length, 3, JSON.stringify(arrivals));
  for (const { body, queue, at } of arrivals) {
    const [dest, seconds] = body.split(":");
    assert.equal(queue, `dq.${dest}`);
    assert.ok(at >= Number(seconds) * 1000 - 100, `${body} arrived early: ${at}ms`);
    await t.annotate(`${body} arrived after ${at}ms`);
    assert.ok(at < Number(seconds) * 1000 + 3_000, `${body} arrived late: ${at}ms`);
  }
});

test("a delayed message keeps its other headers", async (t) => {
  if (skipIfNoDocker(t)) return;

  const seen: Array<string> = [];
  await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* DelayedQueue.declare("dq.headers");
      yield* rmq.consume("dq.headers", (_body, delivery) => void seen.push(String(delivery.properties.attempt)));
      yield* DelayedQueue.sendDelayed("dq.headers", 1, "wake", { headers: { attempt: "7" } });
      yield* waitFor(() => seen.length >= 1, 10_000);
    })
  );
  assert.deepEqual(seen, ["7"]);
});
