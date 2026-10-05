import { AMQPClient } from "@cloudamqp/amqp-client";
import { Effect } from "effect";
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import { Rmq } from "../../src/Client.ts";
import * as Delay from "../../src/DelayedDelivery.ts";
import { broker, skipIfNoDocker, startBroker, stopBroker, text, waitFor } from "./harness.ts";

/**
 * The delay chain against a real broker: what arrives, where, and how late. Real time, because a broker's TTL
 * check is not something a fake clock can stand in for.
 */

beforeAll(startBroker);
afterAll(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

test("a delayed message arrives once, at its own destination, no earlier than asked", async (t) => {
  if (skipIfNoDocker(t)) return;

  // 1s enters and leaves level 0; 5s (101) waits in level 2, is passed
  // through level 1, then waits in level 0 — every kind of hop.
  const delays = [1, 2, 5];
  const arrivals: Array<{ body: string; queue: string; at: number; }> = [];
  const sentAt = Date.now();

  await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* Delay.declare();
      yield* Effect.forEach(["delay.a", "delay.b"], (queue) =>
        Effect.gen(function*() {
          yield* rmq.declareQueue(queue);
          yield* Delay.receive(queue);
          yield* rmq.consume(queue, (body) => void arrivals.push({ body: text(body), queue, at: Date.now() - sentAt }));
        }));
      yield* Effect.forEach(delays, (s) => Delay.sendDelayed("delay.a", s, `a:${s}`));
      yield* Delay.sendDelayed("delay.b", 2, "b:2");
      yield* waitFor(() => arrivals.length >= 4, 15_000);
    })
  );

  assert.equal(arrivals.length, 4, JSON.stringify(arrivals));
  for (const { body, queue, at } of arrivals) {
    const [dest, seconds] = body.split(":");
    assert.equal(queue, `delay.${dest}`, "each message reaches only the queue it was addressed to");
    assert.ok(at >= Number(seconds) * 1000 - 100, `${body} arrived early: ${at}ms`);
    await t.annotate(`${body} arrived after ${at}ms`);
    assert.ok(at < Number(seconds) * 1000 + 3_000, `${body} arrived late: ${at}ms`);
  }
});

test("a delay near a day is accepted into the top of the chain and holds there", async (t) => {
  if (skipIfNoDocker(t)) return;

  const depth = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* Delay.declare();
      yield* rmq.declareQueue("delay.day");
      yield* Delay.receive("delay.day");
      yield* Delay.sendDelayed("delay.day", 86_400, "tomorrow");
      return yield* Effect.promise(async () => {
        const client = new AMQPClient(`amqp://${broker.host}:${broker.port}`);
        await client.connect();
        const ch = await client.channel();
        const top = await ch.queueDeclare(Delay.levelName(Delay.entryLevel(86_400)), { passive: true });
        const dest = await ch.queueDeclare("delay.day", { passive: true });
        await client.close();
        return { top: top.messageCount, dest: dest.messageCount };
      });
    })
  );

  assert.deepEqual(depth, { top: 1, dest: 0 });
});
