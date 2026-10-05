import { Effect } from "effect";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { Rmq } from "../../src/Client.ts";
import * as Delay from "../../src/DelayedDelivery.ts";
import { broker, skipIfNoDocker, startBroker, stopBroker, text, waitFor } from "./harness.ts";

/**
 * The delay chain against a real broker: what arrives, where, and how late. Real time, because a broker's TTL
 * check is not something a fake clock can stand in for.
 */

before(startBroker);
after(stopBroker);

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
  arrivals.forEach(({ body, queue, at }) => {
    const [dest, seconds] = body.split(":");
    assert.equal(queue, `delay.${dest}`, "each message reaches only the queue it was addressed to");
    assert.ok(at >= Number(seconds) * 1000 - 100, `${body} arrived early: ${at}ms`);
    t.diagnostic(`${body} arrived after ${at}ms`);
    assert.ok(at < Number(seconds) * 1000 + 3_000, `${body} arrived late: ${at}ms`);
  });
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
        const { connect } = await import("amqplib");
        const conn = await connect({ hostname: broker.host, port: broker.port });
        const ch = await conn.createChannel();
        const top = await ch.checkQueue(Delay.levelName(Delay.entryLevel(86_400)));
        const dest = await ch.checkQueue("delay.day");
        await conn.close();
        return { top: top.messageCount, dest: dest.messageCount };
      });
    })
  );

  assert.deepEqual(depth, { top: 1, dest: 0 });
});
