import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Exit, Option as O, Scope } from "effect";
import {
  broker,
  brokerExec,
  skipIfNoDocker,
  startBroker,
  stopBroker,
  waitFor,
} from "./harness.ts";
import { isUnroutable, makeRmq, Rmq, RmqError } from "../../src/Client.ts";
import type { Consumer } from "../../src/Client.ts";

/**
 * The broker- and channel-level properties the daemon fleet is built on, each pinned against a real RabbitMQ.
 * Opt-in (`pnpm run test:rmq`): needs Docker, and skips rather than fails when it is unavailable; `harness.ts`
 * owns which broker and how the skip works.
 */

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>,
  );

test("concurrent publisher creation routes each message to its own binding", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apis = ["alpha", "beta", "gamma"];
  const received: Record<string, string[]> = { alpha: [], beta: [], gamma: [] };

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      const exchange = yield* rmq.declareTopicExchange("pub.concurrency");
      for (const api of apis) {
        const q = yield* rmq.declareQueue(`pub.${api}`);
        yield* rmq.bind(`key.${api}`, exchange, q);
        yield* rmq.consume(`pub.${api}`, (body) => void received[api]!.push(body));
      }

      // Publishers created concurrently must each keep their own routing key.
      const publishers = yield* Effect.all(
        apis.map((api) => rmq.publisherToExchange("pub.concurrency", `key.${api}`)),
        { concurrency: "unbounded" },
      );
      yield* Effect.all(
        publishers.map((pub, i) => rmq.send(pub, `msg-${apis[i]}`)),
        { concurrency: "unbounded" },
      );
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1500)));
    }),
  );

  for (const api of apis) {
    assert.deepEqual(received[api], [`msg-${api}`], `${api} should receive exactly its own message`);
  }
});

test("concurrent consumer creation binds each consumer to its own queue", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queues = ["con.one", "con.two", "con.three"];
  const received: Record<string, string[]> = { "con.one": [], "con.two": [], "con.three": [] };

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      for (const q of queues) yield* rmq.declareQueue(q);

      // Consumers registered concurrently must each keep their own queue.
      yield* Effect.all(
        queues.map((q) => rmq.consume(q, (body) => void received[q]!.push(body))),
        { concurrency: "unbounded" },
      );

      for (const q of queues) {
        const pub = yield* rmq.publisherToQueue(q);
        yield* rmq.send(pub, `msg-${q}`);
      }
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1500)));
    }),
  );

  for (const q of queues) {
    assert.deepEqual(received[q], [`msg-${q}`], `${q} should receive exactly its own message`);
  }
});

test("a publisher's declared content type and encoding reach the consumer, and an undeclared publisher's arrive as none", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `content-type.${Date.now()}`;
  const seen: Array<{ body: string; contentType: O.Option<string>; contentEncoding: O.Option<string> }> = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      yield* rmq.consume(queue, (body, d) => void seen.push({ body, contentType: d.contentType, contentEncoding: d.contentEncoding }));
      const declares = yield* rmq.publisherToQueue(queue, { contentType: "application/json", contentEncoding: "gzip" });
      yield* rmq.send(declares, "declared");
      yield* rmq.send(yield* rmq.publisherToQueue(queue), "undeclared");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1000)));
    }),
  );

  const byBody = Object.fromEntries(seen.map((m) => [m.body, m]));
  assert.deepEqual(byBody["declared"]?.contentType, O.some("application/json"));
  assert.deepEqual(byBody["declared"]?.contentEncoding, O.some("gzip"));
  assert.deepEqual(byBody["undeclared"]?.contentType, O.none());
  assert.deepEqual(byBody["undeclared"]?.contentEncoding, O.none());
});

test("send stamps a message id and timestamp, and a declared type reaches the consumer", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `stamps.${Date.now()}`;
  const before = Date.now() - 1000;
  const seen: Array<{ type: O.Option<string>; messageId: O.Option<string>; publishedAt: O.Option<number> }> = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      yield* rmq.consume(queue, (_body, d) => void seen.push({ type: d.type, messageId: d.messageId, publishedAt: d.publishedAt }));
      yield* rmq.send(yield* rmq.publisherToQueue(queue, { type: "egress.work" }), "one");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
    }),
  );

  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0]!.type, O.some("egress.work"));
  assert.equal(O.isSome(seen[0]!.messageId), true);
  assert.equal(O.isSome(seen[0]!.publishedAt) && seen[0]!.publishedAt.value >= before, true);
});

test("a publish to a queue that does not exist fails as unroutable instead of vanishing", async (t) => {
  if (skipIfNoDocker(t)) return;

  const exit = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      return yield* Effect.exit(rmq.send(yield* rmq.publisherToQueue(`nobody.home.${Date.now()}`), "lost?"));
    }),
  );

  assert.equal(Exit.isFailure(exit), true);
  assert.equal(Exit.isFailure(exit) && exit.cause.reasons.some((r) => r._tag === "Fail" && r.error instanceof RmqError && isUnroutable(r.error)), true);
});

test("a publish to an existing queue, and to a topic with no bindings, still succeeds", async (t) => {
  if (skipIfNoDocker(t)) return;

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      const queue = `routable.${Date.now()}`;
      yield* rmq.declareQueue(queue);
      yield* rmq.send(yield* rmq.publisherToQueue(queue), "ok");
      // An exchange with nothing bound is an ordinary state for a topic, so its
      // publishers are not mandatory: the message is routed nowhere, by design.
      const exchange = `unbound.${Date.now()}`;
      yield* rmq.declareTopicExchange(exchange);
      yield* rmq.send(yield* rmq.publisherToExchange(exchange, "anything"), "also ok");
    }),
  );
});

test("a handler that throws dead-letters the delivery instead of acknowledging it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const stamp = Date.now();
  const work = `throws.${stamp}`;
  const dead = `throws.${stamp}.dead`;
  const deadLettered: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead);
      yield* rmq.declareQueue(work, {
        args: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead },
      });
      yield* rmq.consume(dead, (body) => void deadLettered.push(body));
      yield* rmq.consume(work, () => {
        throw new Error("boom");
      });
      yield* rmq.send(yield* rmq.publisherToQueue(work), "poison");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1000)));
    }),
  );

  assert.deepEqual(deadLettered, ["poison"]);
});

/**
 * RabbitMQ's alarms block a connection that publishes by ceasing to read from it. A consumer sharing that
 * connection would never have its acks read and would stall once its prefetch window filled: a prefetch of 2
 * against 12 waiting messages, where a shared connection delivers exactly two and stops.
 */
test("a consumer keeps acknowledging while the client's publishing connection is blocked", async (t) => {
  if (skipIfNoDocker(t)) return;

  const stamp = Date.now();
  const inbox = `alarm.in.${stamp}`;
  const outbox = `alarm.out.${stamp}`;
  const received: string[] = [];
  const settled = { publish: false };

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(inbox);
      yield* rmq.declareQueue(outbox);
      const toInbox = yield* rmq.publisherToQueue(inbox);
      for (let i = 0; i < 12; i++) yield* rmq.send(toInbox, `m${i}`);

      yield* Effect.promise(() => brokerExec(["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", "1MiB"]));
      try {
        yield* Effect.promise(() => new Promise((r) => setTimeout(r, 2000)));
        // A publish issued under the alarm is what gets the publishing connection blocked.
        yield* Effect.forkChild(
          rmq.send(yield* rmq.publisherToQueue(outbox), "blocked").pipe(Effect.tap(() => Effect.sync(() => void (settled.publish = true)))),
        );
        yield* Effect.promise(() => new Promise((r) => setTimeout(r, 2000)));

        yield* rmq.consume(inbox, (body) => void received.push(body), { prefetch: 2 });
        yield* Effect.promise(() => new Promise((r) => setTimeout(r, 3000)));
        assert.equal(settled.publish, false, "the alarm should be holding the publish");
        assert.equal(received.length, 12, "every message should be delivered and acknowledged despite the alarm");
      } finally {
        yield* Effect.promise(() => brokerExec(["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", "1GiB"]));
      }
    }),
  );
});

/**
 * Recovery, and the half of it amqplib does not do: `recovery` reopens the socket and stops there, so a client
 * that leaned on it alone would come back connected and consuming nothing. `@egress/rmq` records what it was
 * asked to build and rebuilds it in amqplib's `setup` hook. The connection is killed from the broker side, not
 * by restarting the container, so the test measures recovery rather than Docker.
 */
test("a killed connection comes back with its consumers still registered", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `recover.${Date.now()}`;
  const seen: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { durable: true });
      const pub = yield* rmq.publisherToQueue(queue);
      yield* rmq.consume(queue, (body) => void seen.push(body));

      yield* rmq.send(pub, "before");
      yield* waitFor(() => seen.length >= 1);

      // Severs every connection the broker holds, ours included.
      yield* Effect.promise(() =>
        brokerExec(["rabbitmqctl", "close_all_connections", "recovery test"]),
      );

      // Publishing proves it: `send` opens a publish channel on the recovered connection, and the consumer that
      // receives it was rebuilt by the setup hook.
      yield* waitFor(() => false, 3000);
      yield* rmq.send(pub, "after");
      yield* waitFor(() => seen.length >= 2);
    }),
  );

  assert.deepEqual(
    seen,
    ["before", "after"],
    "the consumer registered before the connection died must still be delivering after it",
  );
});

/** `resetConnection` destroys the socket from our side; that only helps if amqplib notices and recovers. */
test("resetConnection drops the connection and the client recovers from it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `reset.${Date.now()}`;
  const seen: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { durable: true });
      const pub = yield* rmq.publisherToQueue(queue);
      yield* rmq.consume(queue, (body) => void seen.push(body));

      yield* rmq.send(pub, "before");
      yield* waitFor(() => seen.length >= 1);

      yield* rmq.resetConnection;
      // Reconnect starts at 200ms, so sample closely rather than after a sleep.
      let dropped = false;
      for (let i = 0; i < 100 && !dropped; i++) {
        dropped = !(yield* rmq.isConnected);
        yield* waitFor(() => false, 5);
      }
      assert.ok(dropped, "the reset must actually drop the connection");

      yield* waitFor(() => false, 3000);
      yield* rmq.send(pub, "after");
      yield* waitFor(() => seen.length >= 2);
    }),
  );

  assert.deepEqual(seen, ["before", "after"]);
});

/**
 * amqplib recovers connections, not channels. A channel that dies alone (protocol error, deleted queue, settle
 * on a known tag) takes its consumer with it and leaves the connection healthy, so the handle the caller holds
 * still looks live while the process goes deaf.
 */
test("a consumer whose channel dies alone is put back", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `channel-death.${Date.now()}`;
  const seen: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, {});
      const pub = yield* rmq.publisherToQueue(queue);
      const consumer = yield* rmq.consume(queue, (body) => void seen.push(body));

      yield* rmq.send(pub, "before");
      yield* waitFor(() => seen.length >= 1);

      // Exactly what a channel-level error does, without needing to provoke one.
      yield* Effect.promise(() => consumer.channel.close().then(() => {}, () => {}));

      yield* rmq.send(pub, "after");
      yield* waitFor(() => seen.length >= 2);
    }),
  );

  assert.deepEqual(seen, ["before", "after"], "the consumer must survive losing its channel");
});

/**
 * A repair budget was worse than none: an idle queue is never reset by deliveries, so a few channel deaths
 * would abandon it for good over a condition the next rebuild fixes immediately.
 */
test("a consumer on a queue that never delivers is still repaired", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `idle-election.${Date.now()}`;
  const seen: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { durable: true });
      const pub = yield* rmq.publisherToQueue(queue);
      const consumer = yield* rmq.consume(queue, (body) => void seen.push(body));

      for (let i = 0; i < 6; i++) {
        yield* Effect.promise(() => consumer.channel.close().then(() => {}, () => {}));
        yield* waitFor(() => false, 400);
      }

      yield* rmq.send(pub, "trigger");
      yield* waitFor(() => seen.length >= 1, 5000);
    }),
  );

  assert.deepEqual(seen, ["trigger"], "an idle consumer must still be a candidate after repairs");
});

/**
 * A consumer retired on purpose must stay retired. Recovery rebuilds every consumer the client still considers
 * live, so the teardown paths drop theirs first; otherwise a consumer closed because the breaker opened would
 * come back consuming the moment the broker restarted.
 */
test("a consumer closed on purpose is not resurrected by a reconnect", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `retired.${Date.now()}`;
  const seen: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { durable: true });
      const pub = yield* rmq.publisherToQueue(queue);
      const consumer = yield* rmq.consume(queue, (body) => void seen.push(body));

      yield* rmq.send(pub, "before");
      yield* waitFor(() => seen.length >= 1);

      yield* rmq.closeConsumer(consumer);

      yield* Effect.promise(() =>
        brokerExec(["rabbitmqctl", "close_all_connections", "retired consumer test"]),
      );
      yield* waitFor(() => false, 3000);

      // The publish also proves the connection came back, so "nothing arrived"
      // cannot be mistaken for "nothing was published".
      yield* rmq.send(pub, "after");
      yield* waitFor(() => seen.length >= 2, 3000);
    }),
  );

  assert.deepEqual(seen, ["before"], "a retired consumer must not come back with the connection");
});

/**
 * A publish channel with no way back is a quiet single point of failure. Publishing to an exchange that does
 * not exist makes RabbitMQ reply 404 NOT_FOUND and close the channel; the send that caused it does not fail (a
 * plain publish is fire-and-forget), and without a reopen every later send would throw on a dead channel while
 * every other signal stayed green.
 */
test("a poisoned publish channel reopens rather than ending publishing", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = `reopen.${Date.now()}`;

  const seen = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      const good = yield* rmq.publisherToQueue(queue);

      // Prove the connection works, so a failure below is about the reopen and
      // not about the setup.
      yield* rmq.send(good, "before the error");

      const poison = yield* rmq.publisherToExchange("no.such.exchange", "irrelevant");
      yield* Effect.ignore(rmq.send(poison, "into the void"));
      // The channel dies asynchronously, after the broker's reply arrives.
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 500)));

      yield* rmq.send(good, "after the error");

      const received: string[] = [];
      yield* rmq.consume(queue, (body) => void received.push(body));
      yield* waitFor(() => received.length >= 2);
      return received;
    }),
  );

  assert.deepEqual(
    seen,
    ["before the error", "after the error"],
    "the send after the channel error must still arrive — on a reopened channel",
  );
});

test("x-single-active-consumer elects one consumer and promotes another when it closes", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "sac.probe-trigger";
  const received: Array<{ id: string; body: string }> = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { args: { "x-single-active-consumer": true } });

      const consumers: Record<string, Awaited<ReturnType<typeof Effect.runPromise>>> = {};
      for (const id of ["a", "b", "c"]) {
        consumers[id] = yield* rmq.consume(queue, (body) => void received.push({ id, body }));
      }

      const pub = yield* rmq.publisherToQueue(queue);
      for (let i = 0; i < 4; i++) yield* rmq.send(pub, `trigger-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1200)));

      const active = [...new Set(received.map((r) => r.id))];
      assert.equal(active.length, 1, `exactly one consumer should be active, got ${active}`);

      // Closing the active one must hand the role to a different consumer,
      // with no election code of our own.
      const activeId = active[0]!;
      yield* rmq.cancelConsumer(consumers[activeId] as never);
      received.length = 0;
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      for (let i = 4; i < 8; i++) yield* rmq.send(pub, `trigger-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1200)));

      const promoted = [...new Set(received.map((r) => r.id))];
      assert.equal(promoted.length, 1, `exactly one consumer should be active after promotion, got ${promoted}`);
      assert.notEqual(promoted[0], activeId, "a different consumer should have been promoted");
    }),
  );
});

test("closing a consumer stops delivery without closing the connection", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "cancel.work";
  const received: string[] = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      const pub = yield* rmq.publisherToQueue(queue);

      const consumer = yield* rmq.consume(queue, (body) => void received.push(body));
      for (let i = 0; i < 3; i++) yield* rmq.send(pub, `before-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 3, "consumer receives while open");

      // This is what OPEN does: stop pulling, keep the connection.
      yield* rmq.cancelConsumer(consumer);
      for (let i = 0; i < 3; i++) yield* rmq.send(pub, `during-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 3, "nothing is delivered while cancelled");

      // And this is what recovery does — on the same, still-open connection.
      yield* rmq.consume(queue, (body) => void received.push(body));
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 6, "the queued messages arrive once consuming resumes");
    }),
  );
});

/**
 * Closing a consumer that still has deliveries in flight strands them; on a shared connection enough strandings
 * would stall every link, including consumers on unrelated queues. Each consumer has its own channel here, so a
 * cancelled consumer costs the broker a requeue and its neighbours nothing. Asserted both ways round (shared
 * connection and a connection per probe): retiring a whole connection to abandon work must work either way.
 */
const CYCLES = 12;
const BACKLOG = 4000;

test("closing a consumer with deliveries in flight leaves the rest of the connection alone", async (t) => {
  if (skipIfNoDocker(t)) return;

  const probeCycles = (isolated: boolean) =>
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      const tag = isolated ? "iso" : "shared";
      const work = `strand.work.${tag}`;
      const canary = `strand.canary.${tag}`;
      yield* rmq.declareQueue(work);
      yield* rmq.declareQueue(canary);
      const workPub = yield* rmq.publisherToQueue(work);
      const canaryPub = yield* rmq.publisherToQueue(canary);

      // Stands in for a subscription that is never closed or touched, on a queue the probe knows nothing about.
      let canaryCount = 0;
      yield* rmq.consume(canary, () => void canaryCount++);
      for (let i = 0; i < BACKLOG; i++) yield* rmq.send(workPub, `w-${i}`);

      for (let cycle = 1; cycle <= CYCLES; cycle++) {
        const scope = isolated ? yield* Scope.make() : null;
        const conn =
          scope === null
            ? rmq
            : yield* Effect.provideService(makeRmq({ host: broker.host, port: broker.port }), Scope.Scope, scope);

        // The consumer is cancelled inline, which stops delivery at the first message, and the connection (when there
        // is a separate one) is retired afterwards from outside the handler.
        let self: Consumer | null = null;
        let taken = false;
        const consumer = yield* conn.consume(work, () => {
          if (taken || self === null) return;
          taken = true;
          Effect.runFork(conn.cancelConsumer(self));
        });
        self = consumer;
        yield* Effect.promise(() => new Promise((r) => setTimeout(r, 600)));
        if (scope !== null) yield* Scope.close(scope, Exit.void);

        const before = canaryCount;
        yield* rmq.send(canaryPub, `ping-${cycle}`);
        yield* Effect.promise(() => new Promise((r) => setTimeout(r, 600)));
        if (canaryCount === before) return cycle; // the connection went deaf
      }
      return null;
    });

  const sharedWedgedAt = await run(probeCycles(false));
  assert.equal(
    sharedWedgedAt,
    null,
    `a consumer sharing the connection went deaf at cycle ${sharedWedgedAt}. ` +
      "That was the AMQP 1.0 behaviour this fleet was built around; on a " +
      "channel-per-consumer client it must not happen at all",
  );

  const isolatedWedgedAt = await run(probeCycles(true));
  assert.equal(
    isolatedWedgedAt,
    null,
    `a connection per probe stalled at cycle ${isolatedWedgedAt}, which is what the daemon relies on not happening`,
  );
});

test("get is a non-blocking fetch: empty returns None, and an unsettled message blocks a second get", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "get.permit";

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      // The permit-queue shape article/03 actually declares: exactly one
      // token, ever — `get` doesn't need that to behave, but this is the
      // real caller.
      yield* rmq.declareQueue(queue, {
        args: { "x-max-length": 1, "x-overflow": "reject-publish" },
      });

      assert.equal(O.isNone(yield* rmq.get(queue)), true, "an empty queue returns None");

      const pub = yield* rmq.publisherToQueue(queue);
      yield* rmq.send(pub, "token");

      const first = yield* rmq.get(queue);
      assert.equal(O.isSome(first), true);
      assert.equal(O.getOrThrow(first).body, "token");

      // Fetched but not yet settled: a second get must not see it too — get
      // is exclusive access to the one message, not a peek.
      assert.equal(
        O.isNone(yield* rmq.get(queue)),
        true,
        "an unsettled message must not be handed to a second get",
      );

      yield* O.getOrThrow(first).nack;
      const afterNack = yield* rmq.get(queue);
      assert.equal(O.isSome(afterNack), true, "nack must requeue it for the next get");
      yield* O.getOrThrow(afterNack).ack;

      assert.equal(O.isNone(yield* rmq.get(queue)), true, "ack must remove it for good");
    }),
  );
});
