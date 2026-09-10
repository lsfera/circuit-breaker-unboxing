import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Exit, Scope } from "effect";
import {
  broker,
  brokerExec,
  skipIfNoDocker,
  startBroker,
  stopBroker,
  waitFor,
} from "./harness.ts";
import { makeRmq, Rmq, RmqLive } from "../../src/Client.ts";
import type { Consumer } from "../../src/Client.ts";

/**
 * The broker- and channel-level properties the daemon fleet is built on,
 * each pinned against a real RabbitMQ.
 *
 * The first two are inherited from the AMQP 1.0 client, where creating links
 * concurrently on a shared connection silently misrouted every message — a
 * defect that needed a connection-wide semaphore to avoid. On amqplib there
 * are no publisher links to race, so they pass by construction; they stay
 * because "by construction" is a claim, and this is the thing that checks it.
 *
 * Opt-in (`pnpm run test:rmq`), same shape as @egress/aggregator's Redis
 * integration test: needs Docker, runs against a real broker, and skips
 * rather than fails when Docker is unavailable. `harness.ts` owns which
 * broker and how the skip works.
 */

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, RmqLive({ host: broker.host, port: broker.port }))) as Effect.Effect<A>,
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

      // The case that silently misroutes without the client's semaphore.
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

      // The case that silently cross-wires without the client's semaphore.
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

/**
 * Recovery, and the half of it amqplib does not do.
 *
 * `recovery` reopens the socket and stops there: channels are not recreated
 * and consumers are not re-registered, so a client that leaned on it alone
 * would come back connected and consuming nothing — the same zombie as no
 * recovery, only harder to spot. `@egress/rmq` records what it was asked to
 * build and rebuilds it in amqplib's `setup` hook.
 *
 * The connection is killed from the broker side rather than by restarting the
 * container, because that is the failure this is about — the socket going away
 * under a process that is otherwise fine — and because it leaves the mapped
 * port alone, so the test is measuring recovery rather than Docker.
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

      // Publishing is what proves it: `send` opens a publish channel on the
      // recovered connection, and the consumer that receives it was rebuilt by
      // the setup hook rather than by anything in this test.
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

/**
 * amqplib recovers connections, not channels. A channel that dies on its own —
 * a protocol error, a queue deleted, a settle on a tag the broker has already
 * seen — takes its consumer with it and leaves the connection healthy, so
 * nothing else notices. The handle the caller holds still looks live, which is
 * how a process goes deaf while reporting itself well: measured before the
 * fix, the consumer below received nothing again, ever, and said nothing.
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
 * A repair budget was worse than no budget, and this is the case that showed it.
 *
 * The election queues are idle by design — being registered and empty *is* their
 * job — so a budget reset by deliveries never reset on them. Six channel deaths
 * over the life of a process and the daemon left the election for good, over a
 * condition the next rebuild fixed immediately.
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
 * The other half of that: a consumer retired on purpose must stay retired.
 *
 * Recovery rebuilds every consumer the client still considers live, so the
 * teardown paths drop theirs first. Without that, a daemon that closed its work
 * consumer because the circuit went OPEN would come back consuming work the
 * moment the broker restarted — pulling from a queue the whole fleet has agreed
 * to leave alone, and reporting itself idle while it did.
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
 * A publish channel with no way back is a single point of failure, and a quiet
 * one. amqplib closes a channel on any channel-level error, and publishing to
 * an exchange that does not exist is enough to cause one — RabbitMQ replies
 * 404 NOT_FOUND and closes it. The send that caused it does not fail, because
 * a plain publish is fire-and-forget, so nothing at the call site notices.
 *
 * Before the channel could reopen, that one bad publish ended publishing for
 * the entire connection: every later send threw on a dead channel while the
 * process stayed up and every other signal stayed green. For a daemon that
 * would mean no probe triggers, no redrive triggers and no replayed work, with
 * a heartbeat still saying it was fine.
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
 * The second silent failure mode, and the reason @egress/rmq-consumer's
 * daemon runs its work consumers on a connection separate from its
 * control-plane one.
 *
 * Closing a consumer that still has deliveries in flight strands them, and
 * enough strandings stall *every* link on that connection — including
 * consumers on unrelated queues that were never touched. The daemon's
 * HALF_OPEN probe is exactly this shape: open onto a deep backlog, take one
 * message, close.
 *
 * Both halves are asserted together on purpose. The first pins the hazard as
 * the property the daemon fleet depends on. It used to assert the opposite:
 * on the AMQP 1.0 client this loop killed a shared connection within a dozen
 * cycles, and that failure is what put a two-connection topology in daemon.ts.
 * The move to amqplib is what changed it — every consumer gets its own
 * channel, so a consumer cancelled with deliveries outstanding costs the
 * broker a requeue and costs its neighbours nothing.
 *
 * Kept running both ways round because the daemon still opens a connection per
 * probe and per redrive pass: that is now about being able to abandon work
 * wholesale, not about damage control, and it must keep working either way.
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

      // Stands in for the daemon's control-plane subscription: never closed,
      // never touched, on a queue the probe knows nothing about.
      let canaryCount = 0;
      yield* rmq.consume(canary, () => void canaryCount++);
      for (let i = 0; i < BACKLOG; i++) yield* rmq.send(workPub, `w-${i}`);

      for (let cycle = 1; cycle <= CYCLES; cycle++) {
        const scope = isolated ? yield* Scope.make() : null;
        const conn =
          scope === null
            ? rmq
            : yield* Effect.provideService(makeRmq({ host: broker.host, port: broker.port }), Scope.Scope, scope);

        // Exactly daemon.ts's ordering: the consumer is cancelled inline,
        // which is what stops delivery at the first message, and the
        // connection — when there is a separate one — is retired afterwards
        // from outside the handler.
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
