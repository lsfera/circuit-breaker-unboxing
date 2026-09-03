import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { Rmq, RmqLive } from "../../src/Client.ts";

/**
 * Regression coverage for the one thing about this client that is actively
 * dangerous: creating links concurrently on a shared connection silently
 * misroutes. Both halves below fail without the semaphore inside `RmqLive`
 * — publishers all deliver to the first publisher's target, and consumers
 * all receive the first consumer's queue — with no error raised either
 * time, which is what makes it worth a permanent test rather than a note.
 *
 * Opt-in (`pnpm run test:rmq`), same shape as @egress/aggregator's
 * Redis integration test: needs Docker, uses a real
 * `rabbitmq:4.0-management-alpine`, and skips rather than fails when Docker
 * is unavailable. The skip is checked inside each test body, not as a
 * static `test(...)` option, because Docker availability is only known
 * after `before()` has run.
 */

let container: StartedTestContainer | null = null;
let host = "";
let port = 0;
let dockerAvailable = true;

before(async () => {
  try {
    container = await new GenericContainer("rabbitmq:4.0-management-alpine")
      .withExposedPorts(5672)
      .withWaitStrategy(Wait.forLogMessage(/Server startup complete/))
      .start();
  } catch {
    dockerAvailable = false;
    return;
  }
  host = container.getHost();
  port = container.getMappedPort(5672);
});

after(async () => {
  await container?.stop().catch(() => {});
});

const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
  if (dockerAvailable) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, RmqLive({ host, port }))) as Effect.Effect<A>,
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
        yield* rmq.consume(`pub.${api}`, (body) => received[api]!.push(body));
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
        queues.map((q) => rmq.consume(q, (body) => received[q]!.push(body))),
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

test("x-single-active-consumer elects one consumer and promotes another when it closes", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "sac.probe-trigger";
  const received: Array<{ id: string; body: string }> = [];

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue, { "x-single-active-consumer": true });

      const consumers: Record<string, Awaited<ReturnType<typeof Effect.runPromise>>> = {};
      for (const id of ["a", "b", "c"]) {
        consumers[id] = yield* rmq.consume(queue, (body) => received.push({ id, body }));
      }

      const pub = yield* rmq.publisherToQueue(queue);
      for (let i = 0; i < 4; i++) yield* rmq.send(pub, `trigger-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1200)));

      const active = [...new Set(received.map((r) => r.id))];
      assert.equal(active.length, 1, `exactly one consumer should be active, got ${active}`);

      // Closing the active one must hand the role to a different consumer,
      // with no election code of our own.
      const activeId = active[0]!;
      yield* rmq.closeConsumer(consumers[activeId] as never);
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

      const consumer = yield* rmq.consume(queue, (body) => received.push(body));
      for (let i = 0; i < 3; i++) yield* rmq.send(pub, `before-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 3, "consumer receives while open");

      // This is what OPEN does: stop pulling, keep the connection.
      yield* rmq.closeConsumer(consumer);
      for (let i = 0; i < 3; i++) yield* rmq.send(pub, `during-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 3, "nothing is delivered while cancelled");

      // And this is what recovery does — on the same, still-open connection.
      yield* rmq.consume(queue, (body) => received.push(body));
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 6, "the queued messages arrive once consuming resumes");
    }),
  );
});
