import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Exit, Scope } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { makeRmq, Rmq, RmqLive } from "../../src/Client.ts";
import type { Consumer } from "../../src/Client.ts";

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

      const consumer = yield* rmq.consume(queue, (body) => void received.push(body));
      for (let i = 0; i < 3; i++) yield* rmq.send(pub, `before-${i}`);
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 800)));
      assert.equal(received.length, 3, "consumer receives while open");

      // This is what OPEN does: stop pulling, keep the connection.
      yield* rmq.closeConsumer(consumer);
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
 * a live property of the pinned client, so that if a future version fixes it
 * this test fails and tells us the workaround can go. The second pins that
 * the workaround actually works — a connection per probe survives the same
 * loop that kills the shared one.
 */
const CYCLES = 12;
const BACKLOG = 4000;

test("closing a consumer with deliveries in flight stalls the whole connection", async (t) => {
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
            : yield* Effect.provideService(makeRmq({ host, port }), Scope.Scope, scope);

        // Exactly daemon.ts's ordering, and for its reason: the consumer is
        // closed inline (that is what stops delivery at the first message),
        // and the connection — when there is a separate one — is retired
        // afterwards, outside the handler. Tearing a connection down from
        // inside a message callback throws `transfer after detach`.
        let self: Consumer | null = null;
        let taken = false;
        const consumer = yield* conn.consume(work, () => {
          if (taken || self === null) return;
          taken = true;
          Effect.runFork(conn.closeConsumer(self));
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

  const wedgedAt = await run(probeCycles(false));
  assert.notEqual(
    wedgedAt,
    null,
    `expected the shared connection to stall within ${CYCLES} probe cycles; ` +
      "if this now survives, the client may have fixed it and the daemon's " +
      "two-connection split can be revisited",
  );

  const isolatedWedgedAt = await run(probeCycles(true));
  assert.equal(
    isolatedWedgedAt,
    null,
    `a connection per probe stalled at cycle ${isolatedWedgedAt}, which is what the daemon relies on not happening`,
  );
});

/**
 * The third thing this client settles quietly, and the reason
 * @egress/rmq-consumer's daemons dead-letter a failed call rather than
 * retrying it.
 *
 * `discard()` sends `modified{delivery_failed: true, undeliverable_here:
 * true}`, which RabbitMQ routes to the queue's `x-dead-letter-exchange` —
 * that half works, and it is what turns a failed third-party call from a
 * silently dropped message into one you can count and replay.
 *
 * `requeue()` sends `modified{delivery_failed: false}`, and RabbitMQ only
 * increments AMQP 1.0's `delivery-count` for a delivery marked *failed*. So
 * a released message comes back looking brand new, forever. The client
 * exposes no outcome in between — there is no "this attempt failed, let
 * someone else try" — so a redelivery budget that survives the message
 * moving to another daemon is not implementable here, and one attempt then
 * dead-letter is the honest policy rather than a lazy one.
 *
 * Same shape of finding as link credit, and pinned for the same reason: if
 * a future client release exposes `modified{delivery_failed: true}`, the
 * second half of this test fails and tells us a real budget became possible.
 */
test("a rejected delivery dead-letters, and a released one is never counted as an attempt", async (t) => {
  if (skipIfNoDocker(t)) return;

  const work = "dl.work";
  const dead = "dl.work.dead";

  const { deadLettered, counts } = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead);
      yield* rmq.declareQueue(work, {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead,
      });

      const deadLettered: string[] = [];
      yield* rmq.consume(dead, (body) => void deadLettered.push(body));

      // Released a fixed number of times, then rejected. `seen` is an
      // in-process counter precisely because the broker-side one is what is
      // under test — and it is also the only thing stopping this from
      // looping forever if delivery_count never moves, which is the finding.
      const counts: number[] = [];
      let seen = 0;
      yield* rmq.consume(work, (_body, delivery) => {
        counts.push(delivery.deliveryCount);
        seen += 1;
        return Promise.resolve(seen >= 3 ? "discard" : "requeue");
      });

      const pub = yield* rmq.publisherToQueue(work);
      yield* rmq.send(pub, "needs-retrying");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 2500)));
      return { deadLettered, counts };
    }),
  );

  assert.deepEqual(
    deadLettered,
    ["needs-retrying"],
    "a discarded message must arrive on the dead-letter queue, not vanish",
  );
  assert.ok(counts.length >= 3, `the message should have been redelivered, saw ${counts.length}`);
  assert.deepEqual(
    counts.slice(0, 3),
    [0, 0, 0],
    "released deliveries are not counted as attempts by this client — if this " +
      "now increments, the client can express modified{delivery_failed: true} " +
      "and a real cross-consumer redelivery budget has become possible",
  );
});

/**
 * One canonical dead-letter queue for every queue in a fleet only works if
 * whatever drains it can tell the messages apart — replaying a control event
 * that failed to decode onto the *work* queue would be nonsense.
 *
 * RabbitMQ 4 records the origin as AMQP 1.0 message annotations, and this
 * pins the two fields @egress/rmq-consumer's redrive filters on. If a broker
 * upgrade stops sending them, `delivery.deadLetter` goes null, the redrive
 * silently stops recognising anything as work, and only this test says so.
 */
test("a dead-lettered message says which queue it came from", async (t) => {
  if (skipIfNoDocker(t)) return;

  const dead = "origin.dead";
  const work = "origin.work";
  const control = "origin.control";

  const seen = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead);
      const args = { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead };
      yield* rmq.declareQueue(work, args);
      yield* rmq.declareQueue(control, args);

      const seen: Array<{ body: string; queue: string | null; reason: string | null }> = [];
      yield* rmq.consume(dead, (body, delivery) => {
        seen.push({
          body,
          queue: delivery.deadLetter?.queue ?? null,
          reason: delivery.deadLetter?.reason ?? null,
        });
      });

      // Both queues reject their message, so both land on the one dead-letter
      // queue — which is the arrangement the origin has to disambiguate.
      yield* rmq.consume(work, () => "discard" as const);
      yield* rmq.consume(control, () => "discard" as const);

      const workPub = yield* rmq.publisherToQueue(work);
      const controlPub = yield* rmq.publisherToQueue(control);
      yield* rmq.send(workPub, "a-work-message");
      yield* rmq.send(controlPub, "an-undecodable-control-message");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 2000)));
      return seen;
    }),
  );

  assert.equal(seen.length, 2, `both messages should reach the dead-letter queue, got ${seen.length}`);
  const byBody = new Map(seen.map((s) => [s.body, s]));
  assert.equal(byBody.get("a-work-message")?.queue, work);
  assert.equal(byBody.get("an-undecodable-control-message")?.queue, control);
  assert.equal(
    byBody.get("a-work-message")?.reason,
    "rejected",
    "the reason distinguishes a rejection from an expiry or an overflow",
  );
});

/**
 * The other half of that: a republish drops the broker's death annotations,
 * so anything that moves messages around inside a dead-letter queue has to
 * carry the provenance itself. @egress/rmq-consumer's redrive stamps the
 * origin as an application property when it parks a non-work message, and
 * uses the presence of that stamp to know it has come full circle.
 */
test("application properties survive a republish, so provenance can outlive the annotations", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "props.queue";
  const seen = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      const got: Array<Record<string, string>> = [];
      yield* rmq.consume(queue, (_body, delivery) => void got.push({ ...delivery.properties }));
      const pub = yield* rmq.publisherToQueue(queue);
      yield* rmq.send(pub, "stamped", {
        "x-egress-origin-queue": "some.control.queue",
        "x-egress-origin-reason": "rejected",
      });
      yield* rmq.send(pub, "unstamped");
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1200)));
      return got;
    }),
  );

  assert.equal(seen.length, 2);
  const stamped = seen.find((p) => p["x-egress-origin-queue"] !== undefined);
  assert.ok(stamped, "the stamped message must arrive with its application properties");
  assert.equal(stamped!["x-egress-origin-queue"], "some.control.queue");
  assert.equal(stamped!["x-egress-origin-reason"], "rejected");
  assert.ok(
    seen.some((p) => Object.keys(p).length === 0),
    "a message published without properties must arrive with none, not with the previous message's",
  );
});
