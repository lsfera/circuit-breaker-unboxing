import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { Rmq, RmqLive } from "../../src/Client.ts";

/**
 * Dead-lettering: what a rejection does, what it carries, and what survives a
 * republish.
 *
 * A separate file from Client.test.ts, and separate on purpose — node runs
 * each test file in its own process, so this gets its own broker. Sharing one
 * with the stranding test is not viable: that test deliberately induces the
 * client bug where closing consumers with deliveries in flight stalls a
 * connection, and afterwards the *broker* stops reliably dead-lettering.
 * Observed repeatedly, on a fresh connection, with a fresh queue: one queue's
 * rejections routed to the dead-letter queue and another identically declared
 * queue's did not, with no error from `discard()` and nothing in any log.
 * That is a real hazard and it is written up in docs/rmq-control-plane.md; it
 * is not pinned by a test here because it reproduces about half the time,
 * and a test that fails half the time teaches nobody anything.
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

/**
 * Poll until `done()` or the deadline, instead of sleeping a fixed amount and
 * hoping.
 *
 * The fixed sleeps elsewhere in this file are fine because those tests are
 * the only thing touching the broker at that moment. The ones that use this
 * are not: they run immediately after the stranding test above, which leaves
 * the broker cleaning up thousands of stranded deliveries across a dozen
 * closed links, and a two-second budget that is generous in isolation stops
 * being generous behind that. Waiting for the condition is what makes them
 * deterministic rather than usually-true.
 */
const waitFor = (done: () => boolean, timeoutMs = 15_000) =>
  Effect.promise(async () => {
    const deadline = Date.now() + timeoutMs;
    while (!done() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    // A short settle after the condition, so a test asserting "exactly N"
    // would still see an N+1 that was already on its way.
    await new Promise((r) => setTimeout(r, 250));
  });

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, RmqLive({ host, port }))) as Effect.Effect<A>,
  );

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
        args: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead },
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
      yield* waitFor(() => deadLettered.length > 0 && counts.length >= 3);
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
      const options = {
        args: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead },
      };
      yield* rmq.declareQueue(work, options);
      yield* rmq.declareQueue(control, options);

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
      yield* waitFor(() => seen.length >= 2);
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
      yield* waitFor(() => got.length >= 2);
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

/**
 * The property the dead-letter queue exists for, and the one it did not have.
 *
 * Rejecting a failed message preserves it only if the queue holding it
 * outlives the broker. Measured on the running stack before this changed: a
 * dead-letter queue holding 24 messages held 0 after `docker compose restart
 * rabbitmq` — the queue was recreated by the next daemon to connect, empty,
 * so nothing even looked wrong.
 *
 * Both halves are asserted together because the contrast is the point. A
 * transient queue is the right choice for a live subscription a restarting
 * consumer rebuilds from the next snapshot; it is the wrong choice for work
 * you promised to keep, and the two differ by one flag.
 */
test("a durable queue keeps its messages across a broker restart; a transient one does not", async (t) => {
  if (skipIfNoDocker(t)) return;

  const durable = "survive.durable";
  const transient = "survive.transient";

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(durable, { durable: true });
      yield* rmq.declareQueue(transient, { durable: false });
      const a = yield* rmq.publisherToQueue(durable);
      const b = yield* rmq.publisherToQueue(transient);
      for (let i = 0; i < 5; i++) {
        yield* rmq.send(a, `keep-${i}`);
        yield* rmq.send(b, `lose-${i}`);
      }
      // Settle before the restart, so this measures durability rather than a
      // race between publishing and the broker going down.
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1000)));
    }),
  );

  await container!.restart();
  await new Promise((r) => setTimeout(r, 3000));
  host = container!.getHost();
  port = container!.getMappedPort(5672);

  const { kept, lost } = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      // Redeclared with the same arguments, exactly as a reconnecting daemon
      // does — which is why an empty durable queue would look identical to a
      // healthy one from the outside.
      yield* rmq.declareQueue(durable, { durable: true });
      yield* rmq.declareQueue(transient, { durable: false });

      const kept: string[] = [];
      const lost: string[] = [];
      yield* rmq.consume(durable, (body) => void kept.push(body));
      yield* rmq.consume(transient, (body) => void lost.push(body));
      yield* waitFor(() => kept.length >= 5);
      return { kept, lost };
    }),
  );

  assert.equal(kept.length, 5, `durable queue must keep its messages, kept ${kept.length}`);
  assert.deepEqual([...kept].sort(), ["keep-0", "keep-1", "keep-2", "keep-3", "keep-4"]);
  assert.equal(lost.length, 0, "a transient queue is empty again, which is the whole contrast");
});
