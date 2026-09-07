import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { Rmq, RmqLive } from "../../src/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  WORK_DELIVERY_LIMIT,
  workQueueFor,
  workQueueOptions,
} from "../../src/ControlPlane.ts";

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
 * Why the work queue is a quorum queue, stated as a property rather than a
 * preference.
 *
 * Rejecting without requeue routes the message to the queue's
 * `x-dead-letter-exchange` — that is what turns a failed third-party call from
 * a silently dropped message into one you can count and replay, and it works
 * on any queue type.
 *
 * Counting the attempts does not. This queue is deliberately *classic*, and on
 * a classic queue there is no `x-delivery-count` at all: a requeued message
 * comes back indistinguishable from a new one, forever, so nothing here could
 * enforce a budget and an unbounded requeue against a dead upstream would be a
 * hot loop with no counter to stop it. The budget lives on the work queue
 * instead, as `x-delivery-limit` on a quorum queue — see the delivery-limit
 * test at the bottom of this file for the other half.
 */
test("a rejected delivery dead-letters, and a classic queue counts no attempts", async (t) => {
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
    "a classic queue exposes no x-delivery-count, so every redelivery looks " +
      "like a first one — which is exactly why WORK_DELIVERY_LIMIT needs a " +
      "quorum queue to mean anything",
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
          queue: O.getOrUndefined(delivery.deadLetter)?.queue ?? null,
          reason: O.getOrUndefined(delivery.deadLetter)?.reason ?? null,
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

/**
 * The redelivery budget this repo spent two documents saying it could not
 * have.
 *
 * The client cannot mark a delivery failed, and RabbitMQ will not count one
 * that is not — both still true, and neither matters, because the budget is a
 * property of the queue. The work queue is a quorum queue carrying
 * `x-delivery-limit`, so the broker counts the attempts and parks the message
 * itself once they are spent.
 *
 * Two things are asserted together because each is only half the behaviour.
 * The budget is spent by *requeue*, the outcome that was supposed to be
 * useless. And a redrive resets it: `Redrive.ts` replays work by publishing
 * the body again, so a replayed message is a new message with a full budget —
 * three attempts per outage, not three ever. The handler here stands in for
 * that republish, deliberately, because that interaction is the part someone
 * reading `WORK_DELIVERY_LIMIT` would get wrong.
 */
test("the work queue parks a message at the delivery limit, and a redrive republish grants a fresh budget", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `budget-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);

  const { attempts, parked } = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const parked: string[] = [];
      let replayed = false;
      yield* rmq.consume(dead, (body, delivery) => {
        parked.push(O.getOrUndefined(delivery.deadLetter)?.reason ?? "unknown");
        if (replayed) return "accept" as const;
        replayed = true;
        // What Redrive.ts does on the transition back to CLOSED: publish the
        // body onto the work queue, which is a new message to the broker.
        return Effect.runPromise(rmq.send(into, body)).then(() => "accept" as const);
      });

      const attempts: number[] = [];
      yield* rmq.send(into, "unit-of-work");
      yield* rmq.consume(work, (_body, delivery) => {
        attempts.push(delivery.deliveryCount);
        return "requeue" as const;
      });

      yield* waitFor(() => parked.length >= 2);
      return { attempts, parked };
    }),
  );

  assert.deepEqual(
    parked,
    ["delivery_limit", "delivery_limit"],
    "both parkings must be the broker enforcing the limit, not something else rejecting",
  );
  assert.equal(
    attempts.length,
    (WORK_DELIVERY_LIMIT + 1) * 2,
    `one delivery plus ${WORK_DELIVERY_LIMIT} redeliveries, twice — the redrive resets the budget`,
  );
  // The 1.0 client reported 0 on every delivery, so this used to assert
  // blindness — the counting was the broker's and the daemon could not see it.
  // The counting is still the broker's, which is what makes it survive a
  // message moving between daemons; what changed is that the header is now
  // readable, so the reset is visible rather than inferred from the parkings.
  assert.deepEqual(
    attempts,
    [0, 1, 2, 3, 0, 1, 2, 3],
    "x-delivery-count climbs to the limit, then starts again from 0 for the republished message",
  );
});
