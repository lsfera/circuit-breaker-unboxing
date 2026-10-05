import { Effect, Option as O } from "effect";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { Rmq } from "../../src/Client.ts";
import { TRACEPARENT } from "../../src/Trace.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  WORK_DELIVERY_LIMIT,
  workQueueFor,
  workQueueOptions
} from "../../src/WorkQueue.ts";
import { broker, restartBroker, skipIfNoDocker, startBroker, stopBroker, waitFor } from "./harness.ts";

/**
 * Dead-lettering: what a rejection does, what it carries, and what survives a republish. A separate file from
 * Client.test.ts on purpose: node runs each test file in its own process, so this gets its own broker, and
 * sharing one with the stranding test is not viable (its stranded consumers leave the broker unreliable at
 * dead-lettering).
 */

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

/**
 * Why the work queue is a quorum queue, stated as a property. Rejecting without requeue routes the message to
 * the queue's `x-dead-letter-exchange` on any queue type. Counting attempts does not: a classic queue has no
 * `x-delivery-count`, so an unbounded requeue against a dead upstream would be a hot loop with no counter to
 * stop it. The budget lives on the queue as `x-delivery-limit` on a quorum queue (see the delivery-limit test below).
 */
test("a rejected delivery dead-letters, and a classic queue counts no attempts", async (t) => {
  if (skipIfNoDocker(t)) return;

  const work = "dl.work";
  const dead = "dl.work.dead";

  const { deadLettered, counts } = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead);
      yield* rmq.declareQueue(work, {
        args: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead }
      });

      const deadLettered: Array<string> = [];
      yield* rmq.consume(dead, (body) => void deadLettered.push(body.toString()));

      // Released a fixed number of times, then rejected. `seen` is an in-process counter because the broker-side one
      // is what is under test, and it stops this looping forever if delivery_count never moves.
      const counts: Array<number> = [];
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
    })
  );

  assert.deepEqual(
    deadLettered,
    ["needs-retrying"],
    "a discarded message must arrive on the dead-letter queue, not vanish"
  );
  assert.ok(counts.length >= 3, `the message should have been redelivered, saw ${counts.length}`);
  assert.deepEqual(
    counts.slice(0, 3),
    [0, 0, 0],
    "a classic queue exposes no x-delivery-count, so every redelivery looks " +
      "like a first one — which is exactly why WORK_DELIVERY_LIMIT needs a " +
      "quorum queue to mean anything"
  );
});

/**
 * One canonical dead-letter queue for every queue in a fleet only works if whatever drains it can tell the
 * messages apart. RabbitMQ 4 records the origin as message annotations; this pins the two fields a drain
 * filters on (`delivery.deadLetter`), so a broker upgrade that stops sending them fails here.
 */
test("a dead-lettered message says which queue it came from", async (t) => {
  if (skipIfNoDocker(t)) return;

  const dead = "origin.dead";
  const work = "origin.work";
  const control = "origin.control";

  const seen = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead);
      const options = {
        args: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead }
      };
      yield* rmq.declareQueue(work, options);
      yield* rmq.declareQueue(control, options);

      const seen: Array<{ body: string; queue: string | null; reason: string | null; }> = [];
      yield* rmq.consume(dead, (bytes, delivery) => {
        const body = bytes.toString();
        seen.push({
          body,
          queue: O.getOrUndefined(delivery.deadLetter)?.queue ?? null,
          reason: O.getOrUndefined(delivery.deadLetter)?.reason ?? null
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
    })
  );

  assert.equal(seen.length, 2, `both messages should reach the dead-letter queue, got ${seen.length}`);
  const byBody = new Map(seen.map((s) => [s.body, s]));
  assert.equal(byBody.get("a-work-message")?.queue, work);
  assert.equal(byBody.get("an-undecodable-control-message")?.queue, control);
  assert.equal(
    byBody.get("a-work-message")?.reason,
    "rejected",
    "the reason distinguishes a rejection from an expiry or an overflow"
  );
});

/**
 * The other half: a republish drops the broker's death annotations, so anything that moves messages around
 * inside a dead-letter queue has to carry the provenance itself, as an application property.
 */
test("application properties survive a republish, so provenance can outlive the annotations", async (t) => {
  if (skipIfNoDocker(t)) return;

  const queue = "props.queue";
  const seen = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(queue);
      const got: Array<Record<string, string>> = [];
      yield* rmq.consume(queue, (_body, delivery) => void got.push({ ...delivery.properties }));
      const pub = yield* rmq.publisherToQueue(queue);
      yield* rmq.send(pub, "stamped", {
        headers: {
          "x-egress-origin-queue": "some.control.queue",
          "x-egress-origin-reason": "rejected"
        }
      });
      yield* rmq.send(pub, "unstamped");
      yield* waitFor(() => got.length >= 2);
      return got;
    })
  );

  assert.equal(seen.length, 2);
  const stamped = seen.find((p) => p["x-egress-origin-queue"] !== undefined);
  assert.ok(stamped, "the stamped message must arrive with its application properties");
  assert.equal(stamped!["x-egress-origin-queue"], "some.control.queue");
  assert.equal(stamped!["x-egress-origin-reason"], "rejected");
  assert.ok(
    seen.some((p) => Object.keys(p).length === 0),
    "a message published without properties must arrive with none, not with the previous message's"
  );
});

/**
 * The property the dead-letter queue exists for: rejecting a failed message preserves it only if the queue holding
 * it outlives the broker. Every queue here is durable (RabbitMQ 4.3 refuses a non-exclusive transient one), so the
 * test restarts the broker and expects the messages back.
 */
test("a durable queue keeps its messages across a broker restart", async (t) => {
  if (skipIfNoDocker(t)) return;

  const durable = "survive.durable";

  await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(durable, { durable: true });
      const a = yield* rmq.publisherToQueue(durable);
      for (let i = 0; i < 5; i++) {
        yield* rmq.send(a, `keep-${i}`);
      }
      // Settle before the restart, so this measures durability rather than a
      // race between publishing and the broker going down.
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1000)));
    })
  );

  await restartBroker();

  const kept = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      // Redeclared with the same arguments, as a reconnecting daemon does: an empty durable queue would look
      // identical to a healthy one from the outside.
      yield* rmq.declareQueue(durable, { durable: true });

      const kept: Array<string> = [];
      yield* rmq.consume(durable, (body) => void kept.push(body.toString()));
      yield* waitFor(() => kept.length >= 5);
      return kept;
    })
  );

  assert.equal(kept.length, 5, `durable queue must keep its messages, kept ${kept.length}`);
  assert.deepEqual([...kept].sort(), ["keep-0", "keep-1", "keep-2", "keep-3", "keep-4"]);
});

/**
 * The redelivery budget belongs to the queue: a quorum queue carrying `x-delivery-limit` has the broker count the
 * attempts and dead-letter the message itself once they are spent. Two things are asserted together: the budget is spent
 * by *requeue*, and a republish resets it (a replayed message is a new message with a full budget, so four
 * attempts per outage, not four ever), which is the part someone reading `WORK_DELIVERY_LIMIT` would get wrong.
 */
/**
 * A replay of failed work should keep its trace: the trace context survives dead-lettering because RabbitMQ keeps
 * application headers, so a republish of the body alone throws away a trace that was right there.
 */
test("a traceparent survives dead-lettering, and only a republish that carries it keeps it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `trace-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);
  const carried = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  const { onDead, replayed } = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const onDead: Array<boolean> = [];
      const replayed: Array<{ how: string; parent: boolean; }> = [];

      // Two independent messages, each dead-lettered once and replayed once: one alone, one carrying the header.
      const deadSeen = new Map<string, number>();
      yield* rmq.consume(dead, (bytes, delivery) => {
        const body = bytes.toString();
        const n = (deadSeen.get(body) ?? 0) + 1;
        deadSeen.set(body, n);
        if (n > 1) return "accept" as const;
        onDead.push(O.isSome(delivery.parent));
        const options = body === "alone" ? {} : { headers: { [TRACEPARENT]: delivery.properties[TRACEPARENT]! } };
        return Effect.runPromise(rmq.send(into, body, options)).then(() => "accept" as const);
      });

      const workSeen = new Map<string, number>();
      yield* rmq.consume(work, (bytes, delivery) => {
        const body = bytes.toString();
        const n = (workSeen.get(body) ?? 0) + 1;
        workSeen.set(body, n);
        if (n === 2) replayed.push({ how: body, parent: O.isSome(delivery.parent) });
        return "discard" as const;
      });

      yield* rmq.send(into, "alone", { headers: { [TRACEPARENT]: carried } });
      yield* rmq.send(into, "carrying", { headers: { [TRACEPARENT]: carried } });
      yield* waitFor(() => replayed.length >= 2);
      return { onDead, replayed };
    })
  );

  assert.deepEqual(
    onDead,
    [true, true],
    "the traceparent must still be readable once the message is dead-lettered"
  );
  assert.deepEqual(
    [...replayed].sort((a, b) => a.how.localeCompare(b.how)),
    [
      { how: "alone", parent: false },
      { how: "carrying", parent: true }
    ]
  );
});

/**
 * The idempotency key a payments call needs under at-least-once delivery is the message's AMQP `message_id`: kept
 * when the message is dead-lettered, and, like the traceparent, kept on a replay only if the republish carries it
 * explicitly. A republish that does not is a new message with a new id, which for a payment is a second charge.
 */
test("a message id survives dead-lettering, and only a redrive republish that carries it keeps it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `idem-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);
  const id = "run1:41";

  const { onDead, replayed } = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const onDead: Array<O.Option<string>> = [];
      const replayed: Array<{ how: string; id: O.Option<string>; }> = [];

      // Two messages, each dead-lettered once and replayed once: one carrying the id, one dropping it (a careless republish).
      const deadSeen = new Map<string, number>();
      yield* rmq.consume(dead, (bytes, delivery) => {
        const body = bytes.toString();
        const n = (deadSeen.get(body) ?? 0) + 1;
        deadSeen.set(body, n);
        if (n > 1) return "accept" as const;
        onDead.push(delivery.messageId);
        const options = body === "carrying" ? { messageId: O.getOrThrow(delivery.messageId) } : {};
        return Effect.runPromise(rmq.send(into, body, options)).then(() => "accept" as const);
      });

      const workSeen = new Map<string, number>();
      yield* rmq.consume(work, (bytes, delivery) => {
        const body = bytes.toString();
        const n = (workSeen.get(body) ?? 0) + 1;
        workSeen.set(body, n);
        if (n === 2) replayed.push({ how: body, id: delivery.messageId });
        return "discard" as const;
      });

      yield* rmq.send(into, "carrying", { messageId: id });
      yield* rmq.send(into, "dropping", { messageId: id });
      yield* waitFor(() => replayed.length >= 2);
      return { onDead, replayed };
    })
  );

  assert.deepEqual(onDead, [O.some(id), O.some(id)], "the id must still be readable once the message is dead-lettered");
  const byHow = Object.fromEntries(replayed.map((r) => [r.how, r.id]));
  assert.deepEqual(byHow["carrying"], O.some(id), "a republish that carries the id keeps the key");
  assert.equal(
    O.isSome(byHow["dropping"]!) && byHow["dropping"]!.value !== id,
    true,
    "a republish that drops it is a different message"
  );
});

test("the work queue parks a message at the delivery limit, and a redrive republish grants a fresh budget", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `budget-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);

  const { attempts, parked } = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const parked: Array<string> = [];
      let replayed = false;
      yield* rmq.consume(dead, (bytes, delivery) => {
        const body = bytes.toString();
        parked.push(O.getOrUndefined(delivery.deadLetter)?.reason ?? "unknown");
        if (replayed) return "accept" as const;
        replayed = true;
        // A republish of the body onto the work queue is a new message to the broker.
        return Effect.runPromise(rmq.send(into, body)).then(() => "accept" as const);
      });

      const attempts: Array<number> = [];
      yield* rmq.send(into, "unit-of-work");
      yield* rmq.consume(work, (_body, delivery) => {
        attempts.push(delivery.deliveryCount);
        return "requeue" as const;
      });

      yield* waitFor(() => parked.length >= 2);
      return { attempts, parked };
    })
  );

  assert.deepEqual(
    parked,
    ["delivery_limit", "delivery_limit"],
    "both parkings must be the broker enforcing the limit, not something else rejecting"
  );
  assert.equal(
    attempts.length,
    (WORK_DELIVERY_LIMIT + 1) * 2,
    `one delivery plus ${WORK_DELIVERY_LIMIT} redeliveries, twice — the redrive resets the budget`
  );
  // The count is the broker's, which is what makes it survive a message moving between daemons; the header is
  // readable, so a reset is visible rather than inferred from the parkings.
  assert.deepEqual(
    attempts,
    [0, 1, 2, 3, 0, 1, 2, 3],
    "x-delivery-count climbs to the limit, then starts again from 0 for the republished message"
  );
});

/**
 * A dead-letter queue must never lose a message to its own delivery limit: a quorum queue with no dead-letter
 * target drops a message once its limit (20 by default) is reached, and a consumer channel that takes the message
 * and closes without settling it counts as a return. Twenty-five such closes is more than the default allowed.
 */
test("the dead-letter queue keeps a message through more returns than a default delivery limit", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "dl-unlimited";
  const dead = deadLetterQueueFor(apiId);

  const left = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.send(yield* rmq.publisherToQueue(dead), "kept");

      for (let round = 0; round < 25; round++) {
        let seen = false;
        const consumer = yield* rmq.consume(dead, () => {
          seen = true;
          return new Promise<never>(() => {});
        }, { prefetch: 1 });
        yield* waitFor(() => seen);
        yield* rmq.closeConsumer(consumer);
      }

      const received: Array<string> = [];
      yield* rmq.consume(dead, (body) => void received.push(body.toString()));
      yield* waitFor(() => received.length > 0);
      return received;
    })
  );

  assert.deepEqual(left, ["kept"]);
});
