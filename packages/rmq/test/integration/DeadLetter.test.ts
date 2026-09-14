import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import {
  broker,
  restartBroker,
  skipIfNoDocker,
  startBroker,
  stopBroker,
  waitFor,
} from "./harness.ts";
import { Rmq } from "../../src/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  IDEMPOTENCY_KEY_HEADER,
  WORK_DELIVERY_LIMIT,
  workQueueFor,
  workQueueOptions,
} from "../../src/ControlPlane.ts";
import { TRACEPARENT } from "../../src/Trace.ts";

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

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>,
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
 * There used to be a transient queue beside it, emptied by the same restart, as
 * the contrast. RabbitMQ 4.3 refuses to declare one that is not exclusive — it
 * closes the connection — so every queue here is durable now.
 */
test("a durable queue keeps its messages across a broker restart", async (t) => {
  if (skipIfNoDocker(t)) return;

  const durable = "survive.durable";

  await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(durable, { durable: true });
      const a = yield* rmq.publisherToQueue(durable);
      for (let i = 0; i < 5; i++) {
        yield* rmq.send(a, `keep-${i}`);
      }
      // Settle before the restart, so this measures durability rather than a
      // race between publishing and the broker going down.
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 1000)));
    }),
  );

  await restartBroker();

  const kept = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      // Redeclared with the same arguments, exactly as a reconnecting daemon
      // does — which is why an empty durable queue would look identical to a
      // healthy one from the outside.
      yield* rmq.declareQueue(durable, { durable: true });

      const kept: string[] = [];
      yield* rmq.consume(durable, (body) => void kept.push(body));
      yield* waitFor(() => kept.length >= 5);
      return kept;
    }),
  );

  assert.equal(kept.length, 5, `durable queue must keep its messages, kept ${kept.length}`);
  assert.deepEqual([...kept].sort(), ["keep-0", "keep-1", "keep-2", "keep-3", "keep-4"]);
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
/**
 * A redrive replays the work that failed — which is the work most worth
 * following, and was the only work that arrived untraceable.
 *
 * The trace context survives being dead-lettered, because RabbitMQ keeps
 * application headers. Republishing the body alone therefore does not lose a
 * trace that was unavailable; it throws away one that was right there.
 */
test("a traceparent survives dead-lettering, and only a republish that carries it keeps it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `trace-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);
  const carried = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  const { onDead, replayed } = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const onDead: boolean[] = [];
      const replayed: Array<{ how: string; parent: boolean }> = [];

      // Two independent messages, each dead-lettered once and replayed once —
      // "alone" the way the redrive used to, "carrying" the way it does now.
      const deadSeen = new Map<string, number>();
      yield* rmq.consume(dead, (body, delivery) => {
        const n = (deadSeen.get(body) ?? 0) + 1;
        deadSeen.set(body, n);
        if (n > 1) return "accept" as const;
        onDead.push(O.isSome(delivery.parent));
        const props =
          body === "alone" ? undefined : { [TRACEPARENT]: delivery.properties[TRACEPARENT]! };
        return Effect.runPromise(rmq.send(into, body, props)).then(() => "accept" as const);
      });

      const workSeen = new Map<string, number>();
      yield* rmq.consume(work, (body, delivery) => {
        const n = (workSeen.get(body) ?? 0) + 1;
        workSeen.set(body, n);
        if (n === 2) replayed.push({ how: body, parent: O.isSome(delivery.parent) });
        return "discard" as const;
      });

      yield* rmq.send(into, "alone", { [TRACEPARENT]: carried });
      yield* rmq.send(into, "carrying", { [TRACEPARENT]: carried });
      yield* waitFor(() => replayed.length >= 2);
      return { onDead, replayed };
    }),
  );

  assert.deepEqual(
    onDead,
    [true, true],
    "the traceparent must still be readable once the message is dead-lettered",
  );
  assert.deepEqual(
    [...replayed].sort((a, b) => a.how.localeCompare(b.how)),
    [
      { how: "alone", parent: false },
      { how: "carrying", parent: true },
    ],
  );
});

/**
 * The idempotency key a payments call needs to be safe under at-least-once
 * delivery, carried the same way the traceparent above is: the header
 * survives being dead-lettered, and only a republish that explicitly carries
 * it keeps it on the replay. This is the property `Redrive.ts`'s move back
 * onto the work queue depends on — a message with no key must still redrive
 * with none, not inherit one from elsewhere.
 */
test("an idempotency key survives dead-lettering, and only a redrive republish that carries it keeps it", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = `idem-${Date.now()}`;
  const work = workQueueFor(apiId);
  const dead = deadLetterQueueFor(apiId);
  const key = "11111111-1111-1111-1111-111111111111";

  const { onDead, replayed } = await run(
    Effect.gen(function* () {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(dead, deadLetterQueueOptions());
      yield* rmq.declareQueue(work, workQueueOptions(apiId));
      const into = yield* rmq.publisherToQueue(work);

      const onDead: Array<O.Option<string>> = [];
      const replayed: Array<{ how: string; key: O.Option<string> }> = [];

      // "keyed" carries x-idempotency-key the way the producer stamps it;
      // "keyless" never had one — both must redrive true to what they arrived
      // with, exactly as the traceparent test pins for the trace header.
      const deadSeen = new Map<string, number>();
      yield* rmq.consume(dead, (body, delivery) => {
        const n = (deadSeen.get(body) ?? 0) + 1;
        deadSeen.set(body, n);
        if (n > 1) return "accept" as const;
        onDead.push(delivery.idempotencyKey);
        const props = O.match(delivery.idempotencyKey, {
          onNone: () => undefined,
          onSome: (v) => ({ [IDEMPOTENCY_KEY_HEADER]: v }),
        });
        return Effect.runPromise(rmq.send(into, body, props)).then(() => "accept" as const);
      });

      const workSeen = new Map<string, number>();
      yield* rmq.consume(work, (body, delivery) => {
        const n = (workSeen.get(body) ?? 0) + 1;
        workSeen.set(body, n);
        if (n === 2) replayed.push({ how: body, key: delivery.idempotencyKey });
        return "discard" as const;
      });

      yield* rmq.send(into, "keyless");
      yield* rmq.send(into, "keyed", { [IDEMPOTENCY_KEY_HEADER]: key });
      yield* waitFor(() => replayed.length >= 2);
      return { onDead, replayed };
    }),
  );

  assert.deepEqual(
    onDead.map(O.isSome),
    [false, true],
    "the idempotency key must still be readable once the message is dead-lettered, and absence must stay absence",
  );
  assert.deepEqual(
    [...replayed].sort((a, b) => a.how.localeCompare(b.how)).map((r) => ({ how: r.how, key: O.getOrUndefined(r.key) })),
    [
      { how: "keyed", key },
      { how: "keyless", key: undefined },
    ],
    "a redrive republish must carry the original key when there was one, and add none when there wasn't",
  );
});

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

/**
 * A dead-letter queue must never lose a message to its own delivery limit.
 *
 * A quorum queue with no dead-letter target drops a message once its delivery
 * limit (20 by default) is reached, and every redrive pass hands back what it did
 * not move by closing its channel — which counts. Measured in a chaos run as 1,570
 * messages gone. Twenty-five consumer channels that take the message and close
 * without settling it is more returns than the old default allowed.
 */
test("the dead-letter queue keeps a message through more returns than a default delivery limit", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "dl-unlimited";
  const dead = deadLetterQueueFor(apiId);

  const left = await run(
    Effect.gen(function* () {
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

      const received: string[] = [];
      yield* rmq.consume(dead, (body) => void received.push(body));
      yield* waitFor(() => received.length > 0);
      return received;
    }),
  );

  assert.deepEqual(left, ["kept"]);
});
