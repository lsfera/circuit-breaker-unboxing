import { Rmq } from "@egress/rmq/Client.ts";
import type { GotMessage } from "@egress/rmq/Client.ts";
import { TRACEPARENT } from "@egress/rmq/Trace.ts";
import { deadLetterQueueFor, deadLetterQueueOptions, workQueueFor, workQueueOptions } from "@egress/rmq/WorkQueue.ts";
import { Effect, Option as O } from "effect";
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import { broker, skipIfNoDocker, startBroker, stopBroker, text } from "../../../rmq/test/integration/harness.ts";
import * as Redrive from "../../src/Redrive.ts";
import { REDRIVE_COUNT_HEADER } from "../../src/Redrive.ts";

beforeAll(startBroker);
afterAll(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

const declareQueues = Effect.fnUntraced(function*(apiId: string) {
  const rmq = yield* Rmq;
  yield* rmq.declareQueue(deadLetterQueueFor(apiId), deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueueFor(apiId), workQueueOptions(apiId));
  yield* rmq.declareQueue(Redrive.parkedQueueFor(apiId), Redrive.parkedQueueOptions());
  return rmq;
});

const drain = Effect.fnUntraced(function*(queue: string) {
  const rmq = yield* Rmq;
  const messages: Array<GotMessage> = [];
  while (true) {
    const got = O.getOrUndefined(yield* rmq.get(queue));
    if (got === undefined) return messages;
    messages.push(got);
    yield* got.ack;
  }
});

test("a pass moves messages from the dead queue onto work and increments their redrive count", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "redrive-basic";
  const outcomes: Array<Redrive.RedriveOutcome> = [];
  const queues = await run(
    Effect.gen(function*() {
      const rmq = yield* declareQueues(apiId);
      const deadPub = yield* rmq.publisherToQueue(deadLetterQueueFor(apiId));
      yield* rmq.send(deadPub, "a", { messageId: "run:a" });
      yield* rmq.send(deadPub, "b", { messageId: "run:b", headers: { [REDRIVE_COUNT_HEADER]: "2" } });
      yield* Redrive.runPass({
        apiId,
        isClosed: Effect.succeed(true),
        onOutcome: (outcome) => Effect.sync(() => void outcomes.push(outcome))
      });
      return {
        dead: yield* drain(deadLetterQueueFor(apiId)),
        work: yield* drain(workQueueFor(apiId)),
        parked: yield* drain(Redrive.parkedQueueFor(apiId))
      };
    })
  );

  assert.deepEqual(outcomes, ["moved", "moved"]);
  assert.deepEqual(
    queues.work.map((message) => ({
      body: text(message.body),
      count: message.properties[REDRIVE_COUNT_HEADER],
      messageId: O.getOrUndefined(message.messageId)
    })),
    [
      { body: "a", count: "1", messageId: "run:a" },
      { body: "b", count: "3", messageId: "run:b" }
    ]
  );
  assert.equal(queues.dead.length, 0, "successfully redriven messages are acked from the dead queue");
  assert.equal(queues.parked.length, 0);
});

test("a redriven message preserves its idempotency key, bytes, format and trace", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "redrive-carry";
  const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  const body = Uint8Array.of(0x1f, 0x8b, 0xff);
  const messages = await run(
    Effect.gen(function*() {
      const rmq = yield* declareQueues(apiId);
      const deadPub = yield* rmq.publisherToQueue(deadLetterQueueFor(apiId), {
        contentType: "application/json",
        contentEncoding: "gzip",
        type: "egress.work"
      });
      yield* rmq.send(deadPub, body, {
        messageId: "run:8",
        headers: {
          [REDRIVE_COUNT_HEADER]: "1",
          [TRACEPARENT]: traceparent,
          "x-first-death-queue": workQueueFor(apiId)
        }
      });
      yield* Redrive.runPass({
        apiId,
        isClosed: Effect.succeed(true),
        onOutcome: () => Effect.void
      });
      return yield* drain(workQueueFor(apiId));
    })
  );

  assert.equal(messages.length, 1);
  assert.deepEqual(Uint8Array.from(messages[0]!.body), body);
  assert.equal(O.getOrUndefined(messages[0]!.contentType), "application/json");
  assert.equal(O.getOrUndefined(messages[0]!.contentEncoding), "gzip");
  assert.equal(O.getOrUndefined(messages[0]!.type), "egress.work");
  assert.equal(O.getOrUndefined(messages[0]!.messageId), "run:8");
  assert.deepEqual(messages[0]!.properties, {
    [TRACEPARENT]: traceparent,
    [REDRIVE_COUNT_HEADER]: "2"
  });
});

test("a message over the redrive limit is parked with a reason", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "redrive-park";
  const outcomes: Array<Redrive.RedriveOutcome> = [];
  const queues = await run(
    Effect.gen(function*() {
      const rmq = yield* declareQueues(apiId);
      const deadPub = yield* rmq.publisherToQueue(deadLetterQueueFor(apiId));
      yield* rmq.send(deadPub, "poison", { headers: { [REDRIVE_COUNT_HEADER]: "5" } });
      yield* Redrive.runPass({
        apiId,
        isClosed: Effect.succeed(true),
        onOutcome: (outcome) => Effect.sync(() => void outcomes.push(outcome))
      });
      return {
        dead: yield* drain(deadLetterQueueFor(apiId)),
        work: yield* drain(workQueueFor(apiId)),
        parked: yield* drain(Redrive.parkedQueueFor(apiId))
      };
    })
  );

  assert.deepEqual(outcomes, ["parked"]);
  assert.equal(queues.dead.length, 0, "parked messages are acked from the dead queue");
  assert.equal(queues.work.length, 0);
  assert.deepEqual(
    queues.parked.map((message) => ({
      body: text(message.body),
      reason: message.properties[Redrive.PARKED_REASON_HEADER]
    })),
    [{ body: "poison", reason: "redriven-too-often" }]
  );
});

test("a pass stops when the breaker opens and leaves the remaining dead letters untouched", async (t) => {
  if (skipIfNoDocker(t)) return;

  const apiId = "redrive-breaker";
  let closedChecks = 1;
  const queues = await run(
    Effect.gen(function*() {
      const rmq = yield* declareQueues(apiId);
      const deadPub = yield* rmq.publisherToQueue(deadLetterQueueFor(apiId));
      yield* rmq.send(deadPub, "a");
      yield* rmq.send(deadPub, "b");
      yield* rmq.send(deadPub, "c");
      yield* Redrive.runPass({
        apiId,
        isClosed: Effect.sync(() => closedChecks-- > 0),
        onOutcome: () => Effect.void
      });
      return {
        dead: yield* drain(deadLetterQueueFor(apiId)),
        work: yield* drain(workQueueFor(apiId))
      };
    })
  );

  assert.deepEqual(queues.work.map((message) => text(message.body)), ["a"]);
  assert.deepEqual(queues.dead.map((message) => text(message.body)), ["b", "c"]);
});
