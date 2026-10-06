import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import { Rmq, RmqError } from "@egress/rmq/Client.ts";
import type { GotMessage, RmqService } from "@egress/rmq/Client.ts";
import { REDRIVE_COUNT_HEADER } from "@egress/rmq/ControlPlane.ts";
import * as Redrive from "../src/Redrive.ts";

/**
 * `nextRedrive` is the one decision in Redrive.ts with no broker in it —
 * pure, so tested directly. Cases mirror master's own coverage for the same
 * function (`packages/rmq-consumer/src/Redrive.ts` at the vendored tag).
 */

test("no header is a first redrive", () => {
  assert.deepEqual(Redrive.nextRedrive(undefined), { destination: "work", count: 1 });
});

test("counts climb by one per redrive while under the cap", () => {
  assert.deepEqual(Redrive.nextRedrive("1"), { destination: "work", count: 2 });
  assert.deepEqual(Redrive.nextRedrive("4"), { destination: "work", count: 5 });
});

test("the redrive that would exceed MAX_REDRIVES parks instead", () => {
  assert.deepEqual(Redrive.nextRedrive("5"), { destination: "parked" });
});

test("further redrives past the cap stay parked", () => {
  assert.deepEqual(Redrive.nextRedrive("6"), { destination: "parked" });
  assert.deepEqual(Redrive.nextRedrive("100"), { destination: "parked" });
});

test("a header that doesn't parse is treated as zero, not trusted", () => {
  // Untrusted rather than merely defensive: there is no way for this header
  // to hold anything but a digit string this module itself published, so a
  // garbage value here means something is forging headers — treating it as
  // zero rather than, say, parked-forever is what keeps a forged header from
  // being a way to skip the entire redrive budget.
  assert.deepEqual(Redrive.nextRedrive("not-a-number"), { destination: "work", count: 1 });
});

/**
 * A fake dead-letter queue and its two destinations, all in memory — no
 * broker, matching Breaker.test.ts's own style for `withPermit`. `get`
 * simply shifts the next fake delivery; `send` records which destination a
 * body landed on, keyed by the queue name `publisherToQueue` was given.
 * `events` logs every send and settlement in the order they happened, so a
 * test can see that a message is acked only after its publish landed.
 * `failSendOf` makes `send` fail for that body, as a broker refusal would.
 */
type Dead = Omit<GotMessage, "ack" | "nack">;

const fakeRedriveRmq = (dead: ReadonlyArray<Dead>, failSendOf?: string) => {
  const events: Array<string> = [];
  const queue: Array<GotMessage> = dead.map((m) => ({
    ...m,
    ack: Effect.sync(() => void events.push(`ack ${m.body}`)),
    nack: Effect.sync(() => void events.push(`nack ${m.body}`)),
  }));
  const sent: Record<string, Array<{ body: string; headers: Record<string, string>; messageId: string | undefined }>> = {};
  const unimplemented =
    (op: string) =>
    (..._args: ReadonlyArray<unknown>) =>
      Effect.die(new Error(`${op}: not used by runPass`));
  const rmq: RmqService = Rmq.of({
    declareQueue: unimplemented("declareQueue"),
    declareTopicExchange: unimplemented("declareTopicExchange"),
    bind: unimplemented("bind"),
    consume: unimplemented("consume"),
    get: () => Effect.sync(() => (queue.length === 0 ? O.none() : O.some(queue.shift()!))),
    publisherToExchange: unimplemented("publisherToExchange"),
    publisherToQueue: (queueName) =>
      Effect.succeed({
        exchange: "",
        routingKey: queueName,
        contentType: O.none(),
        contentEncoding: O.none(),
        type: O.none(),
        mandatory: true,
      }),
    send: (pub, body, options) =>
      body === failSendOf
        ? Effect.fail(new RmqError({ operation: "send", cause: "message nacked" }))
        : Effect.sync(() => {
            events.push(`send ${pub.routingKey} ${body}`);
            (sent[pub.routingKey] ??= []).push({ body, headers: options?.headers ?? {}, messageId: options?.messageId });
          }),
    sendBatch: unimplemented("sendBatch"),
    cancelConsumer: unimplemented("cancelConsumer"),
    closeConsumer: unimplemented("closeConsumer"),
    lost: Effect.never,
    isConnected: Effect.succeed(true),
    resetConnection: Effect.sync(() => {}),
  });
  return { rmq, sent, events };
};

const message = (body: string, redriveCount?: number, messageId?: string): Dead => ({
  body,
  properties: redriveCount === undefined ? {} : { [REDRIVE_COUNT_HEADER]: String(redriveCount) },
  messageId: O.fromNullishOr(messageId),
});

const run = <A>(effect: Effect.Effect<A, unknown, Rmq>, rmq: RmqService) =>
  Effect.runPromise(Effect.provideService(effect, Rmq, rmq));

test("a pass drains the dead queue onto work, incrementing the redrive count", async () => {
  const { rmq, sent, events } = fakeRedriveRmq([message("a"), message("b", 2)]);
  const outcomes: Array<Redrive.RedriveOutcome> = [];
  await run(
    Redrive.runPass({
      apiId: "payments-provider",
      isClosed: Effect.succeed(true),
      onOutcome: (o) => outcomes.push(o),
    }),
    rmq,
  );

  assert.deepEqual(outcomes, ["moved", "moved"]);
  assert.deepEqual(sent["payments-provider.work"], [
    { body: "a", headers: { [REDRIVE_COUNT_HEADER]: "1" }, messageId: undefined },
    { body: "b", headers: { [REDRIVE_COUNT_HEADER]: "3" }, messageId: undefined },
  ]);
  assert.equal(sent["payments-provider.work.parked"], undefined);
  assert.deepEqual(events, [
    "send payments-provider.work a",
    "ack a",
    "send payments-provider.work b",
    "ack b",
  ]);
});

test("a redriven message carries its original message_id forward, so the third party sees the same idempotency key", async () => {
  const { rmq, sent } = fakeRedriveRmq([message("a", undefined, "run:7")]);
  await run(
    Redrive.runPass({ apiId: "payments-provider", isClosed: Effect.succeed(true), onOutcome: () => {} }),
    rmq,
  );

  assert.equal(sent["payments-provider.work"]![0]!.messageId, "run:7");
});

test("a message already at MAX_REDRIVES is parked instead of moved", async () => {
  const { rmq, sent, events } = fakeRedriveRmq([message("poison", 5)]);
  const outcomes: Array<Redrive.RedriveOutcome> = [];
  await run(
    Redrive.runPass({
      apiId: "payments-provider",
      isClosed: Effect.succeed(true),
      onOutcome: (o) => outcomes.push(o),
    }),
    rmq,
  );

  assert.deepEqual(outcomes, ["parked"]);
  assert.equal(sent["payments-provider.work"], undefined);
  assert.deepEqual(sent["payments-provider.work.parked"], [{ body: "poison", headers: { "x-egress-parked-reason": "redriven-too-often" }, messageId: undefined }]);
  assert.deepEqual(events, ["send payments-provider.work.parked poison", "ack poison"]);
});

test("a pass stops the instant the gate closes, leaving the rest of the queue untouched", async () => {
  const { rmq, sent, events } = fakeRedriveRmq([message("a"), message("b"), message("c")]);
  let closedAfter = 1;
  const outcomes: Array<Redrive.RedriveOutcome> = [];
  await run(
    Redrive.runPass({
      apiId: "payments-provider",
      // Open (not closed) from the very first check this pass makes past
      // the first message it moves — a real breaker reopening mid-pass.
      isClosed: Effect.sync(() => closedAfter-- > 0),
      onOutcome: (o) => outcomes.push(o),
    }),
    rmq,
  );

  assert.deepEqual(outcomes, ["moved"], "only the message fetched before the gate closed should move");
  assert.deepEqual(sent["payments-provider.work"], [
    { body: "a", headers: { [REDRIVE_COUNT_HEADER]: "1" }, messageId: undefined },
  ]);
  assert.deepEqual(events, ["send payments-provider.work a", "ack a"], "b and c must never be fetched, let alone settled");
});

test("a publish that fails hands its message back instead of acking it, so it is never lost nor held", async () => {
  const { rmq, events } = fakeRedriveRmq([message("a"), message("b")], "b");
  const exit = await Effect.runPromiseExit(
    Effect.provideService(
      Redrive.runPass({ apiId: "payments-provider", isClosed: Effect.succeed(true), onOutcome: () => {} }),
      Rmq,
      rmq,
    ),
  );

  assert.equal(exit._tag, "Failure");
  assert.equal(events.includes("ack b"), false, "acking b without its publish landing would drop it");
  assert.ok(events.includes("nack b"), "left unsettled, b would sit unacked on get's channel, invisible to later passes");
});
