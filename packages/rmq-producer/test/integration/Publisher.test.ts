import { Rmq } from "@egress/rmq/Client.ts";
import * as Contract from "@egress/rmq/Contract.ts";
import { read, text } from "@egress/rmq/Negotiation.ts";
import { Cause, Effect, Exit, Fiber, Metric, Option as O, Result, Schema } from "effect";
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import { broker, brokerExec, skipIfNoDocker, startBroker, stopBroker } from "../../../rmq/test/integration/harness.ts";
import * as Producer from "../../src/index.ts";
import * as Telemetry from "../../src/Telemetry.ts";

/**
 * The publisher against a real RabbitMQ. It knows only its contract's exchange, so each test plays the consumer's
 * part where it needs one: declaring a queue and binding it, as `@egress/rmq-consumer` binds `<key>.work`.
 */

beforeAll(startBroker);
afterAll(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

const workOn = (exchange: string | Contract.ExchangeInput) =>
  Contract.make(Schema.Struct({ apiId: Schema.String, n: Schema.Int }), {
    exchange,
    type: "egress.work",
    formats: { "application/json": text(Schema.fromJsonString(Schema.Unknown)) }
  });

/** What a consumer does: its own queue, bound to the contract's exchange (`#` on the topic exchange by default). */
const bindQueue = (
  exchange: string | Contract.Exchange,
  queue: string,
  binding: { readonly routingKey?: string; readonly args?: Record<string, unknown>; } = {}
) =>
  Effect.gen(function*() {
    const rmq = yield* Rmq;
    const name = typeof exchange === "string" ? exchange : exchange.name;
    yield* typeof exchange === "string"
      ? rmq.declareExchange(name)
      : rmq.declareExchange(name, exchange);
    yield* rmq.declareQueue(queue, { durable: true });
    yield* rmq.bind(binding.routingKey ?? "#", name, queue, binding.args ?? {});
  });

const failureOf = (exit: Exit.Exit<unknown, Producer.PublishError>) =>
  Exit.isFailure(exit) ? O.getOrThrow(Cause.findErrorOption(exit.cause)) : assert.fail("the publish succeeded");

test("a batch reaches every queue bound to the exchange, in order, in the contract's format, each with a lasting id", async (t) => {
  if (skipIfNoDocker(t)) return;
  const exchange = `publish.${Date.now()}`;
  const Work = workOn(exchange);
  const readWork = read(Contract.negotiate(Work), Schema.decodeUnknownOption(Work.schema));

  const { ids, got } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(exchange, `${exchange}.a`);
    yield* bindQueue(exchange, `${exchange}.b`);
    const work = yield* Producer.publisher(Work);
    assert.equal(work.exchange.name, exchange);
    const ids = yield* work.publish(Producer.batch([{ apiId: "a", n: 0 }, { apiId: "a", n: 1 }, { apiId: "a", n: 2 }]));
    const got = [];
    for (const queue of [`${exchange}.a`, `${exchange}.b`]) {
      for (let i = 0; i < 3; i++) got.push(O.getOrThrow(yield* rmq.get(queue)));
    }
    for (const m of got) yield* m.ack;
    return { ids, got };
  }));

  const [runId] = ids[0]!.split(":");
  assert.deepEqual(ids, [`${runId}:0`, `${runId}:1`, `${runId}:2`], "<run>:<n>, one run per publisher");
  got.forEach((m, i) => {
    const n = i % 3;
    assert.deepEqual(m.messageId, O.some(ids[n]));
    assert.deepEqual(m.contentType, O.some("application/json"));
    assert.deepEqual(m.type, O.some("egress.work"));
    assert.deepEqual(readWork(m.body, m), Result.succeed({ apiId: "a", n }), "a consumer of the contract reads it");
  });
});

test("`one` is published as a batch of one: confirmed, with the next id in the publisher's sequence", async (t) => {
  if (skipIfNoDocker(t)) return;
  const exchange = `single.${Date.now()}`;
  const Work = workOn(exchange);

  const { batch, single, got } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(exchange, `${exchange}.q`);
    const work = yield* Producer.publisher(Work);
    const batch = yield* work.publish(Producer.batch([{ apiId: "a", n: 0 }, { apiId: "a", n: 1 }]));
    const single = yield* work.publish(Producer.one({ apiId: "a", n: 2 }));
    const got = [];
    for (let i = 0; i < 3; i++) got.push(O.getOrThrow(yield* rmq.get(`${exchange}.q`)));
    for (const m of got) yield* m.ack;
    return { batch, single, got };
  }));

  const [runId] = single.split(":");
  assert.equal(single, `${runId}:2`);
  assert.deepEqual(got.map((m) => O.getOrThrow(m.messageId)), [...batch, single]);
});

test("with no queue bound, a publish fails as unroutable instead of vanishing", async (t) => {
  if (skipIfNoDocker(t)) return;
  const Work = workOn(`unbound.${Date.now()}`);

  const exit = await run(Effect.gen(function*() {
    const work = yield* Producer.publisher(Work);
    return yield* Effect.exit(work.publish(Producer.one({ apiId: "a", n: 0 })));
  }));

  assert.equal(failureOf(exit).reason._tag, "Unroutable", "told apart from a nack or a lost channel");
});

test("a message the contract refuses fails the batch with nothing sent", async (t) => {
  if (skipIfNoDocker(t)) return;
  const exchange = `refuse.${Date.now()}`;
  const Work = workOn(exchange);

  const { exit, left, counted } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(exchange, `${exchange}.q`);
    const work = yield* Producer.publisher(Work);
    const exit = yield* Effect.exit(work.publish(Producer.batch([{ apiId: "a", n: 0 }, { apiId: "a", n: 0.5 }])));
    const counted = yield* Metric.value(
      Metric.withAttributes(Telemetry.failed, { exchange, reason: "contract_refused" })
    );
    return { exit, left: yield* rmq.get(`${exchange}.q`), counted: counted.count };
  }));

  assert.equal(failureOf(exit).reason._tag, "ContractRefused");
  assert.ok(O.isNone(left), "the valid message in the batch was not sent either");
  assert.equal(counted, 2, "counted by the SDK, since the broker never saw the publication");
});

test("a headers exchange routes on the headers a publisher sets, matched against each binding's arguments", async (t) => {
  if (skipIfNoDocker(t)) return;
  const name = `headers.${Date.now()}`;
  const Work = workOn({ name, type: "headers" });

  const { eu, us, unmatched } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(Work.exchange, `${name}.eu`, { routingKey: "", args: { "x-match": "all", region: "eu" } });
    yield* bindQueue(Work.exchange, `${name}.us`, { routingKey: "", args: { "x-match": "all", region: "us" } });
    const work = yield* Producer.publisher(Work, { headers: { region: "eu" } });
    yield* work.publish(Producer.one({ apiId: "a", n: 0 }));
    yield* work.publish(Producer.one({ apiId: "a", n: 1 }, { headers: { region: "us" } }));
    const unmatched = yield* Effect.exit(
      work.publish(Producer.one({ apiId: "a", n: 2 }, { headers: { region: "apac" } }))
    );
    const eu = O.getOrThrow(yield* rmq.get(`${name}.eu`));
    const us = O.getOrThrow(yield* rmq.get(`${name}.us`));
    yield* eu.ack;
    yield* us.ack;
    return { eu, us, unmatched };
  }));

  assert.deepEqual(eu.properties["region"], "eu", "the publisher's headers ride on the message");
  assert.deepEqual(us.properties["region"], "us", "a call's headers override the publisher's");
  assert.equal(failureOf(unmatched).reason._tag, "Unroutable", "no binding matches");
});

test("a direct exchange routes on the routing key, set per publisher or per call", async (t) => {
  if (skipIfNoDocker(t)) return;
  const name = `direct.${Date.now()}`;
  const Work = workOn({ name, type: "direct" });

  const got = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(Work.exchange, `${name}.payments`, { routingKey: "payments" });
    yield* bindQueue(Work.exchange, `${name}.refunds`, { routingKey: "refunds" });
    const work = yield* Producer.publisher(Work, { routingKey: "payments" });
    yield* work.publish(Producer.batch([{ apiId: "a", n: 0 }]));
    yield* work.publish(Producer.batch([{ apiId: "a", n: 1 }], { routingKey: "refunds" }));
    const payments = O.getOrThrow(yield* rmq.get(`${name}.payments`));
    const refunds = O.getOrThrow(yield* rmq.get(`${name}.refunds`));
    yield* payments.ack;
    yield* refunds.ack;
    return { payments: payments.messageId, refunds: refunds.messageId };
  }));

  assert.match(O.getOrThrow(got.payments), /:0$/);
  assert.match(O.getOrThrow(got.refunds), /:1$/);
});

test("`mandatory: false` lets the broker drop what no queue is bound to receive", async (t) => {
  if (skipIfNoDocker(t)) return;
  const Work = workOn(`optional.${Date.now()}`);

  const id = await run(Effect.gen(function*() {
    const work = yield* Producer.publisher(Work, { mandatory: false });
    return yield* work.publish(Producer.one({ apiId: "a", n: 0 }));
  }));

  assert.match(id, /^[0-9a-f]{8}:0$/, "confirmed, with its id, though nobody received it");
});

test("a failed publication repeated with the ids it carries arrives as the same messages, not new work", async (t) => {
  if (skipIfNoDocker(t)) return;
  const exchange = `repeat.${Date.now()}`;
  const Work = workOn(exchange);
  const messages = [{ apiId: "a", n: 0 }, { apiId: "a", n: 1 }];

  const { failedWith, repeated, got } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    const work = yield* Producer.publisher(Work);
    // Nobody is bound yet: the batch fails as unroutable, and says which ids it was sent with.
    const failedWith = yield* work.publish(Producer.batch(messages)).pipe(
      Effect.andThen(() => Effect.die("the publish should have been unroutable")),
      Effect.catchReason("PublishError", "Unroutable", (reason) => Effect.succeed(reason.ids))
    );
    yield* bindQueue(exchange, `${exchange}.q`);
    const repeated = yield* work.publish(Producer.batch(messages, { ids: failedWith }));
    const got = [];
    for (let i = 0; i < 2; i++) got.push(O.getOrThrow(yield* rmq.get(`${exchange}.q`)));
    for (const m of got) yield* m.ack;
    return { failedWith, repeated, got };
  }));

  assert.equal(failedWith.length, 2);
  assert.deepEqual(repeated, failedWith, "the repeat kept its ids");
  assert.deepEqual(got.map((m) => O.getOrThrow(m.messageId)), repeated);
});

test("under a broker alarm a publication waits for the connection to be unblocked, then is confirmed", async (t) => {
  if (skipIfNoDocker(t)) return;
  const exchange = `alarm.${Date.now()}`;
  const Work = workOn(exchange);

  const { heldUnderAlarm, ids, got } = await run(Effect.gen(function*() {
    const rmq = yield* Rmq;
    yield* bindQueue(exchange, `${exchange}.q`);
    const work = yield* Producer.publisher(Work);
    yield* Effect.promise(() => brokerExec(["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", "1MiB"]));
    const held = yield* Effect.forkChild(work.publish(Producer.batch([{ apiId: "a", n: 0 }, { apiId: "a", n: 1 }])));
    let heldUnderAlarm: boolean;
    try {
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 3000)));
      heldUnderAlarm = held.pollUnsafe() === undefined;
    } finally {
      yield* Effect.promise(() => brokerExec(["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", "1GiB"]));
    }
    const ids = yield* Fiber.join(held).pipe(Effect.timeout("10 seconds"));
    const got = [];
    for (let i = 0; i < 2; i++) got.push(O.getOrThrow(yield* rmq.get(`${exchange}.q`)));
    for (const m of got) yield* m.ack;
    return { heldUnderAlarm, ids, got };
  }));

  assert.ok(heldUnderAlarm, "the alarm holds the publication rather than failing it");
  assert.deepEqual(got.map((m) => O.getOrThrow(m.messageId)), ids, "confirmed and delivered once unblocked");
});

/** A contract on a shared exchange, opted in by its route, of its own AMQP `type`. */
const routed = (exchange: Contract.ExchangeInput, route: Contract.RouteInput, type: string) =>
  Contract.make(Schema.Struct({ apiId: Schema.String, n: Schema.Int }), {
    exchange,
    route,
    type,
    formats: { "application/json": text(Schema.fromJsonString(Schema.Unknown)) }
  });

/** A queue bound as a consumer of `contract` binds it by default. */
const bindAsConsumer = (contract: Contract.Contract<unknown>, queue: string) =>
  bindQueue(contract.exchange, queue, Contract.binding(contract));

const drainTypes = (queue: string) =>
  Effect.gen(function*() {
    const rmq = yield* Rmq;
    const types: Array<string> = [];
    for (let m = yield* rmq.get(queue); O.isSome(m); m = yield* rmq.get(queue)) {
      types.push(O.getOrThrow(m.value.type));
      yield* m.value.ack;
    }
    return types;
  });

test("contracts that share an exchange by route each reach only their own consumers", async (t) => {
  if (skipIfNoDocker(t)) return;
  const name = `shared.${Date.now()}`;
  const Payment = routed(name, "payment", "test.payment");
  const Refund = routed(name, "refund", "test.refund");

  const got = await run(Effect.gen(function*() {
    yield* bindAsConsumer(Payment, `${name}.payments`);
    yield* bindAsConsumer(Refund, `${name}.refunds`);
    const payments = yield* Producer.publisher(Payment);
    const refunds = yield* Producer.publisher(Refund);
    yield* payments.publish(Producer.batch([{ apiId: "a", n: 0 }, { apiId: "a", n: 1 }]));
    yield* refunds.publish(Producer.one({ apiId: "a", n: 2 }));
    yield* Effect.sleep("200 millis");
    return { payments: yield* drainTypes(`${name}.payments`), refunds: yield* drainTypes(`${name}.refunds`) };
  }));

  assert.deepEqual(got.payments, ["test.payment", "test.payment"]);
  assert.deepEqual(got.refunds, ["test.refund"], "no payment reached the refunds' queue");
});

test("on a shared headers exchange, the route's headers are stamped on every message and win over the caller's", async (t) => {
  if (skipIfNoDocker(t)) return;
  const name = `shared.headers.${Date.now()}`;
  const exchange = { name, type: "headers" } as const;
  const Payment = routed(exchange, { headers: { kind: "payment" } }, "test.payment");
  const Refund = routed(exchange, { headers: { kind: "refund" } }, "test.refund");

  const got = await run(Effect.gen(function*() {
    yield* bindAsConsumer(Payment, `${name}.payments`);
    yield* bindAsConsumer(Refund, `${name}.refunds`);
    const payments = yield* Producer.publisher(Payment, { headers: { region: "eu" } });
    yield* payments.publish(Producer.one({ apiId: "a", n: 0 }, { headers: { kind: "refund" } }));
    yield* Effect.sleep("200 millis");
    return { payments: yield* drainTypes(`${name}.payments`), refunds: yield* drainTypes(`${name}.refunds`) };
  }));

  assert.deepEqual(got.payments, ["test.payment"]);
  assert.deepEqual(got.refunds, [], "a caller's header cannot reroute a routed contract");
});

test("a routing key beside a contract's route is a defect: it would deliver to another contract's consumers", async (t) => {
  if (skipIfNoDocker(t)) return;
  const Payment = routed(`shared.defect.${Date.now()}`, "payment", "test.payment");

  const { atMake, atCall } = await run(Effect.gen(function*() {
    const atMake = yield* Effect.exit(Producer.publisher(Payment, { routingKey: "refund" }));
    const payments = yield* Producer.publisher(Payment);
    const atCall = yield* Effect.exit(payments.publish(Producer.one({ apiId: "a", n: 0 }, { routingKey: "refund" })));
    return { atMake, atCall };
  }));

  const died = (exit: Exit.Exit<unknown, unknown>) => Exit.isFailure(exit) && Cause.hasDies(exit.cause);
  assert.ok(died(atMake), "making the publisher dies rather than fails");
  assert.ok(died(atCall), "so does the call");
});
