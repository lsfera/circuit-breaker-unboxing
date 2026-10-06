import { Effect, Exit, Option as O, Result, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "vitest";
import * as Contract from "../src/Contract.ts";
import { bytes, read, text } from "../src/Negotiation.ts";

/** A binary format with no library behind it: `n` as one byte, so a value the format cannot hold is easy to make. */
const OneByte = bytes({
  decode: (body) => ({ n: body[0] }),
  encode: (value) => {
    const { n } = value as { n: number; };
    if (n > 255) throw new RangeError("one byte");
    return Uint8Array.of(n);
  }
});

const Count = Contract.make(Schema.Struct({ n: Schema.Int }), {
  exchange: "test.counts",
  type: "test.count",
  formats: { "application/json": text(Schema.fromJsonString(Schema.Unknown)), "application/x-one-byte": OneByte },
  undeclared: "application/json"
});

const declared = (contentType?: string, type?: string) => ({
  contentType: O.fromNullishOr(contentType),
  contentEncoding: O.none(),
  type: O.fromNullishOr(type)
});

const readCount = read(Contract.negotiate(Count), Schema.decodeUnknownOption(Count.schema));

test("a message written in each of a contract's formats reads back as the same message", async () => {
  for (const mediaType of ["application/json", "application/x-one-byte"]) {
    const body = await Effect.runPromise(Contract.encoder(Count, mediaType)({ n: 7 }));
    assert.deepEqual(readCount(body, declared(mediaType, "test.count")), Result.succeed({ n: 7 }), mediaType);
  }
  const json = await Effect.runPromise(Contract.encoder(Count, "application/json")({ n: 7 }));
  assert.equal(new TextDecoder().decode(json), `{"n":7}`);
});

test("the consumer's side follows the contract: its type, and its undeclared format", async () => {
  const body = await Effect.runPromise(Contract.encoder(Count, "application/json")({ n: 1 }));
  assert.deepEqual(
    readCount(body, declared(undefined, undefined)),
    Result.succeed({ n: 1 }),
    "undeclared reads as JSON"
  );
  assert.deepEqual(readCount(body, declared("application/json", "other.type")), Result.fail("format"));
  assert.deepEqual(readCount(body, declared("text/plain", "test.count")), Result.fail("format"));
});

test("a message the schema or the format refuses fails to encode instead of being written", async () => {
  const notInt = await Effect.runPromiseExit(Contract.encoder(Count, "application/json")({ n: 1.5 }));
  assert.ok(Exit.isFailure(notInt), "the schema refuses a non-integer n");
  const tooBig = await Effect.runPromiseExit(Contract.encoder(Count, "application/x-one-byte")({ n: 300 }));
  assert.ok(Exit.isFailure(tooBig), "the format refuses what it cannot hold");
});

test("a format that only reads cannot be written, and a contract refuses formats it does not have", async () => {
  const ReadOnly = Contract.make(Schema.Struct({ n: Schema.Int }), {
    exchange: "test.counts",
    formats: { "application/x-one-byte": bytes((body) => ({ n: body[0] })) }
  });
  assert.deepEqual(Contract.writable(ReadOnly), [], "nothing a publisher can write");
  assert.deepEqual(Contract.writable(Count), ["application/json", "application/x-one-byte"]);
  assert.throws(() => Contract.encoder(ReadOnly, "application/x-one-byte"), /only reads/);
  assert.throws(() => Contract.encoder(Count, "text/plain"), /not one of the contract's formats/);
  assert.throws(
    () =>
      Contract.make(Schema.Struct({ n: Schema.Int }), {
        exchange: "test.counts",
        formats: { "application/json": OneByte },
        undeclared: "text/plain"
      }),
    /undeclared names text\/plain/
  );
  assert.ok(Contract.isContract(Count));
  assert.ok(!Contract.isContract(Count.schema));
  const { [Contract.TypeId]: _, ...lookalike } = Count;
  assert.ok(!Contract.isContract(lookalike), "the right keys are not a contract: only `make` makes one");
});

test("an exchange named alone is a durable topic exchange; given as an object, every setting it leaves out is defaulted", () => {
  const formats = { "application/json": text(Schema.fromJsonString(Schema.Unknown)) };
  assert.deepEqual(Count.exchange, {
    name: "test.counts",
    type: "topic",
    durable: true,
    autoDelete: false,
    internal: false,
    args: {}
  });
  const Routed = Contract.make(Schema.Struct({ n: Schema.Int }), {
    exchange: { name: "test.routed", type: "headers", args: { "alternate-exchange": "test.unrouted" } },
    formats
  });
  assert.deepEqual(Routed.exchange, {
    name: "test.routed",
    type: "headers",
    durable: true,
    autoDelete: false,
    internal: false,
    args: { "alternate-exchange": "test.unrouted" }
  });
  assert.throws(
    () =>
      Contract.make(Schema.Struct({ n: Schema.Int }), { exchange: { name: "test.inner", internal: true }, formats }),
    /internal/
  );
});
