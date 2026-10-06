import { Option as O, Result, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "vitest";
import { accept, bytes, read, text } from "../src/Negotiation.ts";

/** Configured as packages/consumer configures it. */
const Json = text(Schema.fromJsonString(Schema.Unknown));
const utf8 = (body: string) => Buffer.from(body, "utf8");
const json = accept({ "application/json": Json }, { undeclared: "application/json", type: "egress.work" });

const declared = (contentType?: string, contentEncoding?: string, type?: string) => ({
  contentType: O.fromNullishOr(contentType),
  contentEncoding: O.fromNullishOr(contentEncoding),
  type: O.fromNullishOr(type)
});

test("JSON is read with or without parameters, unencoded, and a message that declared nothing when undeclared is JSON", () => {
  for (
    const [type, encoding, kind] of [
      ["application/json", undefined, "egress.work"],
      ["Application/JSON; charset=utf-8", undefined, undefined],
      [undefined, undefined, undefined],
      ["application/json", "identity", "egress.work"],
      ["application/json", "", undefined]
    ] as const
  ) {
    assert.ok(O.isSome(json(declared(type, encoding, kind))), `${type} / ${encoding} / ${kind}`);
  }
});

test("a content type, encoding or message type the application does not read is declined", () => {
  for (
    const [type, encoding, kind] of [
      ["text/plain", undefined, undefined],
      ["application/xml", undefined, undefined],
      ["application/jsonp", undefined, undefined],
      ["application/json", "gzip", undefined],
      ["application/json", "identity, gzip", undefined],
      [undefined, "gzip", undefined],
      ["application/json", undefined, "egress.refund"]
    ] as const
  ) {
    assert.ok(O.isNone(json(declared(type, encoding, kind))), `${type} / ${encoding} / ${kind}`);
  }
});

test("nothing is implied: without `undeclared` a message that declares no content type is declined", () => {
  assert.ok(O.isNone(accept({ "application/json": Json })(declared())));
});

test("without `type` any declared message type is read", () => {
  assert.ok(O.isSome(accept({ "application/json": Json })(declared("application/json", undefined, "anything"))));
});

test("each media type gets its own parser", () => {
  const all = accept({
    "application/json": Json,
    "Text/Plain": text(Schema.String),
    "application/octet-stream": Schema.Uint8Array
  });
  const parse = (contentType: string, body: Uint8Array) =>
    O.flatMap(all(declared(contentType)), (parser) => Schema.decodeUnknownOption(parser)(body));
  assert.deepEqual(parse("text/plain; charset=utf-8", utf8("payments-provider,7")), O.some("payments-provider,7"));
  assert.deepEqual(parse("application/json", utf8("{\"n\":7}")), O.some({ n: 7 }));
  // A binary format gets the bytes exactly as published, including ones that are not UTF-8.
  const binary = Uint8Array.of(0x1f, 0x8b, 0xff, 0x00);
  assert.deepEqual(parse("application/octet-stream", binary), O.some(binary));
});

const Payment = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
const readPayment = read(json, Schema.decodeUnknownOption(Payment));
const asJson = declared("application/json", undefined, "egress.work");

test("what the producer publishes, the contract reads", () => {
  assert.deepEqual(
    readPayment(utf8("{\"apiId\":\"payments-provider\",\"n\":7}"), asJson),
    Result.succeed({ apiId: "payments-provider", n: 7 })
  );
});

test("a field the contract does not know about does not stop it reading the rest", () => {
  assert.deepEqual(
    readPayment(utf8("{\"apiId\":\"payments-provider\",\"n\":3,\"extra\":true}"), asJson),
    Result.succeed({ apiId: "payments-provider", n: 3 })
  );
});

test("a body that does not parse, or is not a message of the contract, is malformed — an answer, not an exception", () => {
  for (
    const body of [
      "not json",
      "null",
      "[]",
      "{}",
      "{\"apiId\":\"payments-provider\"}",
      "{\"apiId\":\"payments-provider\",\"n\":\"7\"}",
      "{\"apiId\":\"payments-provider\",\"n\":1.5}",
      "{\"apiId\":7,\"n\":1}"
    ]
  ) {
    assert.deepEqual(readPayment(utf8(body), asJson), Result.fail("malformed"), body);
  }
});

test("a declined format is not parsed at all", () => {
  assert.deepEqual(readPayment(utf8("not json"), declared("text/plain")), Result.fail("format"));
});

test("a binary format is read by its own decoder, and a body the decoder throws on is malformed", () => {
  // Stand-in for a generated decoder: the first byte is `n`, the rest is `apiId`.
  const decode = (body: Uint8Array) => {
    if (body.length < 2) throw new Error("truncated");
    return { n: body[0], apiId: new TextDecoder().decode(body.subarray(1)) };
  };
  const binary = read(accept({ "application/x-payment": bytes(decode) }), Schema.decodeUnknownOption(Payment));
  const asPayment = declared("application/x-payment");
  assert.deepEqual(
    binary(Uint8Array.of(7, ...utf8("payments-provider")), asPayment),
    Result.succeed({ apiId: "payments-provider", n: 7 })
  );
  assert.deepEqual(binary(Uint8Array.of(7), asPayment), Result.fail("malformed"));
});

test("a binary decoder is handed a plain Uint8Array, even when the body arrives as a Buffer", () => {
  const seen: Array<boolean> = [];
  const parser = bytes((body) => (seen.push(Buffer.isBuffer(body)), body.length));
  assert.deepEqual(Schema.decodeUnknownOption(parser)(Buffer.from([1, 2, 3])), O.some(3));
  assert.deepEqual(seen, [false]);
});

test("a text body that is not UTF-8 is malformed, not read through replacement characters", () => {
  assert.deepEqual(readPayment(Uint8Array.of(0x7b, 0xff, 0x7d), asJson), Result.fail("malformed"));
});
