import { test } from "node:test";
import assert from "node:assert/strict";
import { Option as O, Result, Schema } from "effect";
import { accept, read } from "../src/Negotiation.ts";

/** Configured as packages/consumer configures it. */
const Json = Schema.fromJsonString(Schema.Unknown);
const json = accept({ "application/json": Json }, { undeclared: "application/json", type: "egress.work" });

const declared = (contentType?: string, contentEncoding?: string, type?: string) => ({
  contentType: O.fromNullishOr(contentType),
  contentEncoding: O.fromNullishOr(contentEncoding),
  type: O.fromNullishOr(type),
});

test("JSON is read with or without parameters, unencoded, and a message that declared nothing when undeclared is JSON", () => {
  for (const [type, encoding, kind] of [
    ["application/json", undefined, "egress.work"],
    ["Application/JSON; charset=utf-8", undefined, undefined],
    [undefined, undefined, undefined],
    ["application/json", "identity", "egress.work"],
    ["application/json", "", undefined],
  ] as const) {
    assert.ok(O.isSome(json(declared(type, encoding, kind))), `${type} / ${encoding} / ${kind}`);
  }
});

test("a content type, encoding or message type the application does not read is declined", () => {
  for (const [type, encoding, kind] of [
    ["text/plain", undefined, undefined],
    ["application/xml", undefined, undefined],
    ["application/jsonp", undefined, undefined],
    ["application/json", "gzip", undefined],
    ["application/json", "identity, gzip", undefined],
    [undefined, "gzip", undefined],
    ["application/json", undefined, "egress.refund"],
  ] as const) {
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
  const both = accept({ "application/json": Json, "Text/Plain": Schema.String });
  const parse = (contentType: string, body: string) =>
    O.flatMap(both(declared(contentType)), (parser) => Schema.decodeUnknownOption(parser)(body));
  assert.deepEqual(parse("text/plain; charset=utf-8", "payments-provider,7"), O.some("payments-provider,7"));
  assert.deepEqual(parse("application/json", '{"n":7}'), O.some({ n: 7 }));
});

const Payment = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
const readPayment = read(json, Schema.decodeUnknownOption(Payment));
const asJson = declared("application/json", undefined, "egress.work");

test("what the producer publishes, the contract reads", () => {
  assert.deepEqual(readPayment('{"apiId":"payments-provider","n":7}', asJson), Result.succeed({ apiId: "payments-provider", n: 7 }));
});

test("a field the contract does not know about does not stop it reading the rest", () => {
  assert.deepEqual(
    readPayment('{"apiId":"payments-provider","n":3,"extra":true}', asJson),
    Result.succeed({ apiId: "payments-provider", n: 3 }),
  );
});

test("a body that does not parse, or is not a message of the contract, is malformed — an answer, not an exception", () => {
  for (const body of [
    "not json",
    "null",
    "[]",
    "{}",
    '{"apiId":"payments-provider"}',
    '{"apiId":"payments-provider","n":"7"}',
    '{"apiId":"payments-provider","n":1.5}',
    '{"apiId":7,"n":1}',
  ]) {
    assert.deepEqual(readPayment(body, asJson), Result.fail("malformed"), body);
  }
});

test("a declined format is not parsed at all", () => {
  assert.deepEqual(readPayment("not json", declared("text/plain")), Result.fail("format"));
});
