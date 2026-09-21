import { test } from "node:test";
import assert from "node:assert/strict";
import { Option as O } from "effect";
import { decodeWorkMessage, encodeWorkMessage, readsWorkFormat } from "@egress/rmq/ControlPlane.ts";
import { decide } from "../src/consumer.ts";

test("an ok call is accepted", () => {
  assert.equal(decide("ok"), "accept");
});

test("a failed call is handed back to the broker, not retried here", () => {
  assert.equal(decide("failed"), "requeue");
});

test("a client error is dead-lettered at once: repeating the request would get the same answer", () => {
  assert.equal(decide("client_error"), "discard");
});

/**
 * The work message's encoder and decoder are one declaration shared by the
 * producer and every daemon, so the properties that matter are the two ends
 * agreeing and a bad body being an answer rather than a crash.
 */
test("what the producer encodes, a daemon decodes", () => {
  const body = encodeWorkMessage({ apiId: "payments-provider", n: 7 });
  assert.deepEqual(decodeWorkMessage(body), O.some({ apiId: "payments-provider", n: 7 }));
});

test("a body that is not a work message decodes to nothing, not an exception", () => {
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
    assert.deepEqual(decodeWorkMessage(body), O.none(), body);
  }
});

test("a field this daemon does not know about does not stop it reading the rest", () => {
  assert.deepEqual(
    decodeWorkMessage('{"apiId":"payments-provider","n":3,"extra":true}'),
    O.some({ apiId: "payments-provider", n: 3 }),
  );
});

const declared = (contentType?: string, contentEncoding?: string, type?: string) => ({
  contentType: O.fromNullishOr(contentType),
  contentEncoding: O.fromNullishOr(contentEncoding),
  type: O.fromNullishOr(type),
});

test("a daemon reads JSON, with or without parameters, unencoded, and a message that declared nothing", () => {
  for (const [type, encoding, kind] of [
    ["application/json", undefined, "egress.work"],
    ["Application/JSON; charset=utf-8", undefined, undefined],
    [undefined, undefined, undefined],
    ["application/json", "identity", "egress.work"],
    ["application/json", "", undefined],
  ] as const) {
    assert.equal(readsWorkFormat(declared(type, encoding, kind)), true, `${type} / ${encoding} / ${kind}`);
  }
});

test("a daemon declines a content type or encoding it cannot read", () => {
  for (const [type, encoding, kind] of [
    ["text/plain", undefined, undefined],
    ["application/xml", undefined, undefined],
    ["application/jsonp", undefined, undefined],
    ["application/json", "gzip", undefined],
    ["application/json", "identity, gzip", undefined],
    [undefined, "gzip", undefined],
    ["application/json", undefined, "egress.refund"],
  ] as const) {
    assert.equal(readsWorkFormat(declared(type, encoding, kind)), false, `${type} / ${encoding} / ${kind}`);
  }
});

test("a call rejected by this replica's own open breaker also requeues", () => {
  // Same settlement as "failed": the difference is telemetry (no call was attempted), not what happens to the message.
  assert.equal(decide("open"), "requeue");
});
