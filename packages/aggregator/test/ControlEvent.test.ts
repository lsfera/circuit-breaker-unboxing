import { test } from "node:test";
import assert from "node:assert/strict";
import { Option as O } from "effect";
import { decodeControlEvent, encodeControlEvent, readsControlFormat } from "@egress/rmq/ControlPlane.ts";

const event = { apiId: "payments-provider", instance: "a", state: "half_open", at: 1_000 } as const;

test("what a replica encodes, the aggregator decodes", () => {
  assert.deepEqual(decodeControlEvent(encodeControlEvent(event)), O.some(event));
});

test("a body that is not a breaker transition decodes to nothing, not an exception", () => {
  for (const body of [
    "not json",
    "null",
    "[]",
    "{}",
    JSON.stringify({ ...event, state: "degraded" }),
    JSON.stringify({ ...event, state: "" }),
    JSON.stringify({ ...event, at: "1000" }),
    JSON.stringify({ ...event, at: 1.5 }),
    JSON.stringify({ ...event, instance: 7 }),
    JSON.stringify({ apiId: event.apiId, state: event.state, at: event.at }),
  ]) {
    assert.deepEqual(decodeControlEvent(body), O.none(), body);
  }
});

test("each of the four states a replica can publish decodes", () => {
  for (const state of ["closed", "open", "half_open", "isolated"] as const) {
    assert.deepEqual(decodeControlEvent(encodeControlEvent({ ...event, state })), O.some({ ...event, state }));
  }
});

test("a field the aggregator does not know about does not stop it reading the rest", () => {
  assert.deepEqual(decodeControlEvent(JSON.stringify({ ...event, extra: true })), O.some(event));
});

const declared = (contentType?: string, contentEncoding?: string, type?: string) => ({
  contentType: O.fromNullishOr(contentType),
  contentEncoding: O.fromNullishOr(contentEncoding),
  type: O.fromNullishOr(type),
});

test("the aggregator reads a transition, and a message that declared nothing", () => {
  assert.equal(readsControlFormat(declared("application/json", undefined, "egress.circuit.transition")), true);
  assert.equal(readsControlFormat(declared("application/json; charset=utf-8", "identity")), true);
  assert.equal(readsControlFormat(declared()), true);
});

test("the aggregator declines another message type, content type or encoding", () => {
  assert.equal(readsControlFormat(declared("application/json", undefined, "egress.work")), false);
  assert.equal(readsControlFormat(declared("text/plain")), false);
  assert.equal(readsControlFormat(declared("application/json", "gzip")), false);
});
