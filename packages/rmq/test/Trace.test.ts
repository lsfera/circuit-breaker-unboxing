import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import { parentFrom, traceparent } from "../src/Trace.ts";

const TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
const SPAN = "00f067aa0ba902b7";

test("a W3C traceparent becomes the parent span", () => {
  const parent = parentFrom(`00-${TRACE}-${SPAN}-01`);
  assert.deepEqual(O.map(parent, (p) => [p.traceId, p.spanId]), O.some([TRACE, SPAN]));
});

test("anything unparseable is no parent, never an error", () => {
  for (const header of ["", "garbage", `01-${TRACE}-${SPAN}-01`, `00-${TRACE.toUpperCase()}-${SPAN}-01`, `00-${TRACE}-${SPAN}`]) {
    assert.equal(O.isNone(parentFrom(header)), true, header);
  }
});

test("what traceparent writes, parentFrom reads back", async () => {
  const header = await Effect.runPromise(traceparent.pipe(Effect.withSpan("publish")));
  const read = O.flatMap(header, parentFrom);
  assert.equal(O.isSome(read), true, String(O.getOrUndefined(header)));
});
