import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import { makeInMemoryOutbox, OUTBOX_MAX_PER_API } from "../src/Outbox.ts";
import { SOURCE } from "../src/Events.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * The outbox with no Redis and no HTTP: ordering, the bound, and what `commit`
 * may remove. This file is about the rules; test/integration/Outbox.test.ts is
 * about whether Lua and a socket obey them.
 */

const event = (apiId: string, sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type: "egress.circuit.state_changed",
  source: SOURCE,
  subject: `api://${apiId}`,
  id: `id-${apiId}-${sequence}`,
  time: new Date(1_700_000_000_000 + sequence).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId,
    sequence,
    previousState: "CLOSED",
    state: "OPEN",
    reason: "ALL_ENDPOINTS_EJECTED",
    healthyEndpoints: 0,
    totalEndpoints: 6,
    observedSince: new Date(1_700_000_000_000).toISOString(),
    reportingReplicas: 3,
  },
});

test("entries come back in the order they were appended, per API", async () => {
  const seen = await Effect.runPromise(
    Effect.gen(function* () {
      const outbox = yield* makeInMemoryOutbox;
      for (const seq of [1, 2, 3]) yield* outbox.append(event("payments", seq));
      yield* outbox.append(event("shipping", 9));

      const payments = yield* outbox.peek("payments", 10);
      const apis = yield* outbox.apis;
      return { payments: payments.map((e) => e.data.sequence), apis: [...apis].sort() };
    }),
  );

  assert.deepEqual(seen.payments, [1, 2, 3], "order is the guarantee; peek must not reorder");
  assert.deepEqual(seen.apis, ["payments", "shipping"], "each API is its own queue");
});

test("commit removes only what was delivered, and leaves the rest at the head", async () => {
  const { afterCommit, depth, apisWhenEmpty } = await Effect.runPromise(
    Effect.gen(function* () {
      const outbox = yield* makeInMemoryOutbox;
      for (const seq of [1, 2, 3]) yield* outbox.append(event("payments", seq));

      // Two delivered, the third failed: exactly two may go.
      yield* outbox.commit("payments", 2);
      const afterCommit = (yield* outbox.peek("payments", 10)).map((e) => e.data.sequence);
      const depth = yield* outbox.depth("payments");

      yield* outbox.commit("payments", 1);
      return { afterCommit, depth, apisWhenEmpty: yield* outbox.apis };
    }),
  );

  assert.deepEqual(afterCommit, [3], "a partial drain leaves the undelivered tail, in order");
  assert.equal(depth, 1);
  assert.deepEqual(apisWhenEmpty, [], "an API with nothing pending stops being listed");
});

/**
 * Dropping the oldest is deliberate: a returning subscriber loses history it can
 * see it lost, where dropping the newest would leave it confidently stale.
 */
test("the bound drops the oldest entries and says how many", async () => {
  const { dropped, first, depth } = await Effect.runPromise(
    Effect.gen(function* () {
      const outbox = yield* makeInMemoryOutbox;
      let dropped = 0;
      for (let seq = 1; seq <= OUTBOX_MAX_PER_API + 3; seq++) {
        dropped += yield* outbox.append(event("payments", seq));
      }
      const head = yield* outbox.peek("payments", 1);
      return {
        dropped,
        first: head[0]?.data.sequence,
        depth: yield* outbox.depth("payments"),
      };
    }),
  );

  assert.equal(dropped, 3, "three over the bound means three dropped, counted for the metric");
  assert.equal(depth, OUTBOX_MAX_PER_API, "the outbox never grows past its bound");
  assert.equal(first, 4, "the three oldest went, not the three newest");
});
