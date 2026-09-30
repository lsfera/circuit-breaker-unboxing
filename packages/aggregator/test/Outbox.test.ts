import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Exit, Option as O } from "effect";
import { Outbox, OUTBOX_MAX_PER_API, RedisOutboxLayer } from "../src/Outbox.ts";
import { makeInMemoryOutbox } from "./support/InMemory.ts";
import type { RedisLike } from "../src/Coordination.ts";
import { SOURCE } from "../src/Events.ts";
import type { Entry } from "../src/Outbox.ts";
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

/**
 * `peek` returns positions, not just events. `None` is an entry that no longer
 * decodes, which only the Redis outbox can produce — in memory it would be a
 * bug, so it reads as -1 rather than being quietly skipped.
 */
const sequenceOf = (entry: Entry): number =>
  O.match(entry, { onNone: () => -1, onSome: (e) => e.data.sequence });

test("entries come back in the order they were appended, per API", async () => {
  const seen = await Effect.runPromise(
    Effect.gen(function* () {
      const outbox = yield* makeInMemoryOutbox;
      for (const seq of [1, 2, 3]) yield* outbox.append(event("payments", seq));
      yield* outbox.append(event("shipping", 9));

      const payments = (yield* outbox.peek("payments", 10)).entries;
      const apis = yield* outbox.apis;
      return { payments: payments.map(sequenceOf), apis: [...apis].sort() };
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
      const afterCommit = (yield* outbox.peek("payments", 10)).entries.map(sequenceOf);
      const depth = yield* outbox.depth("payments");

      yield* outbox.commit("payments", 3);
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
      const head = (yield* outbox.peek("payments", 1)).entries;
      return {
        dropped,
        first: head[0] === undefined ? undefined : sequenceOf(head[0]),
        depth: yield* outbox.depth("payments"),
      };
    }),
  );

  assert.equal(dropped, 3, "three over the bound means three dropped, counted for the metric");
  assert.equal(depth, OUTBOX_MAX_PER_API, "the outbox never grows past its bound");
  assert.equal(first, 4, "the three oldest went, not the three newest");
});

/**
 * A delivery that fails appends while a drain is posting what it peeked. At the
 * bound, that append drops the oldest entries — the very ones the drain holds —
 * and a commit by count then trimmed that many again from the new head,
 * removing entries nobody had delivered. Committing the position got to trims
 * only what is still there of what was delivered.
 */
test("a commit after the bound dropped entries mid-drain removes only what was delivered", async () => {
  const { remaining } = await Effect.runPromise(
    Effect.gen(function* () {
      const outbox = yield* makeInMemoryOutbox;
      for (let seq = 1; seq <= OUTBOX_MAX_PER_API; seq++) yield* outbox.append(event("payments", seq));

      const { from, entries } = yield* outbox.peek("payments", 50);
      // While those 50 are being posted, 10 more fail: the 10 oldest are dropped.
      for (let seq = OUTBOX_MAX_PER_API + 1; seq <= OUTBOX_MAX_PER_API + 10; seq++) {
        yield* outbox.append(event("payments", seq));
      }
      yield* outbox.commit("payments", from + entries.length);
      return { remaining: (yield* outbox.peek("payments", 1)).entries.map(sequenceOf) };
    }),
  );

  assert.deepEqual(remaining, [51], "the first undelivered entry is next, not the 61st");
});

test("a Redis call that never answers fails the outbox call instead of hanging it", async () => {
  // A one-sided partition: the command is queued on a socket that never answers.
  const hung: RedisLike = { eval: () => new Promise(() => {}) };
  const started = Date.now();
  const exit = await Effect.runPromise(
    Effect.exit(Effect.flatMap(Outbox, (outbox) => outbox.peek("payments", 10))).pipe(
      Effect.provide(RedisOutboxLayer(hung)),
    ),
  );
  assert.equal(Exit.isFailure(exit), true);
  assert.ok(Date.now() - started < 3000, "failed within the coordination timeout");
});
