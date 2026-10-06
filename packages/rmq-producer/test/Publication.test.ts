import { Option as O } from "effect";
import assert from "node:assert/strict";
import { test } from "vitest";
import * as Producer from "../src/index.ts";

test("a publication says whether it is one message or a batch, even when a message is itself an array", () => {
  const pair: ReadonlyArray<number> = [1, 2];
  const single: Producer.One<ReadonlyArray<number>> = Producer.one(pair);
  const several: Producer.Batch<ReadonlyArray<number>> = Producer.batch([pair, pair]);
  assert.equal(single._tag, "One");
  assert.deepEqual(single.message, [1, 2]);
  assert.equal(several._tag, "Batch");
  assert.deepEqual(several.messages, [[1, 2], [1, 2]]);
  assert.ok(O.isNone(single.id) && O.isNone(several.ids), "the publisher stamps ids unless given");
});

test("a publication carries its routing, and none unless given", () => {
  assert.deepEqual(Producer.one("m", { routingKey: "k", headers: { region: "eu" } }).routing, {
    routingKey: "k",
    headers: { region: "eu" }
  });
  assert.deepEqual(Producer.batch(["m"]).routing, {});
});

test("a repeat names its ids, one per message, apart from its routing", () => {
  const repeat = Producer.batch(["a", "b"], { ids: ["r:0", "r:1"], routingKey: "k" });
  assert.deepEqual(repeat.ids, O.some(["r:0", "r:1"]));
  assert.deepEqual(repeat.routing, { routingKey: "k" });
  assert.deepEqual(Producer.one("a", { id: "r:2" }).id, O.some("r:2"));
  assert.throws(() => Producer.batch(["a", "b"], { ids: ["r:0"] }), /2 messages given 1 ids/);
});
