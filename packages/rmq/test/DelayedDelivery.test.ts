import { test } from "node:test";
import assert from "node:assert/strict";
import * as Delay from "../src/DelayedDelivery.ts";

/** The routing key is the whole contract between a sender and the chain, so it is pinned as data. */
const value = (key: string): number =>
  key.split(".").slice(0, Delay.LEVELS).reduce((sum, bit) => sum * 2 + Number(bit), 0);

test("a delay round-trips through its routing key, for every magnitude a breaker will ask for", () => {
  for (const seconds of [1, 2, 3, 5, 30, 60, 3_600, 86_400, Delay.MAX_DELAY_SECONDS]) {
    assert.equal(value(Delay.routingKey(seconds, "q")), seconds);
  }
});

test("the destination follows the bits and may itself contain dots", () => {
  const key = Delay.routingKey(5, "payments.breaker.wake.host-1");
  assert.equal(key.split(".").length, Delay.LEVELS + 4);
  assert.ok(key.endsWith(".payments.breaker.wake.host-1"));
  assert.equal(key.split(".").slice(-6, -4).join(""), "01", "the low bits of 5 are 101, the last two before the destination are 0 and 1");
});

test("a message enters the chain at its highest set bit", () => {
  assert.equal(Delay.entryLevel(1), 0);
  assert.equal(Delay.entryLevel(5), 2);
  assert.equal(Delay.entryLevel(86_400), 16);
  assert.equal(Delay.entryLevel(Delay.MAX_DELAY_SECONDS), Delay.LEVELS - 1);
});

test("a delay is whole seconds, at least one and at most what the chain can count", () => {
  assert.equal(Delay.clampSeconds(0), 1);
  assert.equal(Delay.clampSeconds(-4), 1);
  assert.equal(Delay.clampSeconds(2.2), 3);
  assert.equal(Delay.clampSeconds(10 ** 9), Delay.MAX_DELAY_SECONDS);
});

test("a day fits, with room to spare", () => {
  assert.ok(Delay.MAX_DELAY_SECONDS >= 24 * 60 * 60);
});

test("a destination binds on its own name, whatever the bits in front of it", () => {
  assert.equal(Delay.bindingKey("payments.wake.h1"), "#.payments.wake.h1");
});
