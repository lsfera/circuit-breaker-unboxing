import assert from "node:assert/strict";
import { test } from "vitest";
import { AdaptiveLimit } from "../src/Limiter.ts";

const make = (over: Partial<ConstructorParameters<typeof AdaptiveLimit>[0]> = {}) =>
  new AdaptiveLimit({ min: 2, max: 20, decrease: 0.5, ...over });

test("starts at the configured maximum", () => {
  assert.equal(make().slots, 20);
});

test("a 429 multiplies the limit down, and never below the floor", () => {
  const l = make();
  l.throttled(l.epoch);
  assert.equal(l.slots, 10);
  for (let i = 0; i < 10; i++) l.throttled(l.epoch);
  assert.equal(l.slots, 2);
});

test("a burst of 429s to calls started under the same limit decreases it once", () => {
  const l = make();
  const started = l.epoch;
  for (let i = 0; i < 15; i++) l.throttled(started);
  assert.equal(l.slots, 10, "fifteen answers to one round of calls are one signal, not fifteen");
  l.throttled(l.epoch);
  assert.equal(l.slots, 5, "a call started under the new limit is a new signal");
});

test("success grows it back by one slot per limit's worth of successes, and stops at the maximum", () => {
  const l = make({ max: 8 });
  l.throttled(l.epoch);
  assert.equal(l.slots, 4);
  // 1/limit each, so a little over four successes at a limit of four is one slot.
  for (let i = 0; i < 5; i++) l.succeeded();
  assert.equal(l.slots, 5);
  for (let i = 0; i < 1000; i++) l.succeeded();
  assert.equal(l.slots, 8);
});

test("with min equal to max the limit never moves", () => {
  const l = make({ min: 20 });
  l.throttled(l.epoch);
  l.succeeded();
  assert.equal(l.slots, 20);
});
