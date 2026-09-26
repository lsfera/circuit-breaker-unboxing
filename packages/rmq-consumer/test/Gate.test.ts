import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Option as O } from "effect";
import type { Registration } from "../src/Dependency.ts";
import * as Gate from "../src/Gate.ts";

/**
 * A Gate against fake subscriptions: what the log records is what would reach RabbitMQ — which subscription
 * exists, at which prefetch — since that is how a breaker's state reaches a consumer.
 */

const closed: O.Option<Registration> = O.some({ phase: "closed", report: () => Effect.succeed(0) });
const halfOpen: O.Option<Registration> = O.some({ phase: "half-open", verdict: () => Effect.void });
const open: O.Option<Registration> = O.none();

test("any dependency open stops the consumer; else any half-open probes; else it consumes at full prefetch", () => {
  assert.equal(Gate.modeOf([closed]), "full");
  assert.equal(Gate.modeOf([halfOpen]), "probe");
  assert.equal(Gate.modeOf([open]), "none");
  assert.equal(Gate.modeOf([closed, closed]), "full");
  assert.equal(Gate.modeOf([closed, halfOpen]), "probe");
  assert.equal(Gate.modeOf([halfOpen, open]), "none");
  assert.equal(Gate.modeOf([closed, open]), "none");
});

const world = (registrations: Array<O.Option<Registration>>) =>
  Effect.gen(function* () {
    const log: Array<string> = [];
    let next = 0;
    const gate = yield* Gate.make(Effect.sync(() => registrations), {
      subscribe: (mode) =>
        Effect.sync(() => {
          const id = next++;
          log.push(`subscribe ${mode} #${id}`);
          return id;
        }),
      retire: (id) => Effect.sync(() => void log.push(`retire #${id}`)),
    });
    return { gate, log, registrations };
  });

const run = (body: Effect.Effect<void, unknown>) => Effect.runPromise(body);

test("one dependency reproduces the single breaker: full, stop, one probe, stop, full", () =>
  run(
    Effect.gen(function* () {
      const w = yield* world([open]);
      const step = (r: O.Option<Registration>) => Effect.andThen(Effect.sync(() => void (w.registrations[0] = r)), w.gate.reconcile);
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, [], "nothing subscribes until the breaker registers");
      yield* step(closed);
      yield* step(open);
      yield* step(halfOpen);
      yield* step(open);
      yield* step(closed);
      assert.deepEqual(w.log, ["subscribe full #0", "retire #0", "subscribe probe #1", "retire #1", "subscribe full #2"]);
    }),
  ));

test("with two dependencies, the consumer waits for both, and either one opening stops it", () =>
  run(
    Effect.gen(function* () {
      const w = yield* world([open, open]);
      w.registrations[0] = closed;
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, [], "one breaker registered is not enough");
      w.registrations[1] = closed;
      yield* w.gate.reconcile;
      w.registrations[1] = open;
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, ["subscribe full #0", "retire #0"]);
      assert.equal(yield* w.gate.mode, "none");
    }),
  ));

test("one dependency half-open while the other is closed: the consumer probes, one message at a time", () =>
  run(
    Effect.gen(function* () {
      const w = yield* world([closed, halfOpen]);
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, ["subscribe probe #0"]);
      w.registrations[1] = closed;
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, ["subscribe probe #0", "retire #0", "subscribe full #1"]);
    }),
  ));

test("reconciling without a change touches nothing", () =>
  run(
    Effect.gen(function* () {
      const w = yield* world([closed]);
      yield* w.gate.reconcile;
      yield* w.gate.reconcile;
      yield* w.gate.reconcile;
      assert.deepEqual(w.log, ["subscribe full #0"]);
    }),
  ));

test("a Gate sees only its own dependencies: another's breaker opening leaves it consuming", () =>
  run(
    Effect.gen(function* () {
      const payments = yield* world([closed, closed]);
      const refunds = yield* world([closed]);
      yield* payments.gate.reconcile;
      yield* refunds.gate.reconcile;
      // The third party opens: only payments lists it.
      payments.registrations[0] = open;
      yield* payments.gate.reconcile;
      yield* refunds.gate.reconcile;
      assert.equal(yield* payments.gate.mode, "none");
      assert.equal(yield* refunds.gate.mode, "full");
      assert.deepEqual(refunds.log, ["subscribe full #0"]);
    }),
  ));
