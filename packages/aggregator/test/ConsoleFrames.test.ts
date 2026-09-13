import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Fiber, Ref, type Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Sse } from "effect/unstable/encoding";
import * as ConsoleFrames from "../src/ConsoleFrames.ts";

/**
 * The properties that made the console's frame safe to share, each of which
 * the per-request version had for free and a shared one has to earn: nothing
 * is built for nobody, one build serves every reader, a slow reader skips
 * rather than queues, and a closed tab stops the work it started.
 */

const EVERY = 400;
const text = new TextDecoder();

/** Which build a frame came from — the build below returns its own count. */
const buildOf = (bytes: Uint8Array): number => Number(/^data: (\d+)$/m.exec(text.decode(bytes))?.[1]);

const run = <A>(body: Effect.Effect<A, never, Scope.Scope | TestClock.TestClock>) =>
  Effect.runPromise(Effect.scoped(body).pipe(Effect.provide(TestClock.layer())));

const counting = Effect.gen(function* () {
  const built = yield* Ref.make(0);
  const frames = yield* ConsoleFrames.make(Ref.updateAndGet(built, (n) => n + 1), EVERY);
  return { built, frames };
});

/** Let forked readers run. TestClock moves time; it does not schedule fibers. */
const settle = Effect.forEach(Array.from({ length: 25 }), () => Effect.yieldNow, { discard: true });

/** Advance one interval at a time, letting readers run between them — the
 *  order a real clock would give, rather than ten publishes in one instant. */
const intervals = (n: number) =>
  Effect.forEach(Array.from({ length: n }), () => Effect.andThen(TestClock.adjust(EVERY), settle), {
    discard: true,
  });

/** Bounded, so a count that never arrives fails the assertion that reads it
 *  instead of hanging the suite — which is what a missing finalizer did. */
const untilWatching = (frames: ConsoleFrames.ConsoleFrames, n: number, tries = 1000): Effect.Effect<void> =>
  Effect.flatMap(frames.watching, (now) =>
    now === n || tries === 0 ? Effect.void : Effect.andThen(Effect.yieldNow, untilWatching(frames, n, tries - 1)),
  );

/** A reader that records the build number of every frame it receives. */
const reader = (frames: ConsoleFrames.ConsoleFrames) =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<number>>([]);
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(frames.frames, (bytes) => Ref.update(seen, (xs) => [...xs, buildOf(bytes)])),
    );
    return { seen, fiber };
  });

test("nothing is built while nobody is watching", async () => {
  const built = await run(
    Effect.gen(function* () {
      const { built } = yield* counting;
      yield* intervals(20);
      return yield* Ref.get(built);
    }),
  );
  assert.equal(built, 0, "a process with no console open does no console work");
});

test("one build per interval serves every reader, however many there are", async () => {
  const { built, seen } = await run(
    Effect.gen(function* () {
      const { built, frames } = yield* counting;
      const readers = yield* Effect.forEach(Array.from({ length: 5 }), () => reader(frames));
      yield* untilWatching(frames, 5);
      yield* intervals(10);
      return {
        built: yield* Ref.get(built),
        seen: yield* Effect.forEach(readers, (r) => Ref.get(r.seen)),
      };
    }),
  );
  assert.equal(built, 10, "ten intervals, ten builds — not fifty");
  for (const frames of seen) {
    assert.deepEqual(frames, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "every reader receives every shared frame");
  }
});

test("a reader that falls behind skips to the newest frame instead of queueing", async () => {
  const seen = await run(
    Effect.gen(function* () {
      const { frames } = yield* counting;
      const seen = yield* Ref.make<ReadonlyArray<number>>([]);
      // Takes five intervals to write each frame — a browser on a link slower
      // than the frames arrive.
      yield* Effect.forkChild(
        Stream.runForEach(frames.frames, (bytes) =>
          Effect.andThen(Ref.update(seen, (xs) => [...xs, buildOf(bytes)]), Effect.sleep(EVERY * 5)),
        ),
      );
      yield* untilWatching(frames, 1);
      yield* intervals(21);
      return yield* Ref.get(seen);
    }),
  );
  assert.ok(seen.length <= 6, `21 frames built, a slow reader handed ${seen.length} of them: ${seen}`);
  assert.ok(
    seen.every((n, i) => i === 0 || n - seen[i - 1]! >= 4),
    `each frame it gets is the newest, not the next in a backlog: ${seen}`,
  );
});

test("a closed connection stops the work it started", async () => {
  const { whileOpen, afterClose, watching } = await run(
    Effect.gen(function* () {
      const { built, frames } = yield* counting;
      const r = yield* reader(frames);
      yield* untilWatching(frames, 1);
      yield* intervals(3);
      const whileOpen = yield* Ref.get(built);
      yield* Fiber.interrupt(r.fiber);
      yield* untilWatching(frames, 0);
      yield* intervals(10);
      return { whileOpen, afterClose: yield* Ref.get(built), watching: yield* frames.watching };
    }),
  );
  assert.equal(whileOpen, 3);
  assert.equal(watching, 0, "the tab closing brought the count back down");
  assert.equal(afterClose, whileOpen, "and no frame was built for it afterwards");
});

test("the keep-alive is invisible to the decoder subscribers use", async () => {
  const events = await Effect.runPromise(
    Stream.make(
      ConsoleFrames.KEEP_ALIVE,
      ConsoleFrames.encodeEvent("cloudevent", { sequence: 7 }),
      ConsoleFrames.KEEP_ALIVE,
    ).pipe(Stream.pipeThroughChannel(Sse.decode()), Stream.runCollect),
  );
  assert.deepEqual(
    [...events].map((e) => [e.event, e.data]),
    [["cloudevent", '{"sequence":7}']],
    "a comment is not an event, and does not split or swallow the one after it",
  );
});
