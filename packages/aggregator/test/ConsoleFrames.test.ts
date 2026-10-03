import { test } from "node:test";
import assert from "node:assert/strict";
import { Effect, Fiber, Option as O, Ref, type Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Sse } from "effect/encoding";
import { Reason, State, type ApiSnapshot } from "@egress/domain/Model.ts";
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
 *  instead of hanging the suite — which is what a missing finalizer did.
 *  Structural on `watching` so it works for both `ConsoleFrames` and
 *  `AttentionFrames`. */
const untilWatching = (
  frames: { readonly watching: Effect.Effect<number> },
  n: number,
  tries = 1000,
): Effect.Effect<void> =>
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

/**
 * The attention view (docs/decisions/015, steps 3–4): counts plus the worst
 * fifty APIs instead of the whole fleet, sent as a snapshot on connect and
 * patches afterwards, resumable through `Last-Event-ID`.
 */

let nextApiId = 0;
const api = (overrides: Partial<ApiSnapshot> = {}): ApiSnapshot => ({
  apiId: `api-${nextApiId++}`,
  state: State.CLOSED,
  reason: Reason.HEALTHY,
  sequence: 0,
  healthyEndpoints: 2,
  totalEndpoints: 2,
  reportingReplicas: 1,
  votes: { OK: 1, DEGRADED: 0, DOWN: 0 },
  observedSince: 0,
  changedAt: 0,
  replicas: [],
  ...overrides,
});

/** One `PatchEntry`'s bytes, decoded back into `{event, id, data}`. Each
 *  entry `connect` emits is exactly one whole encoded SSE event, so a plain
 *  field-per-line parse is enough — no need for `Sse.decode`'s streaming
 *  parser, which the keep-alive test above already covers. */
const parseEvent = (bytes: Uint8Array): { event: string; id: string | undefined; data: any } => {
  const raw = text.decode(bytes);
  return {
    event: /^event: (.+)$/m.exec(raw)![1]!,
    id: /^id: (.+)$/m.exec(raw)?.[1],
    data: JSON.parse(/^data: (.+)$/m.exec(raw)![1]!),
  };
};

test("attentionOf: excludes CLOSED, worst state first then most recently changed", () => {
  const apis = [
    api({ apiId: "closed", state: State.CLOSED }),
    api({ apiId: "degraded-old", state: State.DEGRADED, changedAt: 100 }),
    api({ apiId: "degraded-new", state: State.DEGRADED, changedAt: 200 }),
    api({ apiId: "open", state: State.OPEN, changedAt: 50 }),
    api({ apiId: "half-open", state: State.HALF_OPEN, changedAt: 10 }),
  ];
  const view = ConsoleFrames.attentionOf(apis);
  assert.deepEqual(
    view.apis.map((a) => a.apiId),
    ["half-open", "open", "degraded-new", "degraded-old"],
    "worst STATE_CODE first, ties broken by most recently changed",
  );
  assert.equal(view.truncated, null, "nothing needing attention was cut");
  assert.deepEqual(view.counts, { CLOSED: 1, DEGRADED: 2, OPEN: 1, HALF_OPEN: 1 }, "counts cover every API, not just the ones shown");
});

test("attentionOf: caps at fifty and states the truncation rather than staying silent", () => {
  const apis = Array.from({ length: 60 }, (_, i) => api({ state: State.DEGRADED, changedAt: i }));
  const view = ConsoleFrames.attentionOf(apis);
  assert.equal(view.apis.length, ConsoleFrames.ATTENTION_CAP);
  assert.deepEqual(view.truncated, { total: 60, shown: 50 }, "106 need attention, 50 shown — this is that number");
  assert.equal(view.apis[0]!.changedAt, 59, "the cap keeps the most recently changed, not an arbitrary fifty");
});

test("attentionOf: bounded by the cap rather than the fleet", () => {
  // docs/decisions/015's own claim for this view: it costs the same at ten
  // thousand APIs. Sanity-check the server-side payload size this repo can
  // actually measure — not rendering cost, which needs a browser this
  // environment does not have.
  const fleet = Array.from({ length: 1000 }, (_, i) =>
    api({ state: i % 4 === 0 ? State.DEGRADED : State.CLOSED, changedAt: i }),
  );
  const full = JSON.stringify(fleet);
  const view = ConsoleFrames.attentionOf(fleet);
  const attention = JSON.stringify(view.apis);
  assert.ok(view.apis.length <= ConsoleFrames.ATTENTION_CAP);
  assert.ok(
    attention.length < full.length / 10,
    `attention payload (${attention.length}B) should be a small fraction of the full frame (${full.length}B)`,
  );
});

test("attention: a fresh connection gets a snapshot, then a patch once the fleet is known", async () => {
  const flaky = api({ state: State.DEGRADED, changedAt: 1 });
  const events = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([flaky]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);
      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.none()), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 1);
      yield* intervals(1);
      return yield* Ref.get(seen);
    }),
  );
  const parsed = events.map(parseEvent);
  assert.equal(parsed.length, 2);
  assert.deepEqual(
    [parsed[0]!.event, parsed[0]!.id],
    ["snapshot", "0"],
    "nothing is known yet — an honest empty snapshot, not a stale one",
  );
  assert.deepEqual(parsed[0]!.data.apis, []);
  assert.deepEqual([parsed[1]!.event, parsed[1]!.id], ["patch", "1"], "the first tick, carrying the real fleet as an upsert");
  assert.deepEqual(parsed[1]!.data.upserts.map((a: ApiSnapshot) => a.apiId), [flaky.apiId]);
});

test("attention: quiet ticks after the first publish nothing further", async () => {
  const seen = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([api({ state: State.DEGRADED, changedAt: 1 })]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);
      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.none()), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 1);
      yield* intervals(10);
      return yield* Ref.get(seen);
    }),
  );
  assert.equal(seen.length, 2, "the bootstrap snapshot and one patch for the first tick — nothing for the nine quiet ones after");
});

test("attention: an API leaving the view is reported as removed", async () => {
  const events = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([api({ apiId: "flaky", state: State.DEGRADED, changedAt: 1 })]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);
      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.none()), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 1);
      yield* intervals(1);
      yield* Ref.set(apis, [api({ apiId: "flaky", state: State.CLOSED, changedAt: 2 })]);
      yield* intervals(1);
      return yield* Ref.get(seen);
    }),
  );
  const patches = events.map(parseEvent).filter((e) => e.event === "patch");
  assert.equal(patches.length, 2);
  assert.deepEqual(patches[0]!.data.upserts.map((a: ApiSnapshot) => a.apiId), ["flaky"]);
  assert.deepEqual(patches[1]!.data, { revision: 2, upserts: [], removed: ["flaky"], counts: { CLOSED: 1, DEGRADED: 0, OPEN: 0, HALF_OPEN: 0 }, truncated: null });
});

test("attention: resuming from a retained revision replays only what was missed, no snapshot", async () => {
  const events = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([api({ apiId: "a", state: State.DEGRADED, changedAt: 1 })]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);

      // Keep the loop running with one always-draining reader.
      yield* Effect.forkChild(Stream.runDrain(attention.connect(O.none())));
      yield* untilWatching(attention, 1);
      yield* intervals(1); // revision 1: "a" upserted

      yield* Ref.update(apis, (xs) => [...xs, api({ apiId: "b", state: State.OPEN, changedAt: 2 })]);
      yield* intervals(1); // revision 2: "b" upserted

      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.some(1)), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 2);
      yield* settle;
      yield* Fiber.interrupt(fiber);
      return yield* Ref.get(seen);
    }),
  );
  const parsed = events.map(parseEvent);
  assert.equal(parsed.length, 1, "revision 1 is already known — only the missed patch is sent");
  assert.deepEqual([parsed[0]!.event, parsed[0]!.id], ["patch", "2"]);
  assert.deepEqual(parsed[0]!.data.upserts.map((a: ApiSnapshot) => a.apiId), ["b"]);
});

test("attention: resuming from an evicted revision falls back to a fresh snapshot", async () => {
  const changes = ConsoleFrames.RETAINED_PATCHES + 5;
  const events = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);
      yield* Effect.forkChild(Stream.runDrain(attention.connect(O.none())));
      yield* untilWatching(attention, 1);

      for (let i = 0; i < changes; i++) {
        yield* Ref.update(apis, (xs) => [...xs, api({ state: State.DEGRADED, changedAt: i })]);
        yield* intervals(1);
      }

      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.some(1)), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 2);
      yield* settle;
      yield* Fiber.interrupt(fiber);
      return yield* Ref.get(seen);
    }),
  );
  const parsed = events.map(parseEvent);
  assert.equal(parsed[0]!.event, "snapshot", "revision 1 was evicted by the retention window — a fresh snapshot instead");
  assert.equal(parsed[0]!.data.revision, changes, "and it is current, not stale");
  assert.equal(parsed.filter((e) => e.event === "snapshot").length, 1, "exactly one snapshot, not one per evicted revision");
});

test("attention: resuming from a revision that was never published falls back to a fresh snapshot", async () => {
  // A bogus or future Last-Event-ID — most plausibly a browser reconnecting
  // after this process restarted, so its remembered id outruns a revision
  // counter that reset to 0. Treating it as "already caught up" would leave
  // the client waiting forever on a live tail it can never actually catch
  // up from.
  const events = await run(
    Effect.gen(function* () {
      const apis = yield* Ref.make<ReadonlyArray<ApiSnapshot>>([api({ state: State.DEGRADED, changedAt: 1 })]);
      const attention = yield* ConsoleFrames.makeAttention(Ref.get(apis), EVERY);
      yield* Effect.forkChild(Stream.runDrain(attention.connect(O.none())));
      yield* untilWatching(attention, 1);
      yield* intervals(1); // revision 1 exists; nothing higher ever will yet

      const seen = yield* Ref.make<ReadonlyArray<Uint8Array>>([]);
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(attention.connect(O.some(999_999)), (b) => Ref.update(seen, (xs) => [...xs, b])),
      );
      yield* untilWatching(attention, 2);
      yield* settle;
      yield* Fiber.interrupt(fiber);
      return yield* Ref.get(seen);
    }),
  );
  const parsed = events.map(parseEvent);
  assert.equal(parsed[0]!.event, "snapshot", "a revision this process never published is not a valid resume point");
  assert.equal(parsed[0]!.data.revision, 1);
});
