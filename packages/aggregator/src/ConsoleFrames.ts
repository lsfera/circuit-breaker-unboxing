import { type Duration, Effect, Metric, Option as O, PubSub, Ref, Schedule, type Scope, Stream } from "effect";
import { Sse } from "effect/unstable/encoding";
import { STATE_CODE, State, type ApiSnapshot } from "@egress/domain/Model.ts";
import * as Telemetry from "./Telemetry.ts";

/**
 * One SSE event as text. `Sse.encoder` rather than a template string, so the
 * wire format has one definition: @egress/subscriber decodes with the same
 * module, and an encoder and a parser that merely happen to agree are two
 * definitions waiting to drift.
 *
 * `id` carries a revision for the events the attention view publishes
 * (below), so a reconnecting browser's `Last-Event-ID` names a point in this
 * same sequence. Omitted, as before, for the full-frame channel and the tape.
 */
export const encodeEvent = (event: string, data: unknown, id?: string): string =>
  Sse.encoder.write({ _tag: "Event", event, id, data: JSON.stringify(data) });

/**
 * An SSE comment. Every spec-conforming decoder ignores it, `Sse.decode`
 * included, and it keeps a stream that has nothing to say from looking idle to
 * a load balancer — which closes idle connections, typically after a minute,
 * and a subscriber whose connection closed in a quiet hour is one that misses
 * the event that ends it.
 */
export const KEEP_ALIVE = ": keep-alive\n\n";

export interface ConsoleFrames {
  /**
   * The console's state frames, from the moment of subscribing, already
   * encoded. A subscriber that falls behind skips to the newest frame rather
   * than queueing the ones it missed.
   */
  readonly frames: Stream.Stream<Uint8Array>;
  /** How many subscribers are reading `frames` right now. */
  readonly watching: Effect.Effect<number>;
}

/**
 * Builds the console's frame once per interval, for every connection at once.
 *
 * It used to be built inside each request — `Stream.fromEffectSchedule` per
 * connection — so every open browser snapshotted the whole fleet and
 * serialized it every 400ms, on the event loop that runs the breaker. At a
 * thousand APIs that is a 1.1 MB frame per browser per tick: a hundred open
 * consoles took 16–22% of the control loop's cadence and 0.6% of a core each
 * (docs/decisions/015-the-console-at-a-thousand-apis.md). Now the frame is
 * built and encoded once, and what each connection costs is writing bytes it
 * did not compute.
 *
 * Two properties are the reason for the particular pieces:
 *
 * - **Nothing is built while nobody is watching.** A process with no console
 *   open does no console work at all, which the per-request schedule got for
 *   free and a shared one has to count connections to keep.
 * - **A slow reader holds one frame, not a queue of them.** `PubSub.sliding`
 *   with a capacity of one: a newer frame replaces an unread older one. Not
 *   `SubscriptionRef`, whose `changes` is backed by an *unbounded* PubSub — a
 *   browser on a slow link, pulling 1.1 MB frames slower than they arrive,
 *   would have queued them in this process without limit. The HTTP response
 *   already waits on `drain`, so between the write in flight and the one slot,
 *   a reader that stops reading costs two frames and then costs nothing.
 *
 * No replay, deliberately. A replayed frame would be the last one built, and
 * the last one built could be from whenever a console was last open — an hour
 * ago, showing an incident that has since ended. A newly connected console
 * waits at most one interval for a frame that is current instead.
 */
export const make = Effect.fnUntraced(function* <A>(
  build: Effect.Effect<A>,
  every: Duration.Input,
): Effect.fn.Return<ConsoleFrames, never, Scope.Scope> {
  const watching = yield* Ref.make(0);
  const pubsub = yield* PubSub.sliding<Uint8Array>(1);
  const text = new TextEncoder();

  const publish = Effect.gen(function* () {
    const frame = yield* build;
    yield* PubSub.publish(pubsub, text.encode(encodeEvent("state", frame)));
    yield* Metric.update(Telemetry.consoleFramesBuilt, 1);
  });

  yield* Effect.forkScoped(
    Effect.repeat(
      Effect.when(publish, Effect.map(Ref.get(watching), (n) => n > 0)),
      Schedule.spaced(every),
    ),
  );

  const track = (delta: number) =>
    Effect.flatMap(Ref.updateAndGet(watching, (n) => n + delta), (n) =>
      Metric.update(Telemetry.consoleStreams, n),
    );

  return {
    // `ensuring` runs when the stream is interrupted, which is how a browser
    // closing its tab reaches this: the response stream ends with the
    // request's scope, and the count has to come down with it or the frame
    // would go on being built for a console nobody has open.
    frames: Stream.fromPubSub(pubsub).pipe(Stream.onStart(track(1)), Stream.ensuring(track(-1))),
    watching: Ref.get(watching),
  };
});

/**
 * ---------------------------------------------------------------------------
 * The attention view (docs/decisions/015, steps 3–4): counts by state plus
 * the worst APIs, sent as a snapshot on connect and patches afterwards, with
 * resume through the SSE `Last-Event-ID` header.
 * ---------------------------------------------------------------------------
 */

/** Everything not `CLOSED`, worst state first then most recently changed,
 *  capped at fifty (docs/decisions/015, step 3). Bounded by the cap rather
 *  than the fleet, so it costs the same at ten thousand APIs. */
export const ATTENTION_CAP = 50;

export type Counts = Record<State, number>;

const emptyCounts = (): Counts => ({ CLOSED: 0, DEGRADED: 0, OPEN: 0, HALF_OPEN: 0 });

/** All four states, for iterating `Counts` without hard-coding the list a
 *  second time. */
const STATES: ReadonlyArray<State> = Object.values(State);

export interface Truncated {
  readonly total: number;
  readonly shown: number;
}

const truncatedEqual = (a: Truncated | null, b: Truncated | null): boolean =>
  a === b || (a !== null && b !== null && a.total === b.total && a.shown === b.shown);

export interface Attention {
  readonly counts: Counts;
  /** Worst state first, then most recently changed; at most `ATTENTION_CAP`. */
  readonly apis: ReadonlyArray<ApiSnapshot>;
  /** `null` when nothing needing attention was cut by the cap. */
  readonly truncated: Truncated | null;
}

/**
 * The view a person can use at a thousand APIs, computed from the fleet's
 * full snapshots: a number for the APIs that are fine, and a capped,
 * ranked list for the ones that are not.
 */
export const attentionOf = (apis: ReadonlyArray<ApiSnapshot>, cap = ATTENTION_CAP): Attention => {
  const counts = apis.reduce((acc, api) => ({ ...acc, [api.state]: acc[api.state] + 1 }), emptyCounts());
  const worst = apis
    .filter((api) => api.state !== State.CLOSED)
    .sort((a, b) => STATE_CODE[b.state] - STATE_CODE[a.state] || b.changedAt - a.changedAt);
  return {
    counts,
    apis: worst.slice(0, cap),
    truncated: worst.length > cap ? { total: worst.length, shown: cap } : null,
  };
};

/** One tick's worth of change: whole API objects that entered the view or
 *  changed, and the ids of ones that left it — closed, or pushed out by the
 *  cap. Whole objects rather than changed fields: a client merging partial
 *  objects into state it already holds is where this kind of code grows its
 *  bugs (docs/decisions/015, step 4). */
interface Patch {
  readonly revision: number;
  readonly upserts: ReadonlyArray<ApiSnapshot>;
  readonly removed: ReadonlyArray<string>;
  readonly counts: Counts;
  readonly truncated: Truncated | null;
}

/** A patch as published: the revision, for deciding what a resuming
 *  connection still needs, alongside the bytes built for it once. */
interface PatchEntry {
  readonly revision: number;
  readonly bytes: Uint8Array;
}

/** What every connection reads to build a fresh `snapshot`: the last tick's
 *  view, its revision, and the per-id index that lets the next tick's diff
 *  find what changed without re-serializing the whole view to compare it. */
interface Current {
  readonly revision: number;
  readonly index: ReadonlyMap<string, string>;
  readonly counts: Counts;
  readonly truncated: Truncated | null;
  readonly encodedSnapshot: Uint8Array;
}

/** Patches retained for resume — docs/decisions/015 step 4's "last 64
 *  revisions". Past this, a reconnecting browser gets a fresh `snapshot`
 *  instead of the patches it missed. Exported so a test can drive past it
 *  without hard-coding the number. */
export const RETAINED_PATCHES = 64;

export interface AttentionFrames {
  /**
   * One connection's byte stream: a `snapshot` (revision and all) when
   * `lastEventId` is absent or older than the retained history, otherwise
   * just the patches from there forward — then the live patch tail either
   * way. Quiet ticks publish nothing, so a quiet fleet costs nothing past
   * the one snapshot or catch-up.
   */
  readonly connect: (lastEventId: O.Option<number>) => Stream.Stream<Uint8Array>;
  readonly watching: Effect.Effect<number>;
}

/**
 * Builds the attention view once per interval, the same shape as `make`:
 * nothing built while nobody is watching, one build serving every
 * connection. What differs is what gets sent — a `snapshot` or `patch`
 * event with a revision, not the whole frame every tick — and that a
 * connection can ask, via `connect`'s `lastEventId`, to resume rather than
 * start over.
 *
 * Correctness of resume rests on `PubSub`'s own replay buffer rather than a
 * hand-rolled one: `PubSub.sliding({ capacity, replay })` hands a new
 * subscriber its own `replayWindow`, populated atomically with whatever was
 * retained at subscribe time and then drained into the same ordered stream
 * as everything published afterwards (repos/effect/packages/effect/test/PubSub.test.ts,
 * "preserves replay order across multiple slides"). A connection reads that
 * window directly — `subscription.replayWindow.takeAll()` is exactly what
 * `PubSub.take` does internally — decides snapshot-or-resume from it, and
 * then keeps reading the same subscription for the live tail. There is no
 * gap between "what was buffered" and "what arrives next": both come from
 * one subscription.
 */
export const makeAttention = Effect.fnUntraced(function* (
  snapshots: Effect.Effect<ReadonlyArray<ApiSnapshot>>,
  every: Duration.Input,
): Effect.fn.Return<AttentionFrames, never, Scope.Scope> {
  const watching = yield* Ref.make(0);
  const patches = yield* PubSub.sliding<PatchEntry>({ capacity: RETAINED_PATCHES, replay: RETAINED_PATCHES });
  const text = new TextEncoder();

  const emptyAttention = attentionOf([]);
  const current = yield* Ref.make<Current>({
    revision: 0,
    index: new Map(),
    counts: emptyAttention.counts,
    truncated: null,
    encodedSnapshot: text.encode(
      encodeEvent("snapshot", { revision: 0, counts: emptyAttention.counts, apis: [], truncated: null }, "0"),
    ),
  });

  const publish = Effect.gen(function* () {
    const apis = yield* snapshots;
    const view = attentionOf(apis);
    const nextIndex = new Map(view.apis.map((api) => [api.apiId, JSON.stringify(api)] as const));
    const prev = yield* Ref.get(current);

    const upserts = view.apis.filter((api) => nextIndex.get(api.apiId) !== prev.index.get(api.apiId));
    const removed = [...prev.index.keys()].filter((id) => !nextIndex.has(id));
    const changed =
      upserts.length > 0 ||
      removed.length > 0 ||
      STATES.some((s) => prev.counts[s] !== view.counts[s]) ||
      !truncatedEqual(prev.truncated, view.truncated) ||
      // The first tick ever always counts as a change: going from "nothing
      // computed yet" to the fleet's real state is one, even for a fleet
      // that happens to be all CLOSED.
      prev.revision === 0;

    const revision = changed ? prev.revision + 1 : prev.revision;
    yield* Ref.set(current, {
      revision,
      index: nextIndex,
      counts: view.counts,
      truncated: view.truncated,
      encodedSnapshot: text.encode(
        encodeEvent(
          "snapshot",
          { revision, counts: view.counts, apis: view.apis, truncated: view.truncated },
          String(revision),
        ),
      ),
    });

    if (changed) {
      const patch: Patch = { revision, upserts, removed, counts: view.counts, truncated: view.truncated };
      const bytes = text.encode(encodeEvent("patch", patch, String(revision)));
      yield* PubSub.publish(patches, { revision, bytes });
      yield* Metric.update(Telemetry.consoleAttentionBuilt, 1);
    }
  });

  yield* Effect.forkScoped(
    Effect.repeat(
      Effect.when(publish, Effect.map(Ref.get(watching), (n) => n > 0)),
      Schedule.spaced(every),
    ),
  );

  const track = (delta: number) =>
    Effect.flatMap(Ref.updateAndGet(watching, (n) => n + delta), (n) =>
      Metric.update(Telemetry.consoleAttentionStreams, n),
    );

  const liveTail = (subscription: PubSub.Subscription<PatchEntry>) =>
    Stream.fromSubscription(subscription).pipe(Stream.map((entry) => entry.bytes));

  const connect = (lastEventId: O.Option<number>): Stream.Stream<Uint8Array> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(patches);
        // What `PubSub.take` would drain first anyway, read directly so the
        // decision below can see it before committing to a snapshot or not.
        const buffered = subscription.replayWindow.takeAll();
        const oldestRetained = buffered.at(0)?.revision;
        const snap = yield* Ref.get(current);

        const resumeFrom = O.filter(
          lastEventId,
          (after) =>
            // No gap: every revision after `after` that was ever published is
            // still in `buffered`. If the oldest retained one is already past
            // `after + 1`, something in between was evicted.
            (oldestRetained === undefined || oldestRetained <= after + 1) &&
            // Not from the future either — a revision this process never
            // published, most often because it restarted and its revisions
            // reset. Treating that as "already caught up" would leave the
            // client waiting on a live tail it can never be caught up on.
            after <= snap.revision,
        );

        const prefix: Stream.Stream<Uint8Array> = O.match(resumeFrom, {
          onSome: (after) =>
            Stream.fromIterable(buffered.filter((p) => p.revision > after).map((p) => p.bytes)),
          onNone: () =>
            Stream.concat(
              Stream.succeed(snap.encodedSnapshot),
              Stream.fromIterable(buffered.filter((p) => p.revision > snap.revision).map((p) => p.bytes)),
            ),
        });

        return Stream.concat(prefix, liveTail(subscription));
      }),
    ).pipe(Stream.onStart(track(1)), Stream.ensuring(track(-1)));

  return { connect, watching: Ref.get(watching) };
});
