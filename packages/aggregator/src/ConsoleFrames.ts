import { type Duration, Effect, Metric, Option as O, PubSub, Ref, Schedule, type Scope, Stream } from "effect";
import { Sse } from "effect/unstable/encoding";
import { STATE_CODE, State, type ApiSnapshot } from "@egress/domain/Model.ts";
import * as Telemetry from "./Telemetry.ts";

/** `Sse.encoder`, the module the subscriber decodes with. `id` is the attention view's revision. */
export const encodeEvent = (event: string, data: unknown, id?: string): string =>
  Sse.encoder.write({ _tag: "Event", event, id, data: JSON.stringify(data) });

/** Ignored by decoders; keeps a quiet stream from being closed as idle. */
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
 * The console frame, built once per interval for every connection (ADR 015).
 * Nothing is built while nobody watches. `PubSub.sliding(1)`, not
 * `SubscriptionRef`, whose unbounded `changes` would queue 1.1 MB frames for a
 * slow browser. No replay: the last frame built could be an hour old.
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
    // A closed tab interrupts the stream; the count must come down with it.
    frames: Stream.fromPubSub(pubsub).pipe(Stream.onStart(track(1)), Stream.ensuring(track(-1))),
    watching: Ref.get(watching),
  };
});

// The attention view (ADR 015): counts plus the worst APIs, snapshot then patches, resumable.

/** Everything not `CLOSED`, worst first then most recent; the cap bounds its cost at any fleet size. */
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

/** Whole API objects, not changed fields: merging partial objects is where bugs grow. */
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

/** Past this many revisions, a reconnect gets a fresh snapshot. */
export const RETAINED_PATCHES = 64;

export interface AttentionFrames {
  /** A snapshot, or the retained patches after `lastEventId`, then the live tail. */
  readonly connect: (lastEventId: O.Option<number>) => Stream.Stream<Uint8Array>;
  readonly watching: Effect.Effect<number>;
}

/**
 * Resume rests on `PubSub.sliding({ capacity, replay })`: a subscriber's
 * `replayWindow` and its live tail come from one subscription, so nothing falls
 * between them (effect's PubSub.test.ts, "preserves replay order across multiple
 * slides").
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
            // An id from the future (this process restarted) is not "caught up".
            after <= snap.revision,
        );

        // The live tail is filtered too: the tick sets `current` before it publishes the
        // patch, so a connection landing between the two reads revision N in the snapshot
        // and then gets patch N again from the tail. Each revision goes out once.
        const after = O.getOrElse(resumeFrom, () => snap.revision);
        const newer = (entry: PatchEntry) => entry.revision > after;
        const snapshot = O.match(resumeFrom, {
          onSome: () => Stream.empty,
          onNone: () => Stream.succeed(snap.encodedSnapshot),
        });

        return Stream.concat(
          snapshot,
          Stream.concat(Stream.fromIterable(buffered), Stream.fromSubscription(subscription)).pipe(
            Stream.filter(newer),
            Stream.map((entry) => entry.bytes),
          ),
        );
      }),
    ).pipe(Stream.onStart(track(1)), Stream.ensuring(track(-1)));

  return { connect, watching: Ref.get(watching) };
});
