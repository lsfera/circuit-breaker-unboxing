import { type Duration, Effect, Metric, PubSub, Ref, Schedule, type Scope, Stream } from "effect";
import { Sse } from "effect/unstable/encoding";
import * as Telemetry from "./Telemetry.ts";

/**
 * One SSE event as text. `Sse.encoder` rather than a template string, so the
 * wire format has one definition: @egress/subscriber decodes with the same
 * module, and an encoder and a parser that merely happen to agree are two
 * definitions waiting to drift.
 */
export const encodeEvent = (event: string, data: unknown): string =>
  Sse.encoder.write({ _tag: "Event", event, id: undefined, data: JSON.stringify(data) });

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
export const make = <A>(
  build: Effect.Effect<A>,
  every: Duration.Input,
): Effect.Effect<ConsoleFrames, never, Scope.Scope> =>
  Effect.gen(function* () {
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
