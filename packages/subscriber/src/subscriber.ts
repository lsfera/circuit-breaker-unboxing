import { Config, Console, Data, Duration, Effect, Match, Option as O, Ref, Result, Schedule, Stream } from "effect";
import { Command, Flag } from "effect/cli";
import { Sse } from "effect/encoding";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { VERSION } from "@egress/config/Settings.ts";
import { classifyEvent, decodeCircuitEvent, SEQUENCED_EVENT, State } from "@egress/domain/Model.ts";
import type { Applied, CircuitEvent, State as StateType } from "@egress/domain/Model.ts";

/**
 * A downstream consumer: syncs from any one event, ranks events by lease then
 * sequence, is idempotent, and reconnects. Decodes with the aggregator's own schema.
 */

type Known = Applied & { readonly state: StateType };

/** One line of output: the event, and how it read against what was already known. */
type Row = { readonly tag: string; readonly event: CircuitEvent };

/** The aggregator could not be reached, refused, or ended the stream: all mean "connect again". */
class Disconnected extends Data.TaggedError("Disconnected")<{ readonly reason: string }> {}

/** Capped, not bounded: a subscriber outlives any aggregator restart. */
const RECONNECT = Schedule.min([Schedule.exponential(Duration.millis(500)), Schedule.spaced(Duration.seconds(5))]);

const program = Effect.fnUntraced(function* (ORIGIN: string) {
  // Outlives each connection: a reconnect resyncs from the next event, and what was known still ranks it.
  const known = yield* Ref.make(new Map<string, Known>());

  const classify = (event: CircuitEvent) =>
    Ref.modify(known, (map) => {
      const { apiId, sequence, state } = event.data;
      const lease = O.fromUndefinedOr(event.data.lease);
      const next = new Map(map).set(apiId, { lease, sequence, state });

      // The shared rule, from a third vantage point.
      return Match.value(classifyEvent(O.fromUndefinedOr(map.get(apiId)), { lease, sequence })).pipe(
        // Idempotent: an already-applied sequence is a no-op.
        Match.when("duplicate", () => ["sync ", map] as const),
        // A paused leader resuming: what it says is already out of date.
        Match.when("stale", () => ["stale", map] as const),
        // Snapshots deliberately republish ahead of the last state_changed,
        // so only that type can reveal a real gap.
        Match.when("gap", () => [event.type === SEQUENCED_EVENT ? "GAP! " : "     ", next] as const),
        // The coordinator lost its state and counts from 1 again: not a gap, not duplicates.
        Match.when("new-epoch", () => ["epoch", next] as const),
        Match.when(Match.is("first", "next"), () => ["     ", next] as const),
        Match.exhaustive,
      );
    });

  const session = Effect.gen(function* () {
    const res = yield* Effect.tryPromise({
      try: () => fetch(`${ORIGIN}/api/events/stream`),
      catch: (cause) => new Disconnected({ reason: `cannot reach aggregator at ${ORIGIN}: ${String(cause)}` }),
    });
    const body = yield* O.match(O.fromNullOr(res.ok ? res.body : null), {
      onNone: () => Effect.fail(new Disconnected({ reason: `aggregator at ${ORIGIN} answered ${res.status}` })),
      onSome: Effect.succeed,
    });
    yield* Effect.log(`subscribed to ${ORIGIN}`);

    // `Sse.decode`: splitting on a blank line fit exactly one publisher.
    yield* Stream.fromReadableStream({
      evaluate: () => body,
      onError: (cause) => new Disconnected({ reason: `stream broke: ${String(cause)}` }),
    }).pipe(
      Stream.decodeText(),
      Stream.pipeThroughChannel(Sse.decode()),
      Stream.mapError((e) =>
        Match.valueTags(e, {
          Disconnected: (d) => d,
          Retry: () => new Disconnected({ reason: "aggregator asked for a reconnect" }),
          SseError: (sse) => new Disconnected({ reason: `malformed stream: ${sse.message}` }),
        }),
      ),
      Stream.filter((frame) => frame.event === "cloudevent"),
      // `Result.fail` is this combinator's "skip".
      Stream.filterMapEffect((frame) =>
        // Through the contract's own reader: a frame that is not JSON at all is
        // a failure to report, not an exception to escape into the stream.
        Effect.suspend((): Effect.Effect<Result.Result<Row, Sse.Event>> =>
          Result.match(decodeCircuitEvent(frame.data), {
            // Loud, not silently dropped: this is contract drift.
            onFailure: () => Effect.as(Effect.logError(`undecodable event: ${frame.data}`), Result.fail(frame)),
            onSuccess: (event) => Effect.map(classify(event), (tag) => Result.succeed({ tag, event })),
          }),
        ),
      ),
      Stream.runForEach(({ tag, event: { data: d } }) =>
        Console.log(
          `${tag} ${d.apiId.padEnd(18)} seq=${String(d.sequence).padEnd(3)} ` +
            `${d.state.padEnd(10)} ${d.reason.toLowerCase().replace(/_/g, " ")}` +
            (d.state === State.OPEN ? "   << stop calling this API" : ""),
        ),
      ),
    );
    // The aggregator closed the stream (a restart, a leader stepping down): as good as a failure.
    return yield* new Disconnected({ reason: "aggregator ended the stream" });
  });

  return yield* session.pipe(
    Effect.tapError((e) => Effect.logWarning(`${e.reason} — reconnecting`)),
    Effect.retry(RECONNECT),
  );
});

const subscriber = Command.make(
  "subscriber",
  {
    aggregator: Flag.String("aggregator").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("AGGREGATOR")),
      Flag.withDefault("http://127.0.0.1:8088"),
      Flag.withDescription("Aggregator to subscribe to; only the leader publishes"),
    ),
  },
  ({ aggregator }) => program(aggregator),
);

Command.run(subscriber, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
