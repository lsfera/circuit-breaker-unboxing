import { Config, Console, Effect, Match, Option as O, Ref, Result, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { Sse } from "effect/unstable/encoding";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { VERSION } from "@egress/config/Settings.ts";
import {
  classifySequence,
  decodeCircuitEvent,
  SEQUENCED_EVENT,
  State,
} from "@egress/domain/Model.ts";
import type { CircuitEvent, State as StateType } from "@egress/domain/Model.ts";

/**
 * A downstream consumer: syncs from any one event, detects loss by sequence,
 * and is idempotent. Decodes with the aggregator's own schema.
 */


type Known = { readonly state: StateType; readonly sequence: number };

/** One line of output: the event, and how it read against what was already known. */
type Row = { readonly tag: string; readonly event: CircuitEvent };

const program = Effect.fnUntraced(function* (ORIGIN: string) {
  const known = yield* Ref.make(new Map<string, Known>());

  const res = yield* Effect.promise(() => fetch(`${ORIGIN}/api/events/stream`));
  if (!res.body) return yield* Effect.die(`cannot reach aggregator at ${ORIGIN}`);
  yield* Effect.log(`subscribed to ${ORIGIN}`);

  // `Sse.decode`: splitting on a blank line fit exactly one publisher.
  yield* Stream.fromReadableStream({
    evaluate: () => res.body!,
    onError: (cause) => new Error(String(cause)),
  }).pipe(
    Stream.decodeText(),
    Stream.pipeThroughChannel(Sse.decode()),
    Stream.filter((frame) => frame.event === "cloudevent"),
    // `Result.fail` is this combinator's "skip".
    Stream.filterMapEffect((frame) =>
      // Through the contract's own reader: a frame that is not JSON at all is
      // a failure to report, not an exception to escape into the stream.
      Effect.suspend((): Effect.Effect<Result.Result<Row, Sse.Event>> =>
        Result.match(decodeCircuitEvent(frame.data), {
          // Loud, not silently dropped: this is contract drift.
          onFailure: () =>
            Effect.as(
              Effect.logError(`undecodable event: ${frame.data}`),
              Result.fail(frame),
            ),
          onSuccess: (event) =>
            Ref.modify(known, (map) => {
              const { apiId, sequence, state } = event.data;
              const highest = O.map(O.fromUndefinedOr(map.get(apiId)), (k) => k.sequence);

              // The shared rule, from a third vantage point.
              return Match.value(classifySequence(highest, sequence)).pipe(
                // Idempotent: an already-applied sequence is a no-op.
                Match.when("duplicate", () => ["sync", map] as const),
                // Snapshots deliberately republish ahead of the last
                // state_changed, so only that type can reveal a real gap.
                Match.when(
                  "gap",
                  () =>
                    [
                      event.type === SEQUENCED_EVENT ? "GAP!" : "    ",
                      new Map(map).set(apiId, { state, sequence }),
                    ] as const,
                ),
                Match.when(
                  Match.is("first", "next"),
                  () => ["    ", new Map(map).set(apiId, { state, sequence })] as const,
                ),
                Match.exhaustive,
              );
            }).pipe(Effect.map((tag) => Result.succeed({ tag, event }))),
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
