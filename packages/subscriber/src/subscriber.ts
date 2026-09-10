import { Config, Effect, Option as O, Ref, Result, Stream } from "effect";
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
 * A downstream consumer (`npm run subscribe`), demonstrating the three properties
 * the event contract is built for: it syncs from a single message, since every
 * event carries full state; it detects loss, since per-API sequences are gapless;
 * and it is idempotent, which is what lets delivery be at-least-once.
 *
 * Decoded through the same Schema the aggregator publishes with, so contract
 * drift fails here instead of corrupting state silently.
 */


type Known = { readonly state: StateType; readonly sequence: number };

/** One line of output: the event, and how it read against what was already known. */
type Row = { readonly tag: string; readonly event: CircuitEvent };

const program = (ORIGIN: string) =>
  Effect.gen(function* () {
    const known = yield* Ref.make(new Map<string, Known>());

    const res = yield* Effect.promise(() => fetch(`${ORIGIN}/api/stream`));
    if (!res.body) return yield* Effect.die(`cannot reach aggregator at ${ORIGIN}`);
    yield* Effect.log(`subscribed to ${ORIGIN}`);

    /**
     * `Sse.decode` rather than splitting on a blank line, which is what this did
     * and which only ever worked against this server: SSE separates on `\r\n\r\n`
     * and `\r\r` too, `data:` may span lines or omit the space, and a stream that
     * never separates grew the buffer without bound. A subscriber this repo
     * offers as the shape a real one should take had a parser that fit exactly
     * one publisher.
     */
    yield* Stream.fromReadableStream({
      evaluate: () => res.body!,
      onError: (cause) => new Error(String(cause)),
    }).pipe(
      Stream.decodeText(),
      Stream.pipeThroughChannel(Sse.decode()),
      Stream.filter((frame) => frame.event === "cloudevent"),
      // `filterMapEffect`, so a frame this cannot use contributes nothing —
      // rather than an `undefined` travelling down the stream for whoever
      // consumes it to remember to check for. `Result.fail` is this
      // combinator's "skip". See docs/decisions/006-representing-absence.md.
      Stream.filterMapEffect((frame) =>
        Effect.suspend((): Effect.Effect<Result.Result<Row, Sse.Event>> => {
          // Through the contract's own reader: a frame that is not JSON at all is
          // a failure to report, not an exception to escape into the stream.
          const decoded = decodeCircuitEvent(frame.data);
          if (Result.isFailure(decoded)) {
            // Loud, not silently dropped: this is contract drift.
            return Effect.as(
              Effect.logError(`undecodable event: ${frame.data}`),
              Result.fail(frame),
            );
          }
          const event = decoded.success;
          return Ref.modify(known, (map) => {
            const { apiId, sequence, state } = event.data;
            const highest = O.map(O.fromUndefinedOr(map.get(apiId)), (k) => k.sequence);

            // The same rule the aggregator's own observer and the daemon fleet
            // apply, from a third vantage point. Reimplementing it here — in the
            // subscriber this repo offers as the shape a real one should take —
            // is how three readings of one guarantee end up disagreeing.
            switch (classifySequence(highest, sequence)) {
              // Idempotent: an already-applied sequence is a no-op.
              case "duplicate":
                return ["sync", map] as const;
              case "gap":
                // Snapshots deliberately republish ahead of the last
                // state_changed, so only that type can reveal a real gap.
                return [
                  event.type === SEQUENCED_EVENT ? "GAP!" : "    ",
                  new Map(map).set(apiId, { state, sequence }),
                ] as const;
              case "first":
              case "next":
                return ["    ", new Map(map).set(apiId, { state, sequence })] as const;
            }
          }).pipe(Effect.map((tag) => Result.succeed({ tag, event })));
        }),
      ),
      Stream.runForEach(({ tag, event }) =>
        Effect.sync(() => {
          const d = event.data;
          console.log(
            `${tag} ${d.apiId.padEnd(18)} seq=${String(d.sequence).padEnd(3)} ` +
              `${d.state.padEnd(10)} ${d.reason.toLowerCase().replace(/_/g, " ")}` +
              (d.state === State.OPEN ? "   << stop calling this API" : ""),
          );
        }),
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
