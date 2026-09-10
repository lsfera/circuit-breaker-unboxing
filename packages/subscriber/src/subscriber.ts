import { Config, Effect, Ref, Result, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { Sse } from "effect/unstable/encoding";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { decodeCircuitEvent, State } from "@egress/domain/Model.ts";
import type { State as StateType } from "@egress/domain/Model.ts";

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
      Stream.mapEffect((frame) =>
        Effect.suspend(() => {
          // Through the contract's own reader: a frame that is not JSON at all is
          // a failure to report, not an exception to escape into the stream.
          const decoded = decodeCircuitEvent(frame.data);
          if (Result.isFailure(decoded)) {
            // Loud, not silently dropped: this is contract drift.
            return Effect.logError(`undecodable event: ${frame.data}`).pipe(
              Effect.as(undefined),
            );
          }
          const event = decoded.success;
          return Ref.modify(known, (map) => {
              const { apiId, sequence, state } = event.data;
              const current = map.get(apiId);

              // Idempotent: an already-applied sequence is a no-op.
              if (current && sequence <= current.sequence) {
                return ["sync", map] as const;
              }
              const gapped =
                current !== undefined &&
                event.type === "egress.circuit.state_changed" &&
                sequence > current.sequence + 1;

            const next = new Map(map);
            next.set(apiId, { state, sequence });
            return [gapped ? "GAP!" : "    ", next] as const;
          }).pipe(Effect.map((tag) => ({ tag, event })));
        }),
      ),
      Stream.runForEach((result) =>
        result === undefined
          ? Effect.void
          : Effect.sync(() => {
              const d = result.event.data;
              console.log(
                `${result.tag} ${d.apiId.padEnd(18)} seq=${String(d.sequence).padEnd(3)} ` +
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
    aggregator: Flag.string("aggregator").pipe(
      Flag.withFallbackConfig(Config.nonEmptyString("AGGREGATOR")),
      Flag.withDefault("http://127.0.0.1:8088"),
      Flag.withDescription("Aggregator to subscribe to; only the leader publishes"),
    ),
  },
  ({ aggregator }) => program(aggregator),
);

Command.run(subscriber, { version: "0.1.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
