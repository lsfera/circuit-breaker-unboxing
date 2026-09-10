import { Effect, Ref, Result, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
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

const ORIGIN = process.env["AGGREGATOR"] ?? "http://127.0.0.1:8088";


type Known = { readonly state: StateType; readonly sequence: number };

/** Split an SSE byte stream into complete frames. */
const frames = (body: ReadableStream<Uint8Array>) =>
  Stream.fromAsyncIterable(
    (async function* () {
      const reader = body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() ?? "";
        for (const part of parts) yield part;
      }
    })(),
    (cause) => new Error(String(cause)),
  );

const program = Effect.gen(function* () {
  const known = yield* Ref.make(new Map<string, Known>());

  const res = yield* Effect.promise(() => fetch(`${ORIGIN}/api/stream`));
  if (!res.body) return yield* Effect.die(`cannot reach aggregator at ${ORIGIN}`);
  yield* Effect.log(`subscribed to ${ORIGIN}`);

  yield* frames(res.body).pipe(
    Stream.filter((f) => f.includes("event: cloudevent")),
    Stream.map((f) => f.split("\n").find((l) => l.startsWith("data: "))),
    Stream.filter((l): l is string => l !== undefined),
    Stream.mapEffect((line) =>
      Effect.suspend(() => {
        // Through the contract's own reader: a frame that is not JSON at all is
        // a failure to report, not an exception to escape into the stream.
        const decoded = decodeCircuitEvent(line.slice(6));
        if (Result.isFailure(decoded)) {
          // Loud, not silently dropped: this is contract drift.
          return Effect.logError(`undecodable event: ${line}`).pipe(
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

NodeRuntime.runMain(program);
