import { Effect, Option as O, Ref, Schema, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { CircuitEvent, State } from "@egress/domain/Model.ts";
import type { State as StateType } from "@egress/domain/Model.ts";

/**
 * A downstream consumer, written the way a real one should be.
 *
 *   npm run subscribe
 *
 * It demonstrates the three properties the event contract is built for:
 *
 *  1. It syncs from a single message. Every event carries full state, so this
 *     can start mid-incident and still know payments-provider is OPEN without
 *     replaying history.
 *  2. It detects loss. Per-API sequence numbers are gapless, so a missing
 *     message is visible rather than silent.
 *  3. It is idempotent. Re-delivery of the same sequence changes nothing,
 *     which is what lets the producer be at-least-once.
 *
 * The payload is decoded through the same Schema the producer publishes with,
 * so contract drift fails loudly here instead of corrupting state silently.
 */

const ORIGIN = process.env["AGGREGATOR"] ?? "http://127.0.0.1:8088";

// Option-returning decoder: an event that does not match the published
// contract is rejected here rather than silently corrupting local state.
const decode = Schema.decodeUnknownOption(CircuitEvent);

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
        const decoded = decode(JSON.parse(line.slice(6)));
        if (O.isNone(decoded)) {
          // Loud, not silently dropped: this is contract drift.
          return Effect.logError(`undecodable event: ${line}`).pipe(
            Effect.as(undefined),
          );
        }
        const event = decoded.value;
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
