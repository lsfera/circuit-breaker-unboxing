import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { Effect, Layer } from "effect";
import { InMemoryOutboxLayer } from "../src/Outbox.ts";
import { makeWebhookSink, SOURCE } from "../src/Events.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/** A real subscriber over HTTP, recording the sequences it accepted, told when to refuse. */

const event = (sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type: "egress.circuit.state_changed",
  source: SOURCE,
  subject: "api://payments-provider",
  id: `id-${sequence}`,
  time: new Date(1_700_000_000_000 + sequence).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId: "payments-provider",
    sequence,
    previousState: "CLOSED",
    state: "OPEN",
    reason: "ALL_ENDPOINTS_EJECTED",
    healthyEndpoints: 0,
    totalEndpoints: 6,
    observedSince: new Date(1_700_000_000_000).toISOString(),
    reportingReplicas: 3,
  },
});

const subscriber = async () => {
  const received: number[] = [];
  const state = { refusing: false };
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const refused = state.refusing;
      if (!refused) received.push(JSON.parse(Buffer.concat(chunks).toString()).data.sequence);
      res.writeHead(refused ? 503 : 202).end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`;
  return { url, received, state, close: () => new Promise<void>((resolve) => server.close(() => resolve())) };
};

const run = <A, E>(effect: Effect.Effect<A, E, never>) => Effect.runPromise(Effect.scoped(effect));

test("an event delivered while an earlier one waits in the outbox arrives after it, not before", async () => {
  const sub = await subscriber();
  try {
    await run(
      Effect.gen(function* () {
        const sink = yield* makeWebhookSink(sub.url);
        sub.state.refusing = true;
        yield* sink.deliver(event(1));
        // Past DELIVERY_RETRY (100 + 200 + 400 ms of backoff): 1 is in the outbox.
        yield* Effect.sleep("1500 millis");
        sub.state.refusing = false;
        yield* sink.deliver(event(2));
        yield* Effect.sleep("300 millis");
        yield* sink.drainOutbox;
        yield* Effect.sleep("300 millis");
      }).pipe(Effect.provide(Layer.fresh(InMemoryOutboxLayer))),
    );
    assert.deepEqual(sub.received, [1, 2]);
  } finally {
    await sub.close();
  }
});

test("back-to-back deliveries for one API arrive in the order they were handed over", async () => {
  const sub = await subscriber();
  try {
    await run(
      Effect.gen(function* () {
        const sink = yield* makeWebhookSink(sub.url);
        yield* Effect.forEach([1, 2, 3, 4, 5, 6, 7, 8], (n) => sink.deliver(event(n)), { discard: true });
        yield* Effect.sleep("500 millis");
      }).pipe(Effect.provide(Layer.fresh(InMemoryOutboxLayer))),
    );
    assert.deepEqual(sub.received, [1, 2, 3, 4, 5, 6, 7, 8]);
  } finally {
    await sub.close();
  }
});
