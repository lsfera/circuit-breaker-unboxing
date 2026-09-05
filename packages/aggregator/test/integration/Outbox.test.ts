import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";
import { Effect } from "effect";
import { Redis } from "ioredis";
import { GenericContainer, Wait } from "testcontainers";
import type { Server } from "node:http";
import type { StartedTestContainer } from "testcontainers";
import { makeWebhookSink, SOURCE } from "../../src/Events.ts";
import { Outbox, RedisOutboxLayer } from "../../src/Outbox.ts";
import type { RedisLike } from "../../src/Coordination.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * The gapless per-API sequence was only ever gapless *inside* the aggregator.
 * A webhook that failed its retries went into a 200-entry in-memory list that
 * exists to be read, not replayed, and that dies with the process — so a
 * subscriber down for a minute lost whatever happened in that minute, and the
 * contract this repo points at stopped one hop short of the party it is for.
 *
 * This drives the real thing end to end: a real Redis holding the outbox, a
 * real HTTP subscriber that refuses and then recovers, and the actual webhook
 * sink between them. Opt-in with `pnpm run test:redis`, for the same reasons
 * as RedisCoordination.test.ts — Docker, real sockets, real wall clock.
 */

let container: StartedTestContainer | null = null;
let client: Redis | null = null;
let dockerAvailable = true;

/** The subscriber. Flips between refusing and accepting, and records what it took. */
let server: Server | null = null;
let subscriberUrl = "";
let accepting = false;
let received: Array<{ apiId: string; sequence: number }> = [];

before(async () => {
  try {
    container = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
  } catch {
    dockerAvailable = false;
    return;
  }
  client = new Redis({
    host: container.getHost(),
    port: container.getMappedPort(6379),
    maxRetriesPerRequest: 1,
  });
  await client.ping();

  server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!accepting) {
        // 503, not a hang: a subscriber that is down usually says so, and this
        // keeps the test's failure path fast.
        res.writeHead(503).end();
        return;
      }
      const event = JSON.parse(body) as CircuitEvent;
      received.push({ apiId: event.data.apiId, sequence: event.data.sequence });
      res.writeHead(202).end();
    });
  });
  await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
  const address = server!.address();
  subscriberUrl = `http://127.0.0.1:${typeof address === "object" && address ? address.port : 0}/`;
});

after(async () => {
  await new Promise<void>((resolve) => (server ? server.close(() => resolve()) : resolve()));
  await client?.quit().catch(() => {});
  await container?.stop().catch(() => {});
});

const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
  if (dockerAvailable) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

const asRedisLike = (redis: Redis): RedisLike => ({
  eval: (script, { keys, args }) =>
    redis.eval(script, keys.length, ...keys, ...args) as Promise<string | number | null>,
});

let prefixCounter = 0;
const freshOutbox = () =>
  RedisOutboxLayer(asRedisLike(client!), `test:${Date.now()}:${prefixCounter++}`);

const event = (apiId: string, sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type: "egress.circuit.state_changed",
  source: SOURCE,
  subject: `api://${apiId}`,
  id: `id-${apiId}-${sequence}`,
  time: new Date(1_700_000_000_000 + sequence).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId,
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

/** The sink forks each delivery, so "it failed" is observed through the outbox rather than awaited. */
const waitForDepth = async (outbox: typeof Outbox.Service, apiId: string, want: number) => {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const depth = await Effect.runPromise(outbox.depth(apiId));
    if (depth >= want) return depth;
    await sleep(100);
  }
  return -1;
};

test("a subscriber that was down gets everything it missed, once, in order", async (t) => {
  if (skipIfNoDocker(t)) return;

  accepting = false;
  received = [];
  const layer = freshOutbox();

  const { depthWhileDown, replayed, depthAfter, apisAfter } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const outbox = yield* Outbox;
        const sink = yield* makeWebhookSink(subscriberUrl);

        for (const seq of [1, 2, 3, 4, 5]) yield* sink.deliver(event("payments", seq));
        const depthWhileDown = yield* Effect.promise(() =>
          waitForDepth(outbox, "payments", 5),
        );

        accepting = true;
        const replayed = yield* sink.drainOutbox;

        return {
          depthWhileDown,
          replayed,
          depthAfter: yield* outbox.depth("payments"),
          apisAfter: yield* outbox.apis,
        };
      }),
      layer,
    ),
  );

  assert.equal(depthWhileDown, 5, "every event a down subscriber refused must be kept");
  assert.equal(replayed, 5, "and replayed once the subscriber is back");
  assert.deepEqual(
    received.map((r) => r.sequence),
    [1, 2, 3, 4, 5],
    "in the order they were published — a subscriber's whole contract is that order",
  );
  assert.equal(depthAfter, 0, "delivered entries are committed, not left to be sent twice");
  assert.deepEqual(apisAfter, [], "an empty outbox stops listing the API");
});

/**
 * The failure that matters more than the happy path: a drain that skipped a
 * stuck event and delivered the ones behind it would manufacture exactly the
 * gap this whole system exists to prevent — and it would look like progress.
 */
test("a drain stops at the first event it cannot deliver, rather than skipping ahead", async (t) => {
  if (skipIfNoDocker(t)) return;

  accepting = false;
  received = [];
  const layer = freshOutbox();

  const { firstPass, stillPending, secondPass } = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const outbox = yield* Outbox;
        const sink = yield* makeWebhookSink(subscriberUrl);

        for (const seq of [1, 2, 3]) yield* sink.deliver(event("payments", seq));
        yield* Effect.promise(() => waitForDepth(outbox, "payments", 3));

        // The subscriber takes two and then falls over again, mid-drain.
        let taken = 0;
        accepting = true;
        const stopAfterTwo = setInterval(() => {
          if (received.length >= 2 && taken === 0) {
            taken = 1;
            accepting = false;
          }
        }, 1);
        const firstPass = yield* sink.drainOutbox;
        clearInterval(stopAfterTwo);

        const stillPending = (yield* outbox.peek("payments", 10)).map((e) => e.data.sequence);

        accepting = true;
        const secondPass = yield* sink.drainOutbox;
        return { firstPass, stillPending, secondPass };
      }),
      layer,
    ),
  );

  assert.ok(firstPass >= 2, `the reachable prefix must be delivered, got ${firstPass}`);
  assert.deepEqual(
    stillPending,
    stillPending.slice().sort((a, b) => a - b),
    "whatever is left is still in order",
  );
  assert.deepEqual(
    received.map((r) => r.sequence),
    [1, 2, 3],
    "and across both passes the subscriber sees every event exactly once, in order",
  );
  assert.equal(firstPass + secondPass, 3, "each event is committed exactly once");
});
