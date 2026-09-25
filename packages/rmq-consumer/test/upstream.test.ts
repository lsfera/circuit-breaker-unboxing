import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { Effect } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { IDEMPOTENCY_KEY_HTTP_HEADER } from "@egress/rmq/WorkQueue.ts";
import * as Upstream from "../src/Upstream.ts";

const run = (url: string, key = "run:1") =>
  Effect.runPromise(Upstream.call(url, key).pipe(Effect.provide(FetchHttpClient.layer)));

const serving = async (handler: Parameters<typeof createServer>[1]) => {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return { url: `http://127.0.0.1:${port}/payments`, close: () => server.closeAllConnections() ?? server.close() };
};

test("a 2xx comes back as its status, and the idempotency key travels as the third party's header", async () => {
  const seen: Array<string | string[] | undefined> = [];
  const upstream = await serving((req, res) => {
    seen.push(req.headers[IDEMPOTENCY_KEY_HTTP_HEADER]);
    res.end("fine");
  });
  assert.equal(await run(upstream.url, "abc:7"), 200);
  assert.deepEqual(seen, ["abc:7"]);
  upstream.close();
});

test("whatever status the third party answers with comes back as it is, unjudged", async () => {
  for (const status of [200, 204, 400, 404, 408, 422, 429, 500, 503, 504]) {
    const upstream = await serving((_, res) => res.writeHead(status).end("body"));
    assert.equal(await run(upstream.url), status);
    upstream.close();
  }
});

test("a refused connection is `network`, not an exception", async () => {
  const upstream = await serving((_, res) => res.end());
  upstream.close();
  assert.equal(await run(upstream.url.replace(/:\d+/, ":1")), "network");
});

test("a connection dropped mid-request is `network`", async () => {
  const upstream = await serving((req) => req.socket.destroy());
  assert.equal(await run(upstream.url), "network");
  upstream.close();
});

test("a third party that never answers is `timeout`, after the timeout", async () => {
  const upstream = await serving(() => {});
  const started = Date.now();
  assert.equal(await run(upstream.url), "timeout");
  assert.ok(Date.now() - started < 3000);
  upstream.close();
});
