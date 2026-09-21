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

test("a 2xx is ok, and the idempotency key travels as the third party's header", async () => {
  const seen: Array<string | string[] | undefined> = [];
  const upstream = await serving((req, res) => {
    seen.push(req.headers[IDEMPOTENCY_KEY_HTTP_HEADER]);
    res.end("fine");
  });
  assert.equal(await run(upstream.url, "abc:7"), "ok");
  assert.deepEqual(seen, ["abc:7"]);
  upstream.close();
});

test("a 5xx and a 429 are both a failed call", async () => {
  for (const status of [500, 503, 429]) {
    const upstream = await serving((_, res) => res.writeHead(status).end("no"));
    assert.equal(await run(upstream.url), "failed");
    upstream.close();
  }
});

test("a refused connection is a failed call, not an exception", async () => {
  const upstream = await serving((_, res) => res.end());
  upstream.close();
  assert.equal(await run(upstream.url.replace(/:\d+/, ":1")), "failed");
});

test("a third party that never answers is a failed call after the timeout", async () => {
  const upstream = await serving(() => {});
  const started = Date.now();
  assert.equal(await run(upstream.url), "failed");
  assert.ok(Date.now() - started < 3000);
  upstream.close();
});
