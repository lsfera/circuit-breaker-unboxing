import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { Effect } from "effect";
import { EnvoyFleetLayer, FleetSource } from "../src/FleetSource.ts";

/** A real admin endpoint answering whatever body the test gives it. */
const admin = async (body: string) => {
  const server = createServer((_, res) => res.writeHead(200, { "content-type": "application/json" }).end(body));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const adminUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return { adminUrl, close: () => new Promise<void>((resolve) => server.close(() => resolve())) };
};

const poll = (adminUrl: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* FleetSource).poll;
    }).pipe(
      Effect.provide(EnvoyFleetLayer([{ replicaId: "envoy-00", adminUrl }], [{ apiId: "payments", endpoints: 6, rps: 0, failureRate: 0 }])),
    ),
  );

for (const [what, body] of [
  ["null", "null"],
  ["stats that is not an array", `{"stats":"nope"}`],
  [
    "a count that is not a number",
    `{"stats":[{"name":"cluster.payments.membership_healthy","value":"4"},{"name":"cluster.payments.membership_total","value":6}]}`,
  ],
] as const) {
  test(`a replica answering ${what} reports nothing, and the poll survives it`, async () => {
    const replica = await admin(body);
    try {
      assert.deepEqual(await poll(replica.adminUrl), []);
    } finally {
      await replica.close();
    }
  });
}

test("histogram entries share the array with counters and are skipped, not fatal", async () => {
  const replica = await admin(
    JSON.stringify({
      stats: [
        { name: "cluster.payments.membership_healthy", value: 4 },
        { name: "cluster.payments.membership_total", value: 6 },
        { histograms: { supported_quantiles: [50, 99], computed_quantiles: [] } },
      ],
    }),
  );
  try {
    const reports = await poll(replica.adminUrl);
    assert.equal(reports.length, 1);
    assert.equal(reports[0]?.healthy, 4);
    assert.equal(reports[0]?.total, 6);
  } finally {
    await replica.close();
  }
});
