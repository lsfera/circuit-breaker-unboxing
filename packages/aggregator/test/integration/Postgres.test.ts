import { test } from "node:test";
import assert from "node:assert/strict";
import { createConnection, createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { Effect, Exit } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { AddressInfo } from "node:net";
import type { StartedTestContainer } from "testcontainers";
import postgres from "postgres";
import {
  connect,
  makePostgresBackend,
  PostgresCoordinationLayer,
} from "@egress/coordination-postgres/PostgresBackend.ts";
import { conformance } from "./suite.ts";

/** The conformance suite against a real PostgreSQL, one schema per store. */
let container: StartedTestContainer | null = null;
let pool: postgres.Sql | null = null;
let schemaCounter = 0;

conformance({
  name: "postgres",
  start: async () => {
    try {
      container = await new GenericContainer("postgres:18-alpine")
        .withEnvironment({ POSTGRES_PASSWORD: "test" })
        .withExposedPorts(5432)
        // The entrypoint starts a temporary server for initdb first; the second
        // "ready" is the real one.
        .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
        .start();
    } catch {
      return false;
    }
    pool = connect(
      `postgres://postgres:test@${container.getHost()}:${container.getMappedPort(5432)}/postgres`,
    );
    await pool`SELECT 1`;
    return true;
  },
  stop: async () => {
    await pool?.end({ timeout: 1 }).catch(() => {});
    await container?.stop().catch(() => {});
  },
  fresh: () => {
    const sql = pool!;
    const schema = `test_${Date.now()}_${schemaCounter++}`;
    return {
      layer: PostgresCoordinationLayer(sql, schema),
      writeRawCheckpoint: async (apiId, raw) => {
        await sql.unsafe(
          `INSERT INTO ${schema}.checkpoint (api_id, payload) VALUES ($1, $2)
           ON CONFLICT (api_id) DO UPDATE SET payload = excluded.payload`,
          [apiId, raw],
        );
      },
      wipeLease: async () => {
        await sql.unsafe(`DELETE FROM ${schema}.lease`);
      },
      pushRawOutboxHead: async (apiId, raw) => {
        await sql.unsafe(
          `INSERT INTO ${schema}.outbox (api_id, pos, payload)
           SELECT $1, coalesce(min(pos), 0) - 1, $2 FROM ${schema}.outbox WHERE api_id = $1`,
          [apiId, raw],
        );
      },
    };
  },
});

/**
 * Found by chaos-load `kill-store`: after PostgreSQL restarted, neither
 * aggregator led again. Every call during the outage waited on a reconnect and
 * timed out, and cancelling a query postgres.js was opening a connection for
 * leaves that connection busy for ever, so the whole pool wedged.
 *
 * Here one pooled connection is dropped and its replacement takes longer to
 * open than a call is given: that call fails, and the next must not.
 */
test("postgres: a call given up on while its connection opens does not wedge the pool", async (t) => {
  if (!container || !pool) return void t.skip("Docker is not available in this environment");

  let delayMs = 0;
  const proxy = createServer((client) => {
    setTimeout(() => {
      const upstream = createConnection(container!.getMappedPort(5432), container!.getHost());
      client.pipe(upstream).pipe(client);
      upstream.on("error", () => client.destroy());
      client.on("error", () => upstream.destroy());
    }, delayMs);
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  const slow = postgres(`postgres://postgres:test@127.0.0.1:${(proxy.address() as AddressInfo).port}/postgres`, {
    max: 1,
    connect_timeout: 10,
    onnotice: () => {},
    connection: { application_name: "wedge-test" },
  });
  const { leaderElection } = makePostgresBackend(slow, `test_wedge_${Date.now()}`);
  const acquire = () => Effect.runPromiseExit(leaderElection.tryAcquireOrRenew("A", 10_000));

  try {
    assert.ok(Exit.isSuccess(await acquire()), "the pool works to begin with");

    delayMs = 1500;
    await pool`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'wedge-test'`;
    await sleep(200);
    assert.ok(Exit.isFailure(await acquire()), "a call outlasting its reconnect fails");

    await sleep(2500);
    assert.ok(Exit.isSuccess(await acquire()), "and once the connection is open, the next call goes through");
  } finally {
    await slow.end({ timeout: 1 }).catch(() => {});
    proxy.close();
  }
});
