import { Effect, Layer, Option as O, Predicate, Result } from "effect";
import postgres from "postgres";
import {
  CheckpointFenced,
  CheckpointStore,
  COORDINATION_TIMEOUT_MS,
  guarded,
  LeaderElection,
  newEpoch,
  parseToken,
  readCheckpoint,
} from "@egress/coordination/Coordination.ts";
import type { Checkpoint, LeaseToken } from "@egress/coordination/Coordination.ts";
import { decodeEntry, Outbox, OUTBOX_MAX_PER_API } from "@egress/coordination/Outbox.ts";
import type { Peeked } from "@egress/coordination/Outbox.ts";

/**
 * The ports in @egress/coordination over PostgreSQL. Where Redis runs a Lua
 * script, this runs one statement (or, for the outbox's append, one short
 * transaction), so the same things cannot interleave:
 *
 * - the lease is one row, and acquiring is one upsert whose `WHERE` decides
 *   between a renewal, a handoff and a refusal. Expiry is the database's clock
 *   (`now()`), never an aggregator's, as Redis's `PX` is Redis's;
 * - a checkpoint reads the lease `FOR SHARE` in the statement that writes it.
 *   Without the lock a takeover could commit between the read and the write
 *   under read committed, and a fenced leader would still write once;
 * - outbox positions are absolute and only grow, so a commit from an older
 *   peek can only ever delete what that peek saw.
 *
 * The epoch stays: a restore from backup, or a promoted replica that was
 * behind, rolls the counter back just as a Redis wipe does.
 */

/** Interpolated into DDL and table names, so it is checked rather than escaped. */
const SCHEMA_NAME = /^[a-z_][a-z0-9_]{0,62}$/;

const ddl = (schema: string) => `
CREATE SCHEMA IF NOT EXISTS ${schema};
CREATE TABLE IF NOT EXISTS ${schema}.lease (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  epoch text NOT NULL,
  counter bigint NOT NULL,
  holder text,
  expires_at timestamptz
);
CREATE TABLE IF NOT EXISTS ${schema}.checkpoint (
  api_id text PRIMARY KEY,
  payload text NOT NULL
);
CREATE TABLE IF NOT EXISTS ${schema}.outbox_position (
  api_id text PRIMARY KEY,
  next bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS ${schema}.outbox (
  api_id text NOT NULL,
  pos bigint NOT NULL,
  payload text NOT NULL,
  PRIMARY KEY (api_id, pos)
);
`;

const statements = (schema: string) => ({
  // $1 holder, $2 ttl ms, $3 candidate epoch. No row means lost state: adopt the
  // candidate epoch. Renewing keeps the counter; anything else past the WHERE is
  // a handoff and bumps it. A live lease held by someone else returns no row.
  acquire: `
    INSERT INTO ${schema}.lease AS l (id, epoch, counter, holder, expires_at)
    VALUES (true, $3, 1, $1, now() + $2::int * interval '1 millisecond')
    ON CONFLICT (id) DO UPDATE SET
      counter = CASE WHEN l.holder = excluded.holder AND l.expires_at > now()
                     THEN l.counter ELSE l.counter + 1 END,
      holder = excluded.holder,
      expires_at = excluded.expires_at
    WHERE l.holder IS NULL OR l.expires_at <= now() OR l.holder = excluded.holder
    RETURNING l.epoch || ':' || l.counter AS token`,
  // The row stays: the counter must outlive a release.
  release: `UPDATE ${schema}.lease SET holder = NULL, expires_at = NULL WHERE holder = $1`,
  // $1 api, $2 payload, $3 attempted epoch, $4 attempted counter.
  save: `
    WITH current AS (SELECT epoch, counter FROM ${schema}.lease WHERE id FOR SHARE),
    written AS (
      INSERT INTO ${schema}.checkpoint (api_id, payload)
      SELECT $1::text, $2::text FROM current WHERE current.epoch = $3::text AND current.counter <= $4::bigint
      ON CONFLICT (api_id) DO UPDATE SET payload = excluded.payload
      RETURNING 1
    )
    SELECT (SELECT epoch || ':' || counter FROM current) AS current,
           (SELECT count(*) FROM written)::int AS written`,
  load: `SELECT payload FROM ${schema}.checkpoint WHERE api_id = $1`,
  // The upsert row-locks the API's position, so appends to one API serialize.
  nextPosition: `
    INSERT INTO ${schema}.outbox_position AS p (api_id, next) VALUES ($1, 1)
    ON CONFLICT (api_id) DO UPDATE SET next = p.next + 1
    RETURNING p.next::text AS next`,
  append: `INSERT INTO ${schema}.outbox (api_id, pos, payload) VALUES ($1, $2::bigint, $3)`,
  trim: `DELETE FROM ${schema}.outbox WHERE api_id = $1 AND pos < $2::bigint`,
  // Ordered by the column, not the text alias: "100" sorts before "50".
  peek: `SELECT o.pos::text AS position, o.payload FROM ${schema}.outbox o WHERE o.api_id = $1 ORDER BY o.pos LIMIT $2::int`,
  apis: `SELECT DISTINCT api_id FROM ${schema}.outbox ORDER BY api_id`,
  depth: `SELECT count(*)::int AS depth FROM ${schema}.outbox WHERE api_id = $1`,
});

type Rows = ReadonlyArray<Record<string, unknown>>;

/**
 * A query given up on is abandoned, never cancelled from here. postgres.js
 * leaves a connection busy for ever when a query it was opening that
 * connection for is cancelled: after a store outage every call waited on a
 * reconnect, timed out, and wedged the whole pool, so no instance led again
 * (chaos-load `kill-store`). What actually runs is bounded on the server by
 * `statement_timeout` and `lock_timeout`, and one that lands late is harmless:
 * a renewal keeps the token, a stale checkpoint is fenced.
 */
const run = (
  sql: postgres.Sql | postgres.TransactionSql,
  text: string,
  params: ReadonlyArray<string | number>,
): Promise<Rows> => sql.unsafe(text, [...params]);

const stringField = (rows: Rows, field: string): O.Option<string> =>
  O.liftPredicate(rows[0]?.[field], Predicate.isString);

/**
 * Every port over one pool. `schema` keeps instances (and tests) apart, as the
 * key prefix does for Redis. Tables are created on the first call that reaches
 * the database, not at startup: unreachable is a failed call the aggregator
 * stands down over, never a crash.
 */
export const makePostgresBackend = (sql: postgres.Sql, schema = "egress_aggregator") => {
  if (!SCHEMA_NAME.test(schema)) throw new Error(`not a usable schema name: ${schema}`);
  const q = statements(schema);

  // One migration per process, retried after a failure. The advisory lock
  // serializes instances, since concurrent CREATE ... IF NOT EXISTS can still collide.
  let migrated: Promise<void> | null = null;
  const ready = (): Promise<void> =>
    (migrated ??= sql
      .begin(async (tx) => {
        await tx.unsafe(`SELECT pg_advisory_xact_lock(hashtext('egress:${schema}'))`);
        await tx.unsafe(ddl(schema));
      })
      .then(
        () => {},
        (error: unknown) => {
          migrated = null;
          throw error;
        },
      ));

  const call = (
    operation: string,
    text: string,
    params: ReadonlyArray<string | number>,
  ) => guarded(operation, () => ready().then(() => run(sql, text, params)));

  const leaderElection: typeof LeaderElection.Service = {
    tryAcquireOrRenew: (holderId, ttlMs) =>
      call("tryAcquireOrRenew", q.acquire, [holderId, ttlMs, newEpoch()]).pipe(
        // No row: someone else holds it. Unparseable reads the same way.
        Effect.map((rows) => O.flatMap(stringField(rows, "token"), parseToken)),
      ),
    release: (holderId) => call("release", q.release, [holderId]).pipe(Effect.asVoid),
  };

  const checkpointStore: typeof CheckpointStore.Service = {
    save: (apiId: string, token: LeaseToken, checkpoint: Checkpoint) =>
      call("save", q.save, [apiId, JSON.stringify(checkpoint), token.epoch, token.counter]).pipe(
        Effect.flatMap((rows) =>
          rows[0]?.["written"] === 1
            ? Effect.void
            : Effect.fail(
                new CheckpointFenced({
                  apiId,
                  attempted: token,
                  current: O.flatMap(stringField(rows, "current"), parseToken),
                }),
              ),
        ),
      ),
    load: (apiId) =>
      call("load", q.load, [apiId]).pipe(
        Effect.flatMap((rows) =>
          O.match(stringField(rows, "payload"), {
            onNone: () => Effect.succeed(O.none<Checkpoint>()),
            // As in Redis: unreadable is a logged cold start, not a failure.
            onSome: (raw) =>
              Result.match(readCheckpoint(raw), {
                onSuccess: (checkpoint) => Effect.succeed(O.some(checkpoint)),
                onFailure: (why) =>
                  Effect.as(
                    Effect.logWarning(`checkpoint for ${apiId} is ${why} — resuming that API from nothing`),
                    O.none<Checkpoint>(),
                  ),
              }),
          }),
        ),
      ),
  };

  const outbox: typeof Outbox.Service = {
    // A transaction, not one statement: under read committed each statement
    // takes a fresh snapshot after the position lock, so the trim sees every
    // entry an append that held the lock before it committed.
    append: (event) =>
      guarded("outbox.append", () =>
        ready().then(() =>
          sql.begin(async (tx) => {
            const apiId = event.data.apiId;
            const next = Number(O.getOrThrow(stringField(await run(tx, q.nextPosition, [apiId]), "next")));
            await run(tx, q.append, [apiId, next - 1, JSON.stringify(event)]);
            const trimmed = await tx.unsafe(q.trim, [apiId, next - OUTBOX_MAX_PER_API]);
            return trimmed.count;
          }),
        ),
      ),

    peek: (apiId, limit) =>
      call("outbox.peek", q.peek, [apiId, limit]).pipe(
        // Empty peeks commit nothing, so their `from` is never used to trim.
        Effect.map((rows): Peeked => ({
          from: Number(rows[0]?.["position"] ?? 0),
          entries: rows.map((row) => decodeEntry(String(row["payload"]))),
        })),
      ),

    commit: (apiId, through) => call("outbox.commit", q.trim, [apiId, through]).pipe(Effect.asVoid),

    apis: call("outbox.apis", q.apis, []).pipe(
      Effect.map((rows) => rows.map((row) => row["api_id"]).filter(Predicate.isString)),
    ),

    depth: (apiId) =>
      call("outbox.depth", q.depth, [apiId]).pipe(
        Effect.map((rows) => Number(rows[0]?.["depth"] ?? 0)),
      ),
  };

  return { leaderElection, checkpointStore, outbox };
};

/** The three services over an existing pool, for tests that own the pool. */
export const PostgresCoordinationLayer = (
  sql: postgres.Sql,
  schema?: string,
): Layer.Layer<LeaderElection | CheckpointStore | Outbox> => {
  const backend = makePostgresBackend(sql, schema);
  return Layer.mergeAll(
    Layer.succeed(LeaderElection, backend.leaderElection),
    Layer.succeed(CheckpointStore, backend.checkpointStore),
    Layer.succeed(Outbox, backend.outbox),
  );
};

/** The pool every process opens: small, quick to give up, and bounded on the server too. */
export const connect = (url: string): postgres.Sql =>
  postgres(url, {
    max: 4,
    connect_timeout: Math.ceil(COORDINATION_TIMEOUT_MS / 1000),
    idle_timeout: 30,
    onnotice: () => {},
    connection: {
      application_name: "egress-aggregator",
      // Server-side bounds behind the client's: a call the client gave up on
      // must not keep holding the lease row's lock.
      statement_timeout: COORDINATION_TIMEOUT_MS,
      lock_timeout: COORDINATION_TIMEOUT_MS,
      idle_in_transaction_session_timeout: COORDINATION_TIMEOUT_MS,
    },
  });

/** Every coordination port over one pool, opened for the layer's lifetime. */
export const PostgresBackendLayer = (
  url: string,
): Layer.Layer<LeaderElection | CheckpointStore | Outbox> =>
  Layer.unwrap(
    Effect.acquireRelease(Effect.sync(() => connect(url)), (sql) =>
      Effect.promise(() => sql.end({ timeout: 1 }).catch(() => {})),
    ).pipe(Effect.map((sql) => PostgresCoordinationLayer(sql))),
  );
