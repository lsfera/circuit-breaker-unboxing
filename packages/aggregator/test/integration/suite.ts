import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";
import { Effect, Option as O } from "effect";
import type { Layer } from "effect";
import type { Server } from "node:http";
import { CheckpointStore, LeaderElection } from "@egress/coordination/Coordination.ts";
import type { LeaseToken } from "@egress/coordination/Coordination.ts";
import { Outbox, OUTBOX_MAX_PER_API } from "@egress/coordination/Outbox.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";
import { makeWebhookSink, SOURCE } from "../../src/Events.ts";

/**
 * What every coordination backend must do, run against the real thing.
 *
 * Everything in Coordination.test.ts and Outbox.test.ts runs against the
 * in-memory double (test/support/InMemory.ts): fast, deterministic, nothing
 * external — that is what `pnpm test` runs. This is the other half: each
 * backend's statements started out reasoned from the store's documented
 * semantics, and this executes them against a real Redis or PostgreSQL in a
 * throwaway Testcontainers-managed container, proving the same fencing and
 * outbox properties by the store's own execution instead of trusting the
 * reasoning. Redis.test.ts and Postgres.test.ts each run it once.
 *
 * Opt-in (`pnpm run test:redis`, `pnpm run test:postgres`), not part of
 * `pnpm test`: it needs Docker, uses real wall-clock sleeps instead of
 * TestClock (lease expiry runs on the store's clock, so simulated time cannot
 * drive it), and is correspondingly slower.
 *
 * Not named `*.test.ts`, so the runner's glob does not pick it up.
 */

/** One isolated store: its layer, and the raw writes the tests use to fake corruption and loss. */
export type Store = {
  readonly layer: Layer.Layer<LeaderElection | CheckpointStore | Outbox>;
  /** Written straight past the store, the way a truncated write or an older build's format would arrive. */
  readonly writeRawCheckpoint: (apiId: string, raw: string) => Promise<void>;
  /** The lease's state, gone: a restart without persistence, an empty replica, an older backup. */
  readonly wipeLease: () => Promise<void>;
  /** An entry ahead of everything pending for `apiId`, bypassing `append`. */
  readonly pushRawOutboxHead: (apiId: string, raw: string) => Promise<void>;
};

export type Backend = {
  readonly name: string;
  /** False when there is no Docker to start the store in: the tests then skip, not fail. */
  readonly start: () => Promise<boolean>;
  readonly stop: () => Promise<void>;
  /** A fresh store per call, so tests do not collide on the one container. */
  readonly fresh: () => Store;
};

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

export const conformance = (backend: Backend): void => {
  let available = false;

  /**
   * Docker availability is only known once `before()` has run, which happens
   * before any test body executes but *after* `test(...)` registration — a
   * static `{ skip }` option evaluated at registration time would always see
   * the initial value and never actually skip. Checking inside the test body
   * (which node:test guarantees runs after `before()`) is what makes this
   * correct.
   */
  const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
    if (available) return false;
    t.skip("Docker is not available in this environment");
    return true;
  };

  before(async () => {
    available = await backend.start();
  });
  after(() => backend.stop());

  test(`${backend.name}: acquire, renew keeps the token, real TTL expiry allows a strictly higher one`, async (t) => {
    if (skipIfNoDocker(t)) return;
    await Effect.runPromise(Effect.provide(
      Effect.gen(function* () {
        const leader = yield* LeaderElection;
        const first = yield* leader.tryAcquireOrRenew("A", 200);
        assert.ok(O.isSome(first));

        const renewed = yield* leader.tryAcquireOrRenew("A", 200);
        assert.deepEqual(renewed, first, "renewal by the same holder keeps the token");

        yield* Effect.promise(() => sleep(400)); // real time — outlive the 200ms TTL

        const handoff = yield* leader.tryAcquireOrRenew("B", 200);
        assert.ok(O.isSome(handoff));
        assert.ok(
          (handoff as O.Some<{ counter: number }>).value.counter >
            (first as O.Some<{ counter: number }>).value.counter,
          "a real handoff must produce a strictly higher token",
        );
      }),
      backend.fresh().layer,
    ));
  });

  test(`${backend.name}: a live holder blocks a competing acquire`, async (t) => {
    if (skipIfNoDocker(t)) return;
    await Effect.runPromise(Effect.provide(
      Effect.gen(function* () {
        const leader = yield* LeaderElection;
        yield* leader.tryAcquireOrRenew("A", 10_000);
        const blocked = yield* leader.tryAcquireOrRenew("B", 10_000);
        assert.ok(O.isNone(blocked), "B must not acquire while A's lease is live");
      }),
      backend.fresh().layer,
    ));
  });

  test(`${backend.name}: a stale token is rejected even for an API no one has checkpointed yet`, async (t) => {
    if (skipIfNoDocker(t)) return;
    await Effect.runPromise(Effect.provide(
      Effect.gen(function* () {
        const leader = yield* LeaderElection;
        const checkpoints = yield* CheckpointStore;

        const aToken = yield* leader.tryAcquireOrRenew("A", 200);
        yield* Effect.promise(() => sleep(400));
        const bToken = yield* leader.tryAcquireOrRenew("B", 200);
        assert.ok(O.isSome(aToken) && O.isSome(bToken));

        // Same property Coordination.test.ts proves in-memory: an untouched
        // key must not let a stale token win just because nothing higher has
        // written there yet. Here it is the real store deciding, not our
        // in-memory Ref.
        const stale = yield* checkpoints
          .save("brand-new-api", (aToken as O.Some<LeaseToken>).value, {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 1,
            changedAt: 0,
            openBackoffMs: 4000,
          })
          .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
        assert.equal(stale, "fenced", "A's stale token must be rejected by the real store, not just reasoned about");

        const fresh = yield* checkpoints
          .save("brand-new-api", (bToken as O.Some<LeaseToken>).value, {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 1,
            changedAt: 0,
            openBackoffMs: 4000,
          })
          .pipe(Effect.as("ok" as const), Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)));
        assert.equal(fresh, "ok", "B's current token must be accepted");

        const loaded = yield* checkpoints.load("brand-new-api");
        assert.ok(O.isSome(loaded));
        assert.equal(loaded.value.sequence, 1);
      }),
      backend.fresh().layer,
    ));
  });

  /**
   * Every instance starts at once after a deploy, against a store nobody has
   * written to yet: exactly one of them may come away with a token.
   */
  test(`${backend.name}: racing acquirers on an empty store elect exactly one`, async (t) => {
    if (skipIfNoDocker(t)) return;
    const store = backend.fresh();
    const tokens = await Promise.all(
      Array.from({ length: 8 }, (_, i) =>
        Effect.runPromise(
          Effect.provide(
            Effect.flatMap(LeaderElection, (leader) => leader.tryAcquireOrRenew(`racer-${i}`, 10_000)),
            store.layer,
          ),
        ),
      ),
    );
    assert.equal(tokens.filter(O.isSome).length, 1, "one leader, however the calls interleave");
  });

  test(`${backend.name}: release only removes the lease if the caller still holds it`, async (t) => {
    if (skipIfNoDocker(t)) return;
    await Effect.runPromise(Effect.provide(
      Effect.gen(function* () {
        const leader = yield* LeaderElection;
        yield* leader.tryAcquireOrRenew("A", 200);
        // A stale release from a holder that no longer holds the lease must
        // not evict whoever holds it now.
        yield* Effect.promise(() => sleep(400));
        yield* leader.tryAcquireOrRenew("B", 10_000);
        yield* leader.release("A");
        const stillB = yield* leader.tryAcquireOrRenew("C", 200);
        assert.ok(O.isNone(stillB), "A's stale release must not evict B's live lease");
      }),
      backend.fresh().layer,
    ));
  });

  /**
   * A checkpoint is the one piece of this system's state that outlives the
   * process, which makes it the one piece that arrives as untrusted input.
   * Casting `JSON.parse` to `Checkpoint` would seed the breaker with
   * `undefined` fields and publish `NaN` sequences to every subscriber; reading
   * a value that does not decode as "no checkpoint" is a cold start, which the
   * aggregator already handles correctly.
   */
  test(`${backend.name}: a corrupt checkpoint reads as absent, not as garbage state`, async (t) => {
    if (skipIfNoDocker(t)) return;

    const store = backend.fresh();

    const { valid, truncated, notJson, wrongShape } = await Effect.runPromise(
      Effect.provide(
        Effect.gen(function* () {
          const checkpoints = yield* CheckpointStore;
          const leader = yield* LeaderElection;
          const token = yield* leader.tryAcquireOrRenew("writer", 10_000);
          assert.ok(O.isSome(token));

          yield* checkpoints.save("good", token.value, {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 7,
            changedAt: 1_000,
            openBackoffMs: 4000,
          });

          // Written straight past the store, the way a truncated write or an
          // older build's format would actually arrive.
          const put = (api: string, raw: string) =>
            Effect.promise(() => store.writeRawCheckpoint(api, raw));
          yield* put("truncated", '{"state":"OPEN","sequence":');
          yield* put("garbage", "not json at all");
          yield* put("shape", '{"state":"NOPE","sequence":"seven"}');

          return {
            valid: yield* checkpoints.load("good"),
            truncated: yield* checkpoints.load("truncated"),
            notJson: yield* checkpoints.load("garbage"),
            wrongShape: yield* checkpoints.load("shape"),
          };
        }),
        store.layer,
      ),
    );

    assert.ok(O.isSome(valid), "a well-formed checkpoint still loads");
    assert.equal((valid as O.Some<{ sequence: number }>).value.sequence, 7);
    assert.ok(O.isNone(truncated), "a truncated write must read as absent");
    assert.ok(O.isNone(notJson), "a non-JSON value must read as absent");
    assert.ok(O.isNone(wrongShape), "valid JSON of the wrong shape must read as absent");
  });

  /**
   * Fencing has to survive the coordinator losing its own state, and this is
   * the case where it did not.
   *
   * The token is a counter, so a coordinator that restarts without its state —
   * Redis without persistence, an empty replica, a database restored from an
   * older backup — starts issuing from 1 again. A leader
   * that was paused when that happened still holds, say, token 5, and the
   * checkpoint script's `attempted < current` test then reads `5 < 1`, which is
   * false. The stale leader is waved through and overwrites the new leader's
   * checkpoints: precisely the split brain fencing tokens exist to prevent,
   * reached by making the counter go backwards rather than by any race.
   *
   * The fix is that a token is not just a counter, it is an epoch and a
   * counter. A coordinator that has lost its state issues a new epoch, and a
   * token from the old one is not stale — it is unrecognisable, which is
   * stronger.
   */
  test(`${backend.name}: a coordinator that loses its state does not let a stale leader win`, async (t) => {
    if (skipIfNoDocker(t)) return;

    const store = backend.fresh();

    const { staleWrite, freshWrite } = await Effect.runPromise(
      Effect.provide(
        Effect.gen(function* () {
          const leader = yield* LeaderElection;
          const checkpoints = yield* CheckpointStore;

          const first = yield* leader.tryAcquireOrRenew("A", 10_000);
          assert.ok(O.isSome(first));
          const cp = {
            state: "OPEN",
            reason: "ALL_ENDPOINTS_EJECTED",
            sequence: 7,
            changedAt: 1_000,
            openBackoffMs: 4000,
          } as const;
          yield* checkpoints.save("payments", first.value, cp);

          // The coordinator loses its lease state: a restart with no persistence,
          // a failover to an empty replica, a restore from an older backup.
          yield* Effect.promise(() => store.wipeLease());

          // B comes along and takes the lease from a blank coordinator.
          const second = yield* leader.tryAcquireOrRenew("B", 10_000);
          assert.ok(O.isSome(second));

          // A was paused through all of this and still believes it leads.
          const staleWrite = yield* checkpoints
            .save("payments", first.value, { ...cp, sequence: 99 })
            .pipe(
              Effect.as("accepted" as const),
              Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)),
            );

          const freshWrite = yield* checkpoints
            .save("payments", second.value, { ...cp, sequence: 8 })
            .pipe(
              Effect.as("accepted" as const),
              Effect.catchTag("CheckpointFenced", () => Effect.succeed("fenced" as const)),
            );

          return { staleWrite, freshWrite };
        }),
        store.layer,
      ),
    );

    assert.equal(
      staleWrite,
      "fenced",
      "a leader holding a token from before the coordinator lost its state must not be able to write",
    );
    assert.equal(freshWrite, "accepted", "and the instance that actually holds the lease must be");
  });

  /** The subscriber. Flips between refusing and accepting, and records what it took. */
  let server: Server | null = null;
  let subscriberUrl = "";
  let accepting = false;
  let received: Array<{ apiId: string; sequence: number }> = [];

  before(async () => {
    // The subscriber this outbox exists to survive: a real HTTP endpoint that
    // can be switched off mid-test.
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
  });

  test(`${backend.name}: a subscriber that was down gets everything it missed, once, in order`, async (t) => {
    if (skipIfNoDocker(t)) return;

    accepting = false;
    received = [];
    const layer = backend.fresh().layer;

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
  test(`${backend.name}: a drain stops at the first event it cannot deliver, rather than skipping ahead`, async (t) => {
    if (skipIfNoDocker(t)) return;

    accepting = false;
    received = [];
    const layer = backend.fresh().layer;

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

          const stillPending = (yield* outbox.peek("payments", 10)).entries.map((e) =>
            O.match(e, { onNone: () => -1, onSome: (x) => x.data.sequence }),
          );

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

  /**
   * The drain commits by count, and `peek` used to filter undecodable entries out
   * of the list it returned — so the count no longer lined up with the stored
   * positions the commit trims. An entry written by a replica on a different
   * schema version (the realistic case: a rolling upgrade, sharing one store)
   * therefore shifted every delivered event one place to the right, and the trim
   * stopped short of the last one.
   *
   * Measured before the fix: an undecodable head in front of sequences 2 and 3
   * delivered `[2, 3, 3]` — the duplicate this entire system exists to prevent,
   * manufactured by the code that protects it. Alone, it was worse: the drain
   * replayed 0 forever and the API never left the pending set.
   */
  test(`${backend.name}: an entry that no longer decodes is dropped and committed past, not stepped around`, async (t) => {
    if (skipIfNoDocker(t)) return;

    accepting = true;
    received = [];
    const store = backend.fresh();

    const { depthAfter, apisAfter, replayed } = await Effect.runPromise(
      Effect.provide(
        Effect.gen(function* () {
          const outbox = yield* Outbox;
          const sink = yield* makeWebhookSink(subscriberUrl);

          for (const seq of [2, 3]) yield* outbox.append(event("payments", seq));
          // Ahead of both: a well-formed JSON entry this version cannot decode.
          yield* Effect.promise(() =>
            store.pushRawOutboxHead(
              "payments",
              JSON.stringify({ specversion: "1.0", type: "from.the.future" }),
            ),
          );

          const replayed = yield* sink.drainOutbox;
          return {
            replayed,
            depthAfter: yield* outbox.depth("payments"),
            apisAfter: yield* outbox.apis,
          };
        }),
        store.layer,
      ),
    );

    assert.deepEqual(
      received.map((r) => r.sequence),
      [2, 3],
      "the readable events go exactly once — the unreadable one must not shift them",
    );
    assert.equal(replayed, 2, "and only the delivered ones are counted as replayed");
    assert.equal(depthAfter, 0, "the entry nobody can read is trimmed rather than retried forever");
    assert.deepEqual(apisAfter, [], "so the API stops being listed instead of wedging the drain");
  });

  test(`${backend.name}: a commit after the bound dropped entries mid-drain removes only what was delivered`, async (t) => {
    if (skipIfNoDocker(t)) return;

    const remaining = await Effect.runPromise(
      Effect.provide(
        Effect.gen(function* () {
          const outbox = yield* Outbox;
          for (let seq = 1; seq <= OUTBOX_MAX_PER_API; seq++) yield* outbox.append(event("payments", seq));

          const { from, entries } = yield* outbox.peek("payments", 50);
          // A delivery failing while those 50 are posted: the bound drops the 10 oldest.
          for (let seq = OUTBOX_MAX_PER_API + 1; seq <= OUTBOX_MAX_PER_API + 10; seq++) {
            yield* outbox.append(event("payments", seq));
          }
          yield* outbox.commit("payments", from + entries.length);
          return (yield* outbox.peek("payments", 1)).entries.map((e) =>
            O.match(e, { onNone: () => -1, onSome: (x) => x.data.sequence }),
          );
        }),
        backend.fresh().layer,
      ),
    );

    assert.deepEqual(remaining, [51], "the first undelivered entry is next, not the 61st");
  });
};
