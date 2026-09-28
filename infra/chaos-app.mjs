/**
 * Chaos for the consumer application (packages/consumer): both consumers under load, faults injected into either
 * dependency — the third party, or the PostgreSQL ledger — and each run judged first on correctness, then on what
 * each dependency's breaker did.
 *
 *   node infra/chaos-app.mjs                          # every scenario
 *   node infra/chaos-app.mjs --scenarios=db-down,upstream-outage
 *   node infra/chaos-app.mjs --list
 *   node infra/chaos-app.mjs --format=mixed           # bodies alternate JSON and protobuf (or --format=protobuf)
 *
 * Correctness, per message. Every message carries `message_id: <run>:<n>`; the publishers report which n the broker
 * confirmed, the fake third party which n it charged, and the ledger which n it recorded. Payments: a confirmed n
 * never charged is dead-lettered or lost, and the ledger must be exact — every charged n recorded once, and nothing
 * recorded that was not charged. Refunds: a confirmed n never recorded is dead-lettered or lost. The bar is nothing
 * lost, both dead-letter queues back where they started, nothing parked, one probe permit per dependency, and every
 * replica's transitions legal for each dependency.
 *
 * Then behaviour, per scenario: which dependency's breakers opened, and whether each consumer kept consuming.
 *
 * Assumes `docker compose up -d` on this branch, and a shell that reaches the services by name and can run `docker`.
 * It stops, kills and pauses real containers, so do not point it at anything you care about. Both compose producers
 * are stopped for a run and started again at the end.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import * as lib from "./chaos-lib.mjs";

const { audit, breakerStates, consumerContainers, decodeBits, exec, flag, isSet, PROJECT, promSum, queueInfo } = lib;
const { setFailure, skewMs, sleep, startPublisher, transitionsOf, tryQueueInfo, waitFor } = lib;

const PAYMENTS = "payments-provider";
const REFUNDS = "refunds-provider";
const THIRD_PARTY = "payments-api";
const LEDGER = "ledger";
const DEPENDENCIES = [THIRD_PARTY, LEDGER];
const POSTGRES = `${PROJECT}-postgres-1`;
const PRODUCERS = [`${PROJECT}-rmq-producer-1`, `${PROJECT}-rmq-producer-refunds-1`];

const PAYMENTS_RATE = Number(flag("rate", 200));
const PAYMENTS_SPIKE = Number(flag("spike", 1000));
const REFUNDS_RATE = Number(flag("refunds-rate", 50));
const REFUNDS_SPIKE = Number(flag("refunds-spike", 200));
const SETTLE_S = Number(flag("settle", 8));
const FAULT_S = Number(flag("fault-seconds", 40));
const RECOVERY_TIMEOUT_S = Number(flag("recovery-timeout", 240));
const DRAIN_TIMEOUT_S = Number(flag("drain-timeout", 180));
const FORMAT = String(flag("format", "json"));
const OUT = flag("out", `history/runs/chaos-app-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

// ---- reading the system ------------------------------------------------------------

const psql = (sql, { user = "consumer", database = "ledger" } = {}) =>
  exec("docker", ["exec", POSTGRES, "psql", "-U", user, "-d", database, "-At", "-c", sql], { maxBuffer: 1 << 26 });

/** The n recorded for a run, with how many rows each has (the primary key makes more than one impossible). */
const recorded = async (table, run) => {
  const { stdout } = await psql(`SELECT n FROM ${table} WHERE message_id LIKE '${run}:%'`);
  return stdout.split("\n").filter(Boolean).map(Number);
};

const allClosed = async () => {
  const states = await breakerStates();
  return states !== undefined && states.length >= 2 * 5 && states.every((s) => s === 0);
};

const observe = async (series, t0, seconds) => {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    const [payments, refunds, third, ledger] = await Promise.all([
      tryQueueInfo(`${PAYMENTS}.work`),
      tryQueueInfo(`${REFUNDS}.work`),
      breakerStates(THIRD_PARTY),
      breakerStates(LEDGER),
    ]);
    const notClosed = (states) => states?.filter((s) => s !== 0).length ?? null;
    series.push({
      t: Math.round((Date.now() - t0) / 1000),
      payments: payments?.ready ?? null,
      paymentsConsumers: payments?.consumers ?? null,
      refunds: refunds?.ready ?? null,
      refundsConsumers: refunds?.consumers ?? null,
      thirdPartyNotClosed: notClosed(third),
      ledgerNotClosed: notClosed(ledger),
    });
    await sleep(1000);
  }
};

const increase = (metric, labels, window) =>
  promSum(`sum(increase(${metric}{${labels}}[${window}]))`).then(Math.round);

// ---- a run -------------------------------------------------------------------------

/**
 * @param fault {{ inject: (ctx) => Promise<void>, during?: (ctx) => Promise<void>, restore: (ctx) => Promise<void>,
 *   expect: (ctx) => Promise<{ pass: boolean, summary: string }> }}
 */
const scenario = async (name, fault) => {
  const started = new Date();
  const skewAtStart = skewMs();
  const tag = `${name.replace(/[^a-z]/g, "").slice(0, 6)}${Date.now().toString(36)}`;
  const runs = { payments: `${tag}p`, refunds: `${tag}r` };
  console.log(`\n== ${name} (runs ${runs.payments}, ${runs.refunds}) ==`);
  await setFailure({});
  await Promise.all(PRODUCERS.map((p) => exec("docker", ["stop", p]).catch(() => {})));
  await waitFor(allClosed, 90);
  const before = {
    paymentsDead: (await queueInfo(`${PAYMENTS}.work.dead`)).ready,
    refundsDead: (await queueInfo(`${REFUNDS}.work.dead`)).ready,
    paymentsParked: (await queueInfo(`${PAYMENTS}.work.parked`)).ready,
    refundsParked: (await queueInfo(`${REFUNDS}.work.parked`)).ready,
  };
  const series = [];
  const t0 = Date.now();
  const ctx = { series, t0, runs, started };
  const publishers = {
    payments: startPublisher(runs.payments, PAYMENTS, PAYMENTS_RATE, FORMAT),
    refunds: startPublisher(runs.refunds, REFUNDS, REFUNDS_RATE, FORMAT),
  };

  await observe(series, t0, SETTLE_S);
  publishers.payments.rate(PAYMENTS_SPIKE);
  publishers.refunds.rate(REFUNDS_SPIKE);
  ctx.faultAt = Math.round((Date.now() - t0) / 1000);
  console.log(`  t+${ctx.faultAt}s  fault injected under the spike`);
  await fault.inject(ctx);
  const during = fault.during?.(ctx) ?? Promise.resolve();
  await observe(series, t0, FAULT_S);
  await during;
  await fault.restore(ctx);
  publishers.payments.rate(PAYMENTS_RATE);
  publishers.refunds.rate(REFUNDS_RATE);
  ctx.restoredAt = Math.round((Date.now() - t0) / 1000);
  console.log(`  t+${ctx.restoredAt}s  restored`);

  const recovered = await waitFor(async () => {
    await observe(series, t0, 1);
    return allClosed();
  }, RECOVERY_TIMEOUT_S);
  const recoveredAt = Math.round((Date.now() - t0) / 1000);

  const final = { payments: await publishers.payments.stop(), refunds: await publishers.refunds.stop() };
  const drained = await waitFor(async () => {
    await observe(series, t0, 1);
    const [p, r, pd, rd] = await Promise.all([
      tryQueueInfo(`${PAYMENTS}.work`),
      tryQueueInfo(`${REFUNDS}.work`),
      tryQueueInfo(`${PAYMENTS}.work.dead`),
      tryQueueInfo(`${REFUNDS}.work.dead`),
    ]);
    return p?.ready === 0 && r?.ready === 0 && pd?.ready === before.paymentsDead && rd?.ready === before.refundsDead;
  }, DRAIN_TIMEOUT_S);
  await sleep(3000);

  // ---- correctness ----
  const charged = await audit(runs.payments);
  const chargedBits = decodeBits(charged.bits);
  const paymentRows = await recorded("payments", runs.payments);
  const refundRows = await recorded("refunds", runs.refunds);
  const paymentSet = new Set(paymentRows);
  const refundSet = new Set(refundRows);
  const dead = {
    payments: (await queueInfo(`${PAYMENTS}.work.dead`)).ready - before.paymentsDead,
    refunds: (await queueInfo(`${REFUNDS}.work.dead`)).ready - before.refundsDead,
  };
  const parked = {
    payments: (await queueInfo(`${PAYMENTS}.work.parked`)).ready - before.paymentsParked,
    refunds: (await queueInfo(`${REFUNDS}.work.parked`)).ready - before.refundsParked,
  };
  const confirmedP = decodeBits(final.payments.bits);
  const confirmedR = decodeBits(final.refunds.bits);
  const range = (n) => Array.from({ length: n }, (_, i) => i);
  const unchargedP = range(final.payments.sent).filter((n) => isSet(confirmedP, n) && !isSet(chargedBits, n));
  const unrecordedR = range(final.refunds.sent).filter((n) => isSet(confirmedR, n) && !refundSet.has(n));
  const lostP = Math.max(0, unchargedP.length - dead.payments - parked.payments);
  const lostR = Math.max(0, unrecordedR.length - dead.refunds - parked.refunds);
  // The ledger is exact: a charge is recorded once, and nothing is recorded without one.
  const chargedNotRecorded = range(Math.max(final.payments.sent, charged.maxN + 1)).filter(
    (n) => isSet(chargedBits, n) && !paymentSet.has(n),
  );
  const recordedNotCharged = paymentRows.filter((n) => !isSet(chargedBits, n));
  const duplicateRows = paymentRows.length - paymentSet.size + (refundRows.length - refundSet.size);

  // ---- the breakers ----
  const window = `${Math.ceil((Date.now() - started.getTime()) / 1000)}s`;
  const trips = Object.fromEntries(
    await Promise.all(
      DEPENDENCIES.map(async (d) => [d, await increase("egress_consumer_breaker_trips_total", `dependency="${d}"`, window)]),
    ),
  );
  const calls = Object.fromEntries(
    await Promise.all(
      DEPENDENCIES.flatMap((d) =>
        ["ok", "failed", "throttled", "client_error"].map(async (o) => [
          `${d}:${o}`,
          await increase("egress_consumer_calls_total", `dependency="${d}",outcome="${o}"`, window),
        ]),
      ),
    ),
  );
  const permits = Object.fromEntries(
    await Promise.all(DEPENDENCIES.map(async (d) => [d, await lib.permitTokens(lib.permitQueueFor(d)).catch(() => ({}))])),
  );
  const onePermitEach = DEPENDENCIES.every((d) => permits[d].ready === 1 && permits[d].held === 0);
  const replicas = await Promise.all(
    (await consumerContainers()).flatMap((c) => DEPENDENCIES.map((d) => transitionsOf(c, started.toISOString(), { dependency: d }))),
  );
  const violations = replicas.flatMap((r) => r.violations.map((v) => `${r.container} ${r.dependency}: ${v}`));
  ctx.trips = trips;
  ctx.calls = calls;
  ctx.window = window;
  const expected = await fault.expect(ctx);

  const correct =
    lostP === 0 &&
    lostR === 0 &&
    dead.payments === 0 &&
    dead.refunds === 0 &&
    parked.payments === 0 &&
    parked.refunds === 0 &&
    chargedNotRecorded.length === 0 &&
    recordedNotCharged.length === 0 &&
    duplicateRows === 0;
  const pass = correct && drained && recovered && violations.length === 0 && onePermitEach && expected.pass;
  const suspendedMs = Math.round(skewMs() - skewAtStart);
  const voided = suspendedMs > 2000;

  console.log(
    `  payments: sent ${final.payments.sent}, confirmed ${final.payments.confirmed}, charged ${charged.processed} ` +
      `(${charged.duplicates} repeat charges), recorded ${paymentSet.size}; uncharged ${unchargedP.length}, lost ${lostP}, ` +
      `dead ${dead.payments}, parked ${parked.payments}`,
  );
  console.log(
    `  ledger exact: ${chargedNotRecorded.length} charged but not recorded, ${recordedNotCharged.length} recorded but not charged`,
  );
  console.log(
    `  refunds: sent ${final.refunds.sent}, confirmed ${final.refunds.confirmed}, recorded ${refundSet.size}; ` +
      `unrecorded ${unrecordedR.length}, lost ${lostR}, dead ${dead.refunds}, parked ${parked.refunds}`,
  );
  console.log(
    `  breakers: trips ${DEPENDENCIES.map((d) => `${d}=${trips[d]}`).join(" ")}; all closed ${recoveredAt - ctx.restoredAt}s after restore; ` +
      `transitions legal: ${violations.length === 0}; permits ${DEPENDENCIES.map((d) => `${d}=${permits[d].ready}+${permits[d].held}`).join(" ")}`,
  );
  console.log(`  calls: ${Object.entries(calls).filter(([, v]) => v > 0).map(([k, v]) => `${k}=${v}`).join(" ")}`);
  console.log(`  expected: ${expected.summary}`);
  console.log(`  ${voided ? `VOID (the host was suspended for ${Math.round(suspendedMs / 1000)}s)` : pass ? "PASS" : "FAIL"}`);
  await Promise.all(PRODUCERS.map((p) => exec("docker", ["start", p]).catch(() => {})));

  return {
    scenario: name,
    runs,
    startedAt: started.toISOString(),
    correctness: {
      payments: {
        sent: final.payments.sent,
        confirmed: final.payments.confirmed,
        charged: charged.processed,
        repeatCharges: charged.duplicates,
        recorded: paymentSet.size,
        uncharged: unchargedP.length,
        lost: lostP,
        dead: dead.payments,
        parked: parked.payments,
      },
      ledger: { chargedNotRecorded: chargedNotRecorded.length, recordedNotCharged: recordedNotCharged.length, duplicateRows },
      refunds: {
        sent: final.refunds.sent,
        confirmed: final.refunds.confirmed,
        recorded: refundSet.size,
        unrecorded: unrecordedR.length,
        lost: lostR,
        dead: dead.refunds,
        parked: parked.refunds,
      },
      drained,
    },
    breakers: { trips, recovered, secondsFromRestoreToAllClosed: recoveredAt - ctx.restoredAt, violations, permits },
    calls,
    expected,
    series,
    suspendedMs,
    void: voided,
    pass,
  };
};

// ---- expectations ------------------------------------------------------------------

const during = (ctx) => ctx.series.filter((s) => s.t >= ctx.faultAt && s.t <= ctx.restoredAt);
const minOf = (xs) => Math.min(...xs.filter((x) => x !== null));
const replicaCount = async () => (await consumerContainers()).length;

const expectOpened = (opened, closedToo) => async (ctx) => {
  const summary = `${opened} tripped ${ctx.trips[opened]} times (want > 0), ${closedToo ?? "-"} ${closedToo ? `${ctx.trips[closedToo]} (want 0)` : ""}`;
  return { pass: ctx.trips[opened] > 0 && (closedToo === undefined || ctx.trips[closedToo] === 0), summary };
};

// ---- the faults ---------------------------------------------------------------------

const upstreamOutage = {
  inject: () => setFailure({ rate: 1 }),
  restore: () => setFailure({}),
  /** The case for per-consumer dependency lists: refunds never call the third party, so they never stop. */
  expect: async (ctx) => {
    const opened = await expectOpened(THIRD_PARTY, LEDGER)(ctx);
    const replicas = await replicaCount();
    const refundsFloor = minOf(during(ctx).map((s) => s.refundsConsumers));
    const paymentsFloor = minOf(during(ctx).map((s) => s.paymentsConsumers));
    return {
      pass: opened.pass && refundsFloor === replicas && paymentsFloor < replicas,
      summary: `${opened.summary}; refunds consumers never below ${refundsFloor}/${replicas} (want all); payments down to ${paymentsFloor}`,
    };
  },
};

/** How many new keys the third party charged between two points of the fault: payments stopped, bar probes. */
const chargedDuringLedgerOutage = async (ctx) => {
  const a = await audit(ctx.runs.payments);
  return a.processed;
};

const ledgerOutage = (inject, restore) => ({
  inject: async (ctx) => {
    await inject();
    await sleep(15_000);
    ctx.chargedAt15s = await chargedDuringLedgerOutage(ctx);
  },
  restore: async (ctx) => {
    ctx.chargedAtRestore = await chargedDuringLedgerOutage(ctx);
    await restore();
    await waitFor(async () => (await psql("SELECT 1")).stdout.trim() === "1", 60);
  },
  expect: async (ctx) => {
    const opened = await expectOpened(LEDGER, THIRD_PARTY)(ctx);
    const replicas = await replicaCount();
    const refundsFloor = minOf(during(ctx).map((s) => s.refundsConsumers));
    const paymentsFloor = minOf(during(ctx).map((s) => s.paymentsConsumers));
    const chargedWhileOpen = ctx.chargedAtRestore - ctx.chargedAt15s;
    return {
      pass: opened.pass && refundsFloor < replicas && paymentsFloor < replicas,
      summary:
        `${opened.summary}; consumers down to payments ${paymentsFloor}, refunds ${refundsFloor} of ${replicas}; ` +
        `${chargedWhileOpen} new charges from 15s into the outage to its end (probes only)`,
    };
  },
});

const dbDown = ledgerOutage(
  () => exec("docker", ["stop", POSTGRES]),
  () => exec("docker", ["start", POSTGRES]),
);
const dbCrash = ledgerOutage(
  () => exec("docker", ["kill", "-s", "KILL", POSTGRES]),
  () => exec("docker", ["start", POSTGRES]),
);
const dbHang = ledgerOutage(
  () => exec("docker", ["pause", POSTGRES]),
  () => exec("docker", ["unpause", POSTGRES]),
);

/** The consumer role's backends are terminated every 2s: the pool must reconnect, and the ledger stay exact. */
const dbConnectionsKilled = {
  inject: async (ctx) => {
    ctx.killing = true;
    ctx.killer = (async () => {
      let rounds = 0;
      while (ctx.killing) {
        await psql("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename = 'consumer'", {
          user: "postgres",
        }).catch(() => {});
        rounds++;
        await sleep(2000);
      }
      return rounds;
    })();
  },
  restore: async (ctx) => {
    ctx.killing = false;
    ctx.rounds = await ctx.killer;
  },
  expect: async (ctx) => ({
    pass: true,
    summary: `${ctx.rounds} rounds of terminated connections; ledger tripped ${ctx.trips[LEDGER]} times (informational)`,
  }),
};

/**
 * A session holds `payments` exclusively: the application's inserts wait `lock_timeout` (500ms, set on its role),
 * get `LockTimeoutError`, and read it as throttled — contention, so the payments limit shrinks and the ledger's
 * breaker stays closed. Refunds write another table and are untouched.
 */
const dbContention = {
  inject: async (ctx) => {
    ctx.locker = psql(`BEGIN; LOCK TABLE payments IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(${FAULT_S}); COMMIT;`, {
      user: "postgres",
    }).catch((e) => e);
  },
  restore: async (ctx) => {
    await ctx.locker;
  },
  expect: async (ctx) => {
    const throttled = ctx.calls[`${LEDGER}:throttled`];
    const replicas = await replicaCount();
    const refundsFloor = minOf(during(ctx).map((s) => s.refundsConsumers));
    return {
      pass: ctx.trips[LEDGER] === 0 && throttled > 0 && refundsFloor === replicas,
      summary: `${throttled} ledger answers throttled (want > 0), ledger tripped ${ctx.trips[LEDGER]} (want 0), refunds consumers never below ${refundsFloor}/${replicas}`,
    };
  },
};

/**
 * Replicas are killed mid-action while every payment insert takes 200ms: the charge has happened, the record has
 * not. The redelivery must charge again with the same key (a repeat charge at the third party, not a new one) and
 * record once.
 */
const killReplicaMidWrite = {
  inject: async () => {
    await psql(
      `CREATE OR REPLACE FUNCTION chaos_slow() RETURNS trigger AS $$ BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END $$ LANGUAGE plpgsql;
       CREATE OR REPLACE TRIGGER chaos_slow BEFORE INSERT ON payments FOR EACH ROW EXECUTE FUNCTION chaos_slow();`,
      { user: "postgres" },
    );
  },
  during: async () => {
    for (let i = 0; i < 3; i++) {
      await sleep(8000);
      const all = await consumerContainers();
      const victim = all[Math.floor(Math.random() * all.length)];
      console.log(`  killing ${victim} with payments mid-write`);
      await exec("docker", ["kill", victim]);
      await sleep(2000);
      await exec("docker", ["start", victim]);
    }
  },
  restore: async () => {
    await psql("DROP TRIGGER IF EXISTS chaos_slow ON payments; DROP FUNCTION IF EXISTS chaos_slow();", { user: "postgres" });
  },
  expect: async () => ({ pass: true, summary: "correctness and ledger exactness only" }),
};

/**
 * Both dependencies down together, the ledger restored first: once its breakers have closed (their holds have grown
 * through the outage), refunds must be consuming again while payments still wait on the third party.
 */
const bothDown = {
  inject: async () => {
    await setFailure({ rate: 1 });
    await exec("docker", ["stop", POSTGRES]);
  },
  restore: async (ctx) => {
    await exec("docker", ["start", POSTGRES]);
    await waitFor(async () => (await psql("SELECT 1")).stdout.trim() === "1", 60);
    ctx.ledgerClosed = await waitFor(async () => {
      await observe(ctx.series, ctx.t0, 1);
      return ((await breakerStates(LEDGER)) ?? [1]).every((s) => s === 0);
    }, 120);
    await observe(ctx.series, ctx.t0, 3);
    ctx.onlyThirdPartyDown = ctx.series.slice(-3);
    await setFailure({});
  },
  expect: async (ctx) => {
    const replicas = await replicaCount();
    const refundsBack = ctx.onlyThirdPartyDown.every((s) => s.refundsConsumers === replicas);
    const paymentsHeld = ctx.onlyThirdPartyDown.every((s) => s.paymentsConsumers < replicas);
    return {
      pass: ctx.trips[THIRD_PARTY] > 0 && ctx.trips[LEDGER] > 0 && ctx.ledgerClosed && refundsBack && paymentsHeld,
      summary:
        `trips ${THIRD_PARTY}=${ctx.trips[THIRD_PARTY]} ${LEDGER}=${ctx.trips[LEDGER]} (want both > 0); with only the third ` +
        `party still down: ledger closed ${ctx.ledgerClosed}, refunds consuming on every replica ${refundsBack}, payments held ${paymentsHeld}`,
    };
  },
};

const SCENARIOS = {
  "upstream-outage": upstreamOutage,
  "db-down": dbDown,
  "db-crash": dbCrash,
  "db-hang": dbHang,
  "db-connections-killed": dbConnectionsKilled,
  "db-contention": dbContention,
  "kill-replica-mid-write": killReplicaMidWrite,
  "both-down": bothDown,
};

const main = async () => {
  if (flag("list", false)) return void console.log(Object.keys(SCENARIOS).join("\n"));
  const chosen = String(flag("scenarios", Object.keys(SCENARIOS).join(","))).split(",");
  const results = [];
  try {
    for (const name of chosen) {
      results.push(await scenario(name, SCENARIOS[name]));
      // Between scenarios: everything healthy, so one cannot leave residue for the next.
      await setFailure({});
      await exec("docker", ["unpause", POSTGRES]).catch(() => {});
      await exec("docker", ["start", POSTGRES]).catch(() => {});
      await Promise.all((await consumerContainers()).map((c) => exec("docker", ["start", c]).catch(() => {})));
      await waitFor(allClosed, 120);
    }
  } finally {
    await setFailure({});
    await Promise.all(PRODUCERS.map((p) => exec("docker", ["start", p]).catch(() => {})));
    mkdirSync(dirname(OUT), { recursive: true });
    writeFileSync(OUT, JSON.stringify({ at: new Date().toISOString(), format: FORMAT, results }, null, 2));
    console.log(`\nwrote ${OUT}`);
  }
  const graded = results.filter((r) => !r.void);
  console.log(`\n${graded.filter((r) => r.pass).length}/${graded.length} graded scenarios passed`);
  process.exitCode = graded.every((r) => r.pass) ? 0 : 1;
};

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
