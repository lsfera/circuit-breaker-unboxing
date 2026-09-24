// Chaos under load: process and flaky-service faults, injected one at a time
// against a live fleet under sustained traffic, each judged first on
// correctness (no confirmed message ever goes missing) and only then on
// whether the breakers behaved the way the reports in this
// series claim they do.
//
//   node infra/chaos-load.mjs --list
//   node infra/chaos-load.mjs                                   # every fault
//   node infra/chaos-load.mjs --faults=kill-one-consumer
//   node infra/chaos-load.mjs --faults=kill-all-consumers,flaky-storm --fault-seconds=30
//
// All traffic comes from one forked publisher (chaos-publisher.mjs) — the
// compose producer is stopped for the run's duration so every idempotency
// key comes from one source. Assumes `docker compose up -d` is already
// running.
//
// Correctness bar, adjusted from this series' own findings rather than
// copied from master's harness unchanged: master's bar was "no message
// lost, dead-letter queue empty once the scenario drains." Article 3's own
// report showed redrive can legitimately stall for tens of seconds with a
// harmless backlog sitting in work.dead until an unrelated breaker
// transition fires the next trigger — so an empty dead-letter queue is not
// a safe pass/fail signal on its own. What this harness checks instead:
// every message the broker confirmed is either processed by the upstream
// (flaky-upstream's own audit) or still physically sitting in work,
// work.dead, or work.parked. Anything neither is a real loss.

import { execFile, fork } from "node:child_process";
import { promisify } from "node:util";
import { mkdirSync, writeFileSync } from "node:fs";

const exec = promisify(execFile);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const hit = args.find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return fallback;
  return hit.includes("=") ? hit.slice(name.length + 3) : true;
};

const PROJECT = process.env.COMPOSE_PROJECT_NAME ?? "workspace";
const RABBITMQ_MGMT = process.env.RABBITMQ_MGMT ?? "http://localhost:15672";
const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM ?? "http://localhost:8080";
const PROMETHEUS = process.env.PROMETHEUS ?? "http://localhost:9090";
const AMQP_URL = process.env.AMQP_URL ?? "amqp://guest:guest@rabbitmq:5672";
const API_ID = "payments-provider";
const WORK = `${API_ID}.work`;
const DEAD = `${API_ID}.work.dead`;
const PARKED = `${API_ID}.work.parked`;

const RATE = Number(flag("rate", 200));
const SPIKE = Number(flag("spike", 1000));
const FAULT_SECONDS = Number(flag("fault-seconds", 20));
const SETTLE_TIMEOUT_MS = Number(flag("settle-timeout", 60_000));
const BASELINE_WARMUP_MS = 3000;
const OUT = flag("out", `history/runs/chaos-load-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

const auth = "Basic " + Buffer.from("guest:guest").toString("base64");

// ---- reading the stack ------------------------------------------------

// `undefined` on any failure, matching `breakerStates` below —
// a `kill-broker` fault means this endpoint is briefly unreachable by
// design, not a harness bug, and the settle loop below already treats
// "no fresh answer this tick" as "not settled yet."
const queueDepth = async (name) => {
  const res = await fetch(`${RABBITMQ_MGMT}/api/queues/%2F/${encodeURIComponent(name)}`, {
    headers: { Authorization: auth },
  }).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json().catch(() => undefined);
  if (!body) return undefined;
  return { ready: body.messages_ready ?? 0, unacked: body.messages_unacknowledged ?? 0, total: body.messages ?? 0 };
};

/** Retries until the management API answers, for the few reads that need a real number rather than "not yet" — the loop already tolerates absence, these don't. */
const queueDepthReady = async (name, retries = 10, delayMs = 1000) => {
  for (let i = 0; i < retries; i++) {
    const d = await queueDepth(name);
    if (d) return d;
    await sleep(delayMs);
  }
  throw new Error(`queue ${name}: management API unreachable after ${retries} retries`);
};

const purgeQueue = (name) =>
  fetch(`${RABBITMQ_MGMT}/api/queues/%2F/${encodeURIComponent(name)}/contents`, {
    method: "DELETE",
    headers: { Authorization: auth },
  });

const STATE_NAME = ["CLOSED", "OPEN", "HALF_OPEN"];
const breakerStates = async () => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=egress_consumer_breaker_state`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success") return undefined;
  return body.data.result.map((r) => ({ instance: r.metric.instance, state: Number(r.value[1]) }));
};

const setFailure = (rate, mode) =>
  fetch(`${FLAKY_UPSTREAM}/__fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(rate === 0 ? {} : { rate, ...(mode ? { mode } : {}) }),
  });

// A consumer forwards each message's AMQP message_id, `<runId>:<n>` as chaos-publisher.mjs stamps it, as the
// idempotency key, so flaky-upstream's audit bucket for a run holds exactly that run's messages.
const audit = async (runId) => {
  const res = await fetch(`${FLAKY_UPSTREAM}/__audit?run=${encodeURIComponent(runId)}`);
  return res.ok ? res.json() : undefined;
};

// ---- docker -------------------------------------------------------------

const dockerNames = async (namePattern) => {
  const { stdout } = await exec("docker", ["ps", "--format", "{{.Names}}", "--filter", `name=${namePattern}`]);
  return stdout.split("\n").map((s) => s.trim()).filter(Boolean);
};
const dockerKill = (name) => exec("docker", ["kill", name]).catch((err) => {
  throw new Error(`docker kill ${name}: ${err.message}`);
});

/**
 * `docker kill` then an explicit `docker start` — never left to `restart:
 * unless-stopped` alone. Measured live in this environment: killing a
 * container leaves it dead indefinitely (`RestartCount` stuck at 0, still
 * `exited` 30+ seconds later) — the restart policy simply never fires here,
 * a Docker Desktop devcontainer quirk unrelated to anything this repo's own
 * code does. A real orchestrator (Kubernetes, ECS, Swarm on a real host)
 * restarts a crashed container reliably and fast; simulating that
 * explicitly is what actually exercises "does this component survive a
 * hard replica restart," which is the point of these faults — not "does
 * this specific devcontainer's restart-policy daemon work."
 */
const dockerKillAndRestart = async (name) => {
  await dockerKill(name);
  await exec("docker", ["start", name]);
};

// ---- bitmap accounting ----------------------------------------------------

/** Bits set in `a` but not in `b`, treating either buffer as zero-padded past its own length. */
const countSetNotIn = (a, b) => {
  const len = Math.max(a.length, b.length);
  let count = 0;
  for (let i = 0; i < len; i++) {
    const av = a[i] ?? 0;
    const bv = b[i] ?? 0;
    let bits = av & ~bv;
    while (bits) {
      count += bits & 1;
      bits >>= 1;
    }
  }
  return count;
};

// ---- fault catalog ----------------------------------------------------

const FAULTS = {
  "kill-one-consumer": {
    description: "SIGKILLs one random consumer replica mid-spike, then explicitly restarts it",
    async run() {
      const names = await dockerNames(`${PROJECT}-rmq-consumer-`);
      if (names.length === 0) throw new Error("no rmq-consumer containers found");
      const victim = names[Math.floor(Math.random() * names.length)];
      console.log(`    killing ${victim}`);
      await dockerKillAndRestart(victim);
    },
  },
  "kill-all-consumers": {
    description: "SIGKILLs every consumer replica at once, then explicitly restarts each — a cold restart of the whole fleet under load",
    async run() {
      const names = await dockerNames(`${PROJECT}-rmq-consumer-`);
      if (names.length === 0) throw new Error("no rmq-consumer containers found");
      console.log(`    killing ${names.join(", ")}`);
      await Promise.all(names.map(dockerKillAndRestart));
    },
  },
  "kill-broker": {
    description: "SIGKILLs the RabbitMQ broker itself, then explicitly restarts it and waits for the management API — master's own decisive ADR-016 fault",
    async run() {
      const names = await dockerNames(`${PROJECT}-rabbitmq-`);
      if (names.length === 0) throw new Error("no rabbitmq container found");
      console.log(`    killing ${names.join(", ")}`);
      await Promise.all(names.map(dockerKillAndRestart));
      console.log(`    waiting for the management API to answer again...`);
      await queueDepthReady(WORK, 30, 1000);
    },
  },
  "flaky-storm": {
    description: "cycles flaky-upstream through error -> hang -> reset -> healthy under one spike",
    async run({ faultSeconds }) {
      const steps = [{ mode: undefined }, { mode: "hang" }, { mode: "reset" }];
      const stepMs = Math.max(1000, Math.floor((faultSeconds * 1000) / steps.length));
      for (const step of steps) {
        console.log(`    flaky-upstream mode=${step.mode ?? "error"}`);
        await setFailure(1, step.mode);
        await sleep(stepMs);
      }
      await setFailure(0);
    },
    // flaky-storm already restores health itself mid-`run`, so the harness's
    // own post-fault `setFailure(0)` below is a harmless repeat, not a
    // second distinct step — kept anyway so every fault leaves the upstream
    // healthy the same way, whether or not its own `run` already did.
  },
};

const printCatalog = () => {
  console.log("Available faults:");
  for (const [name, f] of Object.entries(FAULTS)) console.log(`  ${name.padEnd(20)} ${f.description}`);
};

// ---- one fault's full lifecycle ----------------------------------------

const runOneFault = async (name) => {
  const fault = FAULTS[name];
  const runId = `chaos-${name}-${Date.now()}`;
  console.log(`\n== ${name}: ${fault.description} ==`);

  await Promise.all([purgeQueue(WORK), purgeQueue(DEAD), purgeQueue(PARKED)]);
  await setFailure(0); // start from a healthy upstream regardless of the previous fault

  const publisherPath = new URL("./chaos-publisher.mjs", import.meta.url);
  const publisher = fork(publisherPath, [], {
    env: { ...process.env, RUN_ID: runId, QUEUE: WORK, API_ID, AMQP_URL },
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  let finalStats;
  publisher.on("message", (msg) => {
    if (msg.type === "final") finalStats = msg;
  });

  publisher.send({ type: "rate", rate: RATE });
  await sleep(BASELINE_WARMUP_MS);

  console.log(`  injecting fault at spike rate (${SPIKE}/s)...`);
  publisher.send({ type: "rate", rate: SPIKE });
  const faultStarted = Date.now();
  await fault.run({ faultSeconds: FAULT_SECONDS });
  const remaining = FAULT_SECONDS * 1000 - (Date.now() - faultStarted);
  if (remaining > 0) await sleep(remaining);

  // Rate to 0, not back to baseline: with the publisher still producing,
  // the queue can legitimately never hit zero at a rate near the fleet's
  // own throughput ceiling — measured live, "still queued: 5" at rate=500
  // that never budged in 90s across every fault, purely because inflow
  // never stopped. Cutting inflow is what makes "how long to fully drain"
  // a real, bounded number instead of an artifact of still-running traffic.
  console.log(`  fault cleared, stopping new traffic and watching the backlog fully drain...`);
  publisher.send({ type: "rate", rate: 0 });
  await setFailure(0); // in case the fault itself left the upstream unhealthy

  const settleStart = Date.now();
  // No fallback value: a queue read failing before the loop's first tick
  // (plausible right after a `kill-broker` fault) must read as "not settled,"
  // never as an empty queue — `work.total === 0` below only evaluates once
  // `work` is defined.
  let work;
  let breakersClosed = false;
  let sawOpenBreaker = false;
  while (Date.now() - settleStart < SETTLE_TIMEOUT_MS) {
    const states = await breakerStates();
    if (states?.some((s) => s.state === 1)) sawOpenBreaker = true;
    breakersClosed = states !== undefined && states.length > 0 && states.every((s) => s.state === 0);
    work = (await queueDepth(WORK)) ?? work;
    if (work?.total === 0 && breakersClosed) break;
    await sleep(1000);
  }
  const settleMs = Date.now() - settleStart;
  work ??= { ready: 0, unacked: 0, total: -1 }; // -1: never got a real read, distinguishable from a genuine 0

  publisher.send({ type: "stop" });
  await new Promise((resolve) => publisher.on("exit", resolve));

  const dead = await queueDepthReady(DEAD);
  const parked = await queueDepthReady(PARKED);
  const auditResult = await audit(runId);

  const confirmedBits = finalStats ? Buffer.from(finalStats.bits, "base64") : Buffer.alloc(0);
  const processedBits = auditResult?.bits ? Buffer.from(auditResult.bits, "base64") : Buffer.alloc(0);
  const confirmedNotProcessed = countSetNotIn(confirmedBits, processedBits);
  const stillQueued = work.total + dead.total + parked.total;
  const unaccounted = confirmedNotProcessed - stillQueued;
  const pass = unaccounted <= 0;

  const result = {
    fault: name,
    runId,
    pass,
    faultSeconds: FAULT_SECONDS,
    rate: RATE,
    spike: SPIKE,
    sent: finalStats?.sent ?? 0,
    confirmed: finalStats?.confirmed ?? 0,
    nacked: finalStats?.nacked ?? 0,
    returned: finalStats?.returned ?? 0,
    processed: auditResult?.processed ?? 0,
    duplicates: auditResult?.duplicates ?? 0,
    confirmedNotYetProcessed: confirmedNotProcessed,
    stillQueued,
    stillInWork: work.total,
    stillInDead: dead.total,
    stillInParked: parked.total,
    unaccounted: Math.max(0, unaccounted),
    settledWithinTimeout: work.total === 0 && breakersClosed,
    settleMs,
    sawOpenBreaker,
  };

  console.log(
    `  ${pass ? "PASS" : "FAIL"} — confirmed=${result.confirmed} processed=${result.processed} ` +
      `unaccounted=${result.unaccounted} (still queued: work=${work.total} dead=${dead.total} parked=${parked.total})`,
  );
  console.log(
    `  settled ${result.settledWithinTimeout ? "within" : "NOT within"} ${SETTLE_TIMEOUT_MS}ms ` +
      `(${(settleMs / 1000).toFixed(1)}s) — breaker open seen: ${sawOpenBreaker}`,
  );
  if (!pass) {
    console.error(
      `  !! ${result.unaccounted} confirmed message(s) are neither processed nor sitting in work/dead/parked — genuine loss`,
    );
  }
  return result;
};

const main = async () => {
  mkdirSync("history/runs", { recursive: true });

  if (flag("list", false)) return printCatalog();

  const requested = flag("faults", undefined);
  const names = requested ? String(requested).split(",") : Object.keys(FAULTS);
  for (const n of names) {
    if (!FAULTS[n]) {
      console.error(`Unknown fault "${n}". Run --list for the catalog.`);
      process.exitCode = 1;
      return;
    }
  }

  console.log(`Stopping rmq-producer for the duration of this run (${names.length} fault(s))...`);
  await exec("docker", ["compose", "stop", "rmq-producer"]);

  const results = [];
  try {
    for (const name of names) {
      const result = await runOneFault(name);
      results.push(result);
      if (!result.pass) {
        console.error(`\nStopping the matrix: "${name}" failed the no-loss bar. Fix before running the rest.`);
        break;
      }
    }
  } finally {
    console.log("\nRestarting rmq-producer...");
    await exec("docker", ["compose", "start", "rmq-producer"]);
  }

  writeFileSync(OUT, JSON.stringify(results, null, 2));
  console.log(`\nWrote ${OUT}`);

  const failed = results.filter((r) => !r.pass);
  const ranAll = results.length === names.length;
  if (failed.length > 0) {
    console.error(`\n${failed.length}/${results.length} fault(s) FAILED the no-loss bar.`);
    process.exitCode = 1;
  } else if (!ranAll) {
    console.error(`\nStopped early after ${results.length}/${names.length} fault(s).`);
    process.exitCode = 1;
  } else {
    console.log(`\nAll ${results.length} fault(s) passed.`);
  }
};

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
