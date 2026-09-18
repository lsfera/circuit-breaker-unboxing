// Drives one incident against the base scenario and reports what actually
// happened — no aggregator, no circuit state, so there is nothing to assert
// about a breaker. What there is to measure: how big the backlog gets with
// nothing slowing arrivals down, how much gets dead-lettered, and how long a
// full recovery takes once the third party comes back on its own. These are
// the numbers the next article's "what a breaker buys you" comparison cites.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # this is the one that grows the work queue
//
// Assumes `docker compose up -d` is already running.

const RABBITMQ_MGMT = process.env.RABBITMQ_MGMT ?? "http://localhost:15672";
const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM ?? "http://localhost:8080";
const API_ID = process.env.API_ID ?? "payments-provider";
const RATE = Number(process.env.RATE ?? "1.0");
// Measured, not assumed: with the default "error" mode a failed call answers
// almost instantly, so five daemons at maxInFlight=20 clear a 200/s arrival
// rate as fast as it fails — the work queue never visibly backs up, and the
// only symptom is the dead-letter queue climbing. "hang" holds every call
// for the full 2s client timeout instead, which pins all 100 in-flight slots
// and starves the queue's actual drain rate below the arrival rate — that is
// what makes the backlog itself grow. Same lack of a breaker either way; two
// different, both real, symptoms.
const MODE = process.env.MODE; // unset = "error" (flaky-upstream's default)
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "20000");
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? "60000");
const POLL_MS = 1000;

const auth = "Basic " + Buffer.from("guest:guest").toString("base64");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const queueDepth = async (name) => {
  const res = await fetch(`${RABBITMQ_MGMT}/api/queues/%2F/${encodeURIComponent(name)}`, {
    headers: { Authorization: auth },
  });
  if (!res.ok) throw new Error(`queue ${name}: ${res.status}`);
  const body = await res.json();
  return {
    ready: body.messages_ready ?? 0,
    unacked: body.messages_unacknowledged ?? 0,
    total: body.messages ?? 0,
  };
};

const setFailure = (rate) =>
  fetch(`${FLAKY_UPSTREAM}/__fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(rate === 0 ? {} : { rate, ...(MODE ? { mode: MODE } : {}) }),
  });

const audit = async (run) => {
  const res = await fetch(`${FLAKY_UPSTREAM}/__audit?run=${encodeURIComponent(run)}`);
  return res.ok ? res.json() : undefined;
};

const workQueue = `${API_ID}.work`;
const deadQueue = `${API_ID}.work.dead`;

const report = (label, depth) =>
  console.log(`  ${label}: ready=${depth.ready} unacked=${depth.unacked} total=${depth.total}`);

const main = async () => {
  console.log(`== Steady state ==`);
  report(workQueue, await queueDepth(workQueue));
  report(deadQueue, await queueDepth(deadQueue));

  console.log(`\n== Injecting failure: rate=${RATE} mode=${MODE ?? "error"} for ${WINDOW_MS}ms ==`);
  await setFailure(RATE);

  let peakBacklog = 0;
  const started = Date.now();
  while (Date.now() - started < WINDOW_MS) {
    await sleep(POLL_MS);
    const work = await queueDepth(workQueue);
    const dead = await queueDepth(deadQueue);
    peakBacklog = Math.max(peakBacklog, work.total);
    console.log(
      `  t+${Math.round((Date.now() - started) / 1000)}s  work=${work.total} dead=${dead.total}`,
    );
  }

  console.log(`\n== Restoring ==`);
  await setFailure(0);
  const restoredAt = Date.now();

  let work = await queueDepth(workQueue);
  while (work.total > 0 && Date.now() - restoredAt < DRAIN_TIMEOUT_MS) {
    await sleep(POLL_MS);
    work = await queueDepth(workQueue);
  }
  const drainMs = Date.now() - restoredAt;
  const drained = work.total === 0;

  const dead = await queueDepth(deadQueue);
  const stats = await audit(API_ID);

  console.log(`\n== Summary ==`);
  console.log(`  peak backlog (work queue): ${peakBacklog}`);
  console.log(`  dead-lettered: ${dead.total}`);
  console.log(
    drained
      ? `  drained to 0 in ${(drainMs / 1000).toFixed(1)}s after restore`
      : `  did NOT drain within ${DRAIN_TIMEOUT_MS}ms (${work.total} left)`,
  );
  if (stats) {
    console.log(
      `  audit: processed=${stats.processed ?? "?"} duplicates=${stats.duplicates ?? "?"}`,
    );
  }
};

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
