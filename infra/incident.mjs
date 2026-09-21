// Drives one incident against the base scenario and reports what happened: how big the backlog gets with nothing
// slowing arrivals down, how much gets dead-lettered, and how long a full recovery takes once the third party comes
// back on its own.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # the one that grows the work queue
//
// Assumes `docker compose up -d` is already running.
import { createRequire } from "node:module";

const BROKER = process.env.BROKER ?? "amqp://guest:guest@localhost:5672";
const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM ?? "http://localhost:8080";
const API_ID = process.env.API_ID ?? "payments-provider";
const RATE = Number(process.env.RATE ?? "1.0");
// With the default "error" mode a failed call answers almost instantly, so five daemons at maxInFlight=20 clear a
// 200/s arrival rate as fast as it fails and only the dead-letter queue climbs. "hang" holds every call for the
// full 2s client timeout, which pins all 100 in-flight slots and starves the drain below the arrival rate, so the
// backlog itself grows.
const MODE = process.env.MODE; // unset = "error" (flaky-upstream's default)
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "20000");
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? "60000");
const POLL_MS = 1000;
const DRAIN_POLL_MS = 200;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Depth comes from the broker itself, over AMQP, not from the management API: its queue figures refresh only every
// 5s, so a read at the moment the outage ends can be seconds stale. A passive declare answers from the queue's own
// state; it counts ready messages only, and the unacked few (at most a prefetch window) finish within a round trip
// of the third party coming back.
const amqp = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("amqplib");
const connection = await amqp.connect(BROKER);
const channel = await connection.createChannel();
const queueDepth = async (name) => ({ ready: (await channel.checkQueue(name)).messageCount });

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
  console.log(`  ${label}: ready=${depth.ready}`);

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
    peakBacklog = Math.max(peakBacklog, work.ready);
    console.log(
      `  t+${Math.round((Date.now() - started) / 1000)}s  work=${work.ready} dead=${dead.ready}`,
    );
  }

  console.log(`\n== Restoring ==`);
  await setFailure(0);
  const restoredAt = Date.now();

  let work = await queueDepth(workQueue);
  while (work.ready > 0 && Date.now() - restoredAt < DRAIN_TIMEOUT_MS) {
    await sleep(DRAIN_POLL_MS);
    work = await queueDepth(workQueue);
  }
  const drainMs = Date.now() - restoredAt;
  const drained = work.ready === 0;

  const dead = await queueDepth(deadQueue);
  const stats = await audit("*"); // every producer run: a message id is `<run>:<n>`

  console.log(`\n== Summary ==`);
  console.log(`  peak backlog (work queue, ready): ${peakBacklog}`);
  console.log(`  dead-lettered: ${dead.ready}`);
  console.log(
    drained
      ? `  drained to 0 in ${(drainMs / 1000).toFixed(1)}s after restore`
      : `  did NOT drain within ${DRAIN_TIMEOUT_MS}ms (${work.ready} left)`,
  );
  if (stats) {
    console.log(
      `  audit: processed=${stats.processed ?? "?"} duplicates=${stats.duplicates ?? "?"}`,
    );
  }
};

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => connection.close());
