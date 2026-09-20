// Drives one incident against the fleet and reports what actually happened —
// including whether five independent, in-process breakers agree with each
// other about the same third party. They share nothing (see
// packages/consumer/src/Breaker.ts), so this is the measured version of the
// article series' own claim that per-process breakers disagree, produced by
// this branch's own run rather than quoted from elsewhere.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # this is the one that grows the work queue
//
// Assumes `docker compose up -d` is already running.
import { createRequire } from "node:module";

const BROKER = process.env.BROKER ?? "amqp://guest:guest@localhost:5672";
const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM ?? "http://localhost:8080";
const PROMETHEUS = process.env.PROMETHEUS ?? "http://localhost:9090";
const API_ID = process.env.API_ID ?? "payments-provider";
const RATE = Number(process.env.RATE ?? "1.0");
// Measured, not assumed: with the default "error" mode a failed call answers
// almost instantly, so five daemons at maxInFlight=20 clear a 200/s arrival
// rate as fast as it fails — the work queue never visibly backs up, and the
// only symptom is the dead-letter queue climbing. "hang" holds every call
// for the full 2s client timeout instead, which pins all 100 in-flight slots
// and starves the queue's actual drain rate below the arrival rate — that is
// what makes the backlog itself grow.
const MODE = process.env.MODE; // unset = "error" (flaky-upstream's default)
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "20000");
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? "60000");
const RECOVERY_TIMEOUT_MS = Number(process.env.RECOVERY_TIMEOUT_MS ?? "90000");
const POLL_MS = 1000;
const DRAIN_POLL_MS = 200;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Depth comes from the broker itself, over AMQP, not from the management API:
// the management API's queue figures are refreshed every 5s, so a read taken
// at the moment the outage ends can be seconds stale — an incident that
// dead-lettered 4,000 messages read as 2,200, and every drain read as 0.0s.
// A passive declare answers from the queue's own state. It counts ready
// messages only; the unacked few (at most a prefetch window) finish within a
// round trip of the third party coming back.
const amqp = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("amqplib");
const connection = await amqp.connect(BROKER);
const channel = await connection.createChannel();
const queueDepth = async (name) => ({ ready: (await channel.checkQueue(name)).messageCount });

const STATE_NAME = ["CLOSED", "OPEN", "HALF_OPEN"];

/** Breaker openings so far, fleet-wide: `onBreak` fires once per replica per opening, not once per incident. */
const breakerTrips = async () => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=sum(egress_consumer_breaker_trips_total)`).catch(
    () => undefined,
  );
  const body = res?.ok ? await res.json() : undefined;
  const value = body?.data?.result?.[0]?.value?.[1];
  return value === undefined ? undefined : Number(value);
};

/** Attempts by outcome so far, fleet-wide: `failed` reached the third party — an open breaker consumes nothing, so there is no third outcome. */
const callsByOutcome = async () => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=sum by (outcome)(egress_consumer_calls_total)`).catch(
    () => undefined,
  );
  const body = res?.ok ? await res.json() : undefined;
  return body?.data?.result
    ? Object.fromEntries(body.data.result.map((r) => [r.metric.outcome, Number(r.value[1])]))
    : undefined;
};

/** One reading per replica, or `undefined` if Prometheus isn't reachable — never fails the run over it. */
const breakerStates = async () => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=egress_consumer_breaker_state`).catch(
    () => undefined,
  );
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success") return undefined;
  return body.data.result
    .map((r) => ({ instance: r.metric.instance, state: Number(r.value[1]) }))
    .sort((a, b) => a.instance.localeCompare(b.instance));
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
  console.log(`  ${label}: ready=${depth.ready}`);

/** Agreement bookkeeping, shared across the incident window and the drain — divergence during recovery (each replica's own half-open probe, on its own clock) is the more interesting half. */
const agreement = { ticksWithData: 0, ticksAgreed: 0, peakOpen: 0, replicaCount: 0 };

const pollBreakers = async (label) => {
  const states = await breakerStates();
  if (!states || states.length === 0) return states;
  agreement.ticksWithData++;
  agreement.replicaCount = Math.max(agreement.replicaCount, states.length);
  const distinct = new Set(states.map((s) => s.state)).size;
  if (distinct === 1) agreement.ticksAgreed++;
  const openCount = states.filter((s) => s.state === 1).length;
  agreement.peakOpen = Math.max(agreement.peakOpen, openCount);
  const summary = states.map((s) => STATE_NAME[s.state] ?? s.state).join(",");
  console.log(`  ${label} breakers: [${summary}]`);
  return states;
};

const main = async () => {
  console.log(`== Steady state ==`);
  report(workQueue, await queueDepth(workQueue));
  report(deadQueue, await queueDepth(deadQueue));
  await pollBreakers("t+0s");

  const tripsBefore = await breakerTrips();
  const callsBefore = await callsByOutcome();
  console.log(`\n== Injecting failure: rate=${RATE} mode=${MODE ?? "error"} for ${WINDOW_MS}ms ==`);
  await setFailure(RATE);

  let peakBacklog = 0;
  const started = Date.now();
  while (Date.now() - started < WINDOW_MS) {
    await sleep(POLL_MS);
    const work = await queueDepth(workQueue);
    const dead = await queueDepth(deadQueue);
    peakBacklog = Math.max(peakBacklog, work.ready);
    const t = `t+${Math.round((Date.now() - started) / 1000)}s`;
    console.log(`  ${t}  work=${work.ready} dead=${dead.ready}`);
    await pollBreakers(t);
  }

  console.log(`\n== Restoring ==`);
  await setFailure(0);
  const restoredAt = Date.now();

  let work = await queueDepth(workQueue);
  while (work.ready > 0 && Date.now() - restoredAt < DRAIN_TIMEOUT_MS) {
    await sleep(DRAIN_POLL_MS);
    work = await queueDepth(workQueue);
    await pollBreakers(`+${Math.round((Date.now() - restoredAt) / 1000)}s`);
  }
  const drainMs = Date.now() - restoredAt;
  const drained = work.ready === 0;

  // The queue is empty long before the breakers are: each replica closes on
  // its own half-open clock, and that recovery is the half of the incident
  // where they disagree most. Stopping the observation at the drain would
  // measure agreement over a window that mostly excludes it.
  let states = await pollBreakers(`+${Math.round((Date.now() - restoredAt) / 1000)}s`);
  while (states?.some((s) => s.state !== 0) && Date.now() - restoredAt < RECOVERY_TIMEOUT_MS) {
    await sleep(POLL_MS);
    states = await pollBreakers(`+${Math.round((Date.now() - restoredAt) / 1000)}s`);
  }
  const recoveredMs = Date.now() - restoredAt;
  await sleep(2500); // one scrape interval, so the counter has caught up
  const tripsAfter = await breakerTrips();
  const callsAfter = await callsByOutcome();
  const recovered = states?.every((s) => s.state === 0);

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
  if (tripsAfter !== undefined) {
    // A counter has no series until its first increment, so no reading before means none yet.
    console.log(`  breaker openings across the fleet: ${tripsAfter - (tripsBefore ?? 0)}`);
  }
  if (callsBefore && callsAfter) {
    const delta = (outcome) => (callsAfter[outcome] ?? 0) - (callsBefore[outcome] ?? 0);
    console.log(
      `  attempts during the incident: ${delta("ok")} ok, ${delta("failed")} reached the third party and failed`,
    );
  }
  if (states) {
    console.log(
      recovered
        ? `  every breaker CLOSED ${(recoveredMs / 1000).toFixed(1)}s after restore`
        : `  breakers still not all CLOSED ${(recoveredMs / 1000).toFixed(1)}s after restore`,
    );
  }
  if (agreement.ticksWithData > 0) {
    const pct = Math.round((100 * agreement.ticksAgreed) / agreement.ticksWithData);
    console.log(
      `  breaker agreement: ${agreement.ticksAgreed}/${agreement.ticksWithData} ticks (${pct}%) had every replica in the same state`,
    );
    console.log(`  peak replicas OPEN at once: ${agreement.peakOpen} of ${agreement.replicaCount}`);
  } else {
    console.log(`  breaker agreement: no data — is Prometheus reachable at ${PROMETHEUS}?`);
  }
};

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => connection.close());
