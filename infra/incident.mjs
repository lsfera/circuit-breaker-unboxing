// Drives one incident against the fleet and reports what happened, including whether the five in-process breakers
// agree with each other about the same third party: they share nothing (see packages/rmq-consumer/src/Breaker.ts), so
// this measures the disagreement instead of asserting it.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # the one that grows the work queue
//   STATUS=422 node infra/incident.mjs                 # a 4xx: Breaker.ts's classify calls it client_error,
//                                                       # discarded at once — no trip, no backlog
//   CAPACITY=5 DELAY_MS=100 node infra/incident.mjs    # full, not broken: 5 at once (50/s), 429 beyond. Needs
//                                                       # RATE_PER_SECOND above that ceiling
//
// Assumes `docker compose up -d` is already running.
import { createRequire } from "node:module";

const BROKER = process.env.BROKER ?? "amqp://guest:guest@localhost:5672";
const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM ?? "http://localhost:8080";
const PROMETHEUS = process.env.PROMETHEUS ?? "http://localhost:9090";
const API_ID = process.env.API_ID ?? "payments-provider";
const RATE = Number(process.env.RATE ?? "1.0");
// With the default "error" mode a failed call answers almost instantly, so five daemons at maxInFlight=20 clear a
// 200/s arrival rate as fast as it fails and only the dead-letter queue climbs. "hang" holds every call for the
// full 2s client timeout, which pins all 100 in-flight slots and starves the drain below the arrival rate, so the
// backlog itself grows.
const MODE = process.env.MODE; // unset = "error" (flaky-upstream's default)
const STATUS = process.env.STATUS ? Number(process.env.STATUS) : undefined; // unset = 503 (flaky-upstream's default)
// A third party with a ceiling instead of a failure rate: at most CAPACITY in flight, each taking DELAY_MS.
const CAPACITY = Number(process.env.CAPACITY ?? "0");
const DELAY_MS = Number(process.env.DELAY_MS ?? "0");
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "20000");
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? "60000");
const RECOVERY_TIMEOUT_MS = Number(process.env.RECOVERY_TIMEOUT_MS ?? "90000");
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

/** Attempts by outcome so far, fleet-wide: `failed` reached the third party and counts toward tripping; `client_error` also reached it but is the request's fault, not counted, and dead-lettered at once. An open breaker consumes nothing, so neither is ever "turned away" the way the in-process breaker's rejections are. */
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
    body: JSON.stringify(
      rate === 0
        ? {}
        : CAPACITY > 0
          ? { capacity: CAPACITY, delayMs: DELAY_MS }
          : { rate, ...(MODE ? { mode: MODE } : {}), ...(STATUS ? { status: STATUS } : {}) },
    ),
  });

/** One Prometheus reading, summed; `undefined` when there is none yet. */
const scalar = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).catch(() => undefined);
  const body = res?.ok ? await res.json() : undefined;
  const value = body?.data?.result?.[0]?.value?.[1];
  return value === undefined ? undefined : Number(value);
};

const audit = async (run) => {
  const res = await fetch(`${FLAKY_UPSTREAM}/__audit?run=${encodeURIComponent(run)}`);
  return res.ok ? res.json() : undefined;
};

const workQueue = `${API_ID}.work`;
const deadQueue = `${API_ID}.work.dead`;

const report = (label, depth) =>
  console.log(`  ${label}: ready=${depth.ready}`);

/** Agreement bookkeeping, shared across the incident window and the drain: divergence during recovery is the more interesting half. */
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
  console.log(
    `\n== Injecting failure: rate=${RATE} mode=${MODE ?? "error"}${STATUS ? ` status=${STATUS}` : ""} for ${WINDOW_MS}ms ==`,
  );
  const auditAtStart = await audit("*");
  await setFailure(RATE);

  let peakBacklog = 0;
  const limits = [];
  const fleetOpen = { open: 0, total: 0 };
  const started = Date.now();
  while (Date.now() - started < WINDOW_MS) {
    await sleep(POLL_MS);
    const [limit, open] = await Promise.all([
      scalar("sum(egress_consumer_concurrency_limit)"),
      scalar("egress:fleet_open"),
    ]);
    if (limit !== undefined) limits.push(limit);
    if (open !== undefined) (fleetOpen.total++, (fleetOpen.open += open));
    const work = await queueDepth(workQueue);
    const dead = await queueDepth(deadQueue);
    peakBacklog = Math.max(peakBacklog, work.ready);
    const t = `t+${Math.round((Date.now() - started) / 1000)}s`;
    console.log(`  ${t}  work=${work.ready} dead=${dead.ready}`);
    await pollBreakers(t);
  }

  const auditAtEnd = await audit("*");
  const faultMs = Date.now() - started;
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

  // The queue is empty long before the breakers are: each replica closes on its own half-open clock, and that
  // recovery is where they disagree most, so observation continues until every breaker has closed.
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
      `  attempts during the incident: ${delta("ok")} ok, ${delta("failed")} reached the third party and failed, ` +
        `${delta("throttled")} answered 429 (slow down), ` +
        `${delta("client_error")} refused (dead-lettered at once, not counted toward tripping)`,
    );
  }
  if (auditAtStart?.processed !== undefined && auditAtEnd?.processed !== undefined) {
    // The third party's own count of distinct messages it answered 200: exact, where a counter lags a scrape.
    const ceiling = CAPACITY > 0 && DELAY_MS > 0 ? ` (the third party's ceiling is ${Math.round((CAPACITY * 1000) / DELAY_MS)}/s)` : "";
    console.log(
      `  goodput while the fault was on: ${((auditAtEnd.processed - auditAtStart.processed) / (faultMs / 1000)).toFixed(0)} successful calls/s${ceiling}`,
    );
  }
  if (limits.length > 0) {
    const tail = limits.slice(-Math.max(1, Math.floor(limits.length / 2)));
    console.log(
      `  fleet concurrency limit (sum over replicas): ${Math.min(...limits)} at least, ` +
        `${(tail.reduce((a, b) => a + b, 0) / tail.length).toFixed(1)} on average over the later half of the fault`,
    );
  }
  if (fleetOpen.total > 0) console.log(`  fleet open on ${fleetOpen.open}/${fleetOpen.total} polled ticks`);
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
