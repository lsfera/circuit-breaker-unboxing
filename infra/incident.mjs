// Drives one incident against the fleet and reports what actually happened —
// including whether five independent, in-process breakers agree with each
// other about the same third party (they share nothing, see
// packages/consumer/src/Breaker.ts) and, since article 4, whether the
// messages this incident dead-lettered actually come back once the elected
// redriver's own breaker closes (packages/consumer/src/Redrive.ts) —
// measured, not asserted, same as everything else here.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # this is the one that grows the work queue
//   STATUS=422 node infra/incident.mjs                 # only a 4xx incident, nothing else appended
//
// A default run (STATUS unset) also appends a short client_error (422) phase after the outage and its
// redrive — otherwise every run only ever shows a 503, and never the fourth classify outcome or the
// second line on "Failed and refused calls, by HTTP status." Skipped when STATUS is already set: an
// explicit STATUS run is itself the one incident to show, not something to append a second phase onto.
//
// Assumes `docker compose up -d` is already running.

const RABBITMQ_MGMT = process.env.RABBITMQ_MGMT ?? "http://localhost:15672";
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
const STATUS = process.env.STATUS ? Number(process.env.STATUS) : undefined; // unset = 503 (flaky-upstream's default)
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "20000");
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? "60000");
// How long to watch the dead-letter/parked queues after the work queue
// drains before giving up on the redrive — separate from DRAIN_TIMEOUT_MS
// and deliberately generous. Redrive only starts once the SAC-elected
// replica's own breaker closes, and that replica is whichever one RabbitMQ
// happens to have chosen — not necessarily the fastest to recover. Measured
// against this stack's own BREAKER_MAX_DELAY_MS=30000: the elected leader
// can take up to that long after the third party is actually healthy again
// before its own backoff clock lets it probe and close.
const REDRIVE_WAIT_MS = Number(process.env.REDRIVE_WAIT_MS ?? "40000");
const REDRIVE_IDLE_MS = 6000;
const CLIENT_ERROR_MS = Number(process.env.CLIENT_ERROR_MS ?? "10000");
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

const STATE_NAME = ["CLOSED", "OPEN", "HALF_OPEN"];

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
      rate === 0 ? {} : { rate, ...(MODE ? { mode: MODE } : {}), ...(STATUS ? { status: STATUS } : {}) },
    ),
  });

/** Counts over every producer run: each run keys its messages `<run>:<n>` with its own random run id. */
const audit = async () => {
  const res = await fetch(`${FLAKY_UPSTREAM}/__audit`).catch(() => undefined);
  return res?.ok ? res.json() : undefined;
};

const workQueue = `${API_ID}.work`;
const deadQueue = `${API_ID}.work.dead`;
const parkedQueue = `${API_ID}.work.parked`;

const report = (label, depth) =>
  console.log(`  ${label}: ready=${depth.ready} unacked=${depth.unacked} total=${depth.total}`);

/** Agreement bookkeeping, shared across the incident window and the drain — divergence during recovery (each replica's own half-open probe, on its own clock) is the more interesting half. */
const agreement = { ticksWithData: 0, ticksAgreed: 0, peakOpen: 0, replicaCount: 0 };

/** A single Prometheus scalar, or `undefined` if unreachable and 0 if the series doesn't exist yet. */
const scalar = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success") return undefined;
  return body.data.result.length === 0 ? 0 : Number(body.data.result[0].value[1]);
};

const pollBreakers = async (label) => {
  const states = await breakerStates();

  if (states && states.length > 0) {
    agreement.ticksWithData++;
    agreement.replicaCount = Math.max(agreement.replicaCount, states.length);
    const distinct = new Set(states.map((s) => s.state)).size;
    if (distinct === 1) agreement.ticksAgreed++;
    const openCount = states.filter((s) => s.state === 1).length;
    agreement.peakOpen = Math.max(agreement.peakOpen, openCount);
  }

  const summary = states ? states.map((s) => STATE_NAME[s.state] ?? s.state).join(",") : "no data";
  console.log(`  ${label} breakers: [${summary}]`);
};

const main = async () => {
  console.log(`== Steady state ==`);
  report(workQueue, await queueDepth(workQueue));
  report(deadQueue, await queueDepth(deadQueue));
  await pollBreakers("t+0s");

  console.log(
    `\n== Injecting failure: rate=${RATE} mode=${MODE ?? "error"}${STATUS ? ` status=${STATUS}` : ""} for ${WINDOW_MS}ms ==`,
  );
  const auditBefore = await audit();
  await setFailure(RATE);

  let peakBacklog = 0;
  const started = Date.now();
  while (Date.now() - started < WINDOW_MS) {
    await sleep(POLL_MS);
    const work = await queueDepth(workQueue);
    const dead = await queueDepth(deadQueue);
    peakBacklog = Math.max(peakBacklog, work.total);
    const t = `t+${Math.round((Date.now() - started) / 1000)}s`;
    console.log(`  ${t}  work=${work.total} dead=${dead.total}`);
    await pollBreakers(t);
  }

  console.log(`\n== Restoring ==`);
  await setFailure(0);
  const restoredAt = Date.now();

  let work = await queueDepth(workQueue);
  while (work.total > 0 && Date.now() - restoredAt < DRAIN_TIMEOUT_MS) {
    await sleep(POLL_MS);
    work = await queueDepth(workQueue);
    await pollBreakers(`+${Math.round((Date.now() - restoredAt) / 1000)}s`);
  }
  const drainMs = Date.now() - restoredAt;
  const drained = work.total === 0;

  const deadAtDrainEnd = (await queueDepth(deadQueue)).total;
  const parkedBefore = (await queueDepth(parkedQueue)).total;

  console.log(`\n== Waiting for the elected redriver ==`);
  const redriveStart = Date.now();
  let lastDead = deadAtDrainEnd;
  let idleSince = Date.now();
  while (Date.now() - redriveStart < REDRIVE_WAIT_MS) {
    await sleep(POLL_MS);
    const now = (await queueDepth(deadQueue)).total;
    if (now !== lastDead) {
      idleSince = Date.now();
      lastDead = now;
    }
    console.log(`  +${Math.round((Date.now() - redriveStart) / 1000)}s dead=${now}`);
    if (Date.now() - idleSince > REDRIVE_IDLE_MS) break;
  }
  const dead = await queueDepth(deadQueue);
  const parkedAfter = (await queueDepth(parkedQueue)).total;
  const auditAfter = await audit();
  const stats = auditBefore && auditAfter && {
    processed: auditAfter.processed - auditBefore.processed,
    duplicates: auditAfter.duplicates - auditBefore.duplicates,
  };

  let clientError;
  if (STATUS === undefined) {
    console.log(`\n== Injecting client_error (422): the third party is up, refuses every request ==`);
    const tripsBefore = (await scalar(`sum(egress_consumer_breaker_trips_total)`)) ?? 0;
    const callsBefore = (await scalar(`sum(egress_consumer_calls_total{outcome="client_error"})`)) ?? 0;
    await fetch(`${FLAKY_UPSTREAM}/__fail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ rate: 1, status: 422 }),
    });
    const ceStart = Date.now();
    while (Date.now() - ceStart < CLIENT_ERROR_MS) {
      await sleep(POLL_MS);
      const states = await breakerStates();
      const summary = states ? states.map((s) => STATE_NAME[s.state] ?? s.state).join(",") : "no data";
      console.log(`  t+${Math.round((Date.now() - ceStart) / 1000)}s  breakers: [${summary}]`);
    }
    await setFailure(0);
    await sleep(POLL_MS * 2); // let the last couple of calls land and the next scrape catch them
    const tripsAfter = (await scalar(`sum(egress_consumer_breaker_trips_total)`)) ?? tripsBefore;
    const callsAfter = (await scalar(`sum(egress_consumer_calls_total{outcome="client_error"})`)) ?? callsBefore;
    clientError = { calls: callsAfter - callsBefore, trips: tripsAfter - tripsBefore };
    console.log(`  ${clientError.calls} client_error calls, ${clientError.trips} of them tripped a breaker`);
  }

  console.log(`\n== Summary ==`);
  console.log(`  peak backlog (work queue): ${peakBacklog}`);
  console.log(`  dead-lettered: ${dead.total}`);
  console.log(
    drained
      ? `  drained to 0 in ${(drainMs / 1000).toFixed(1)}s after restore`
      : `  did NOT drain within ${DRAIN_TIMEOUT_MS}ms (${work.total} left)`,
  );
  console.log(
    `  redrive: ${deadAtDrainEnd - dead.total} moved back onto the work queue, ` +
      `${parkedAfter - parkedBefore} parked as poison, ${dead.total} still dead-lettered`,
  );
  if (clientError) {
    console.log(
      `  client_error: ${clientError.calls} calls refused (4xx), ${clientError.trips} counted by any breaker`,
    );
  }
  if (stats) {
    console.log(
      `  audit, this run: processed=${stats.processed ?? "?"} duplicates=${stats.duplicates ?? "?"}`,
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

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
