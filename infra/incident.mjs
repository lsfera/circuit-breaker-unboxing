// Drives one incident against the fleet and reports what actually happened —
// including whether five independent, in-process breakers agree with each
// other about the same third party (they share nothing, see
// packages/consumer/src/Breaker.ts), since article 3, whether the messages
// this incident dead-lettered actually come back (packages/consumer/src/
// Redrive.ts), and since article 3, how long the fleet as a whole took to
// read open (infra/monitoring/rules.yml) after the first replica noticed —
// measured, not asserted, same as everything else here.
//
//   node infra/incident.mjs
//   WINDOW_MS=30000 RATE=0.6 node infra/incident.mjs   # a partial failure instead
//   MODE=hang node infra/incident.mjs                  # this is the one that grows the work queue
//   STATUS=422 node infra/incident.mjs                 # only a 4xx incident, nothing else appended
//   CAPACITY=20 DELAY_MS=100 node infra/incident.mjs   # a third party that is full, not broken: serves 20 at once
//                                                      # (200/s), answers 429 to the rest. Needs RATE_PER_SECOND above that.
//
// A default run (STATUS and CAPACITY unset) also appends a short client_error (422) phase after the outage and its
// redrive — otherwise every run only ever shows a 503, and never the fourth classify outcome or the
// second line on "Failed and refused calls, by HTTP status." Skipped when either is set: an
// explicit STATUS or CAPACITY run is itself the one incident to show, not something to append a second phase onto.
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
// Article 4: a third party with a ceiling instead of a failure rate — at
// most CAPACITY requests in flight, each taking DELAY_MS, the rest told 429.
const CAPACITY = Number(process.env.CAPACITY ?? "0");
const DELAY_MS = Number(process.env.DELAY_MS ?? "0");
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
      rate === 0
        ? {}
        : CAPACITY > 0
          ? { capacity: CAPACITY, delayMs: DELAY_MS }
          : { rate, ...(MODE ? { mode: MODE } : {}), ...(STATUS ? { status: STATUS } : {}) },
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

/** One reading of the `egress:fleet_open` rule (infra/monitoring/rules.yml): 1 when half the fleet or more is open, `undefined` before Prometheus has it. */
const fleetVerdict = async () => {
  const query = encodeURIComponent("egress:fleet_open");
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${query}`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success" || body.data.result.length === 0) return undefined;
  return Number(body.data.result[0].value[1]);
};

/** A single Prometheus scalar, or `undefined` if unreachable and 0 if the series doesn't exist yet. */
const scalar = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success") return undefined;
  return body.data.result.length === 0 ? 0 : Number(body.data.result[0].value[1]);
};

/**
 * When the first replica individually noticed versus when the fleet as a
 * whole read open — set once each, on whichever poll first
 * sees the condition. `startedAt` is stamped when the injected failure
 * actually starts, so both are "ms into the incident," comparable to each
 * other regardless of which tick observes them.
 */
const lag = { startedAt: 0, firstReplicaOpenAt: undefined, verdictOpenAt: undefined };

/**
 * Since article 4: ms after the third party was restored that (a) the first
 * and last replica's own breaker closed, (b) the fleet read closed, and (c)
 * the dead-letter queue first got smaller — i.e. the first redrive move.
 */
const recovery = {
  restoredAt: undefined,
  firstReplicaClosedAt: undefined,
  lastReplicaClosedAt: undefined,
  verdictClosedAt: undefined,
  firstMoveAt: undefined,
  deadSeen: 0,
};
const sinceRestore = () => Date.now() - recovery.restoredAt;
const noteDead = (total) => {
  if (recovery.restoredAt === undefined) {
    recovery.deadSeen = Math.max(recovery.deadSeen, total);
    return;
  }
  if (recovery.firstMoveAt === undefined && total < recovery.deadSeen) recovery.firstMoveAt = sinceRestore();
  recovery.deadSeen = Math.max(recovery.deadSeen, total);
};

const pollBreakers = async (label) => {
  await noteLimit();
  const states = await breakerStates();
  const verdict = await fleetVerdict();
  const elapsed = Date.now() - lag.startedAt;

  if (states && states.length > 0) {
    agreement.ticksWithData++;
    agreement.replicaCount = Math.max(agreement.replicaCount, states.length);
    const distinct = new Set(states.map((s) => s.state)).size;
    if (distinct === 1) agreement.ticksAgreed++;
    const openCount = states.filter((s) => s.state === 1).length;
    agreement.peakOpen = Math.max(agreement.peakOpen, openCount);
    if (openCount > 0 && lag.firstReplicaOpenAt === undefined) lag.firstReplicaOpenAt = elapsed;
  }
  if (verdict !== undefined) {
    verdictOpenTicks.total++;
    if (verdict === 1) verdictOpenTicks.open++;
  }
  if (verdict === 1 && lag.verdictOpenAt === undefined) lag.verdictOpenAt = elapsed;
  if (recovery.restoredAt !== undefined && states && states.length > 0) {
    const closed = states.filter((s) => s.state === 0).length;
    if (closed > 0 && recovery.firstReplicaClosedAt === undefined) recovery.firstReplicaClosedAt = sinceRestore();
    if (closed === states.length && recovery.lastReplicaClosedAt === undefined) recovery.lastReplicaClosedAt = sinceRestore();
  }
  if (recovery.restoredAt !== undefined && verdict === 0 && recovery.verdictClosedAt === undefined) {
    recovery.verdictClosedAt = sinceRestore();
  }

  const summary = states ? states.map((s) => STATE_NAME[s.state] ?? s.state).join(",") : "no data";
  const verdictText = verdict === undefined ? "no data" : verdict === 1 ? "OPEN" : "CLOSED";
  console.log(`  ${label} breakers: [${summary}]  fleet verdict: ${verdictText}`);
};

/**
 * Since article 4: the whole incident's calls by outcome, straight from the
 * consumers' own counters (a delta between two readings, so whatever ran
 * before is excluded), and how many polled seconds the fleet verdict spent
 * open. "failed" is the third party actually being hit and failing, "open" is
 * a call a replica's own breaker refused to make: a partial degradation that
 * a breaker never opens for shows up as failed with no open.
 */
const callTotals = async () => {
  const query = encodeURIComponent("sum by (outcome) (egress_consumer_calls_total)");
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${query}`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success") return undefined;
  return Object.fromEntries(body.data.result.map((r) => [r.metric.outcome, Number(r.value[1])]));
};
const verdictOpenTicks = { open: 0, total: 0 };

/** Since article 4: the fleet's summed concurrency limit, one reading a tick — how far the replicas let themselves be pushed down, and back. */
const limits = { faultOn: false, min: undefined, samples: [], recoveredAt: undefined };
const fleetLimit = async () => {
  const query = encodeURIComponent("sum(egress_consumer_concurrency_limit)");
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${query}`).catch(() => undefined);
  if (!res || !res.ok) return undefined;
  const body = await res.json();
  if (body.status !== "success" || body.data.result.length === 0) return undefined;
  return Number(body.data.result[0].value[1]);
};
const noteLimit = async () => {
  const v = await fleetLimit();
  if (v === undefined) return;
  if (limits.faultOn) {
    limits.samples.push(v);
    limits.min = limits.min === undefined ? v : Math.min(limits.min, v);
  } else if (recovery.restoredAt !== undefined && limits.recoveredAt === undefined && v >= 0.9 * limits.start) {
    limits.recoveredAt = Date.now() - recovery.restoredAt;
  }
};

const main = async () => {
  const callsBefore = await callTotals();
  console.log(`== Steady state ==`);
  report(workQueue, await queueDepth(workQueue));
  report(deadQueue, await queueDepth(deadQueue));
  await pollBreakers("t+0s");

  console.log(
    `\n== Injecting failure: rate=${RATE} mode=${MODE ?? "error"}${STATUS ? ` status=${STATUS}` : ""} for ${WINDOW_MS}ms ==`,
  );
  const auditBefore = await audit();
  lag.startedAt = Date.now();
  limits.start = (await fleetLimit()) ?? 0;
  await setFailure(RATE);
  limits.faultOn = true;
  const callsAtStart = await callTotals();
  const auditAtStart = await audit(API_ID);

  let peakBacklog = 0;
  const started = Date.now();
  while (Date.now() - started < WINDOW_MS) {
    await sleep(POLL_MS);
    const work = await queueDepth(workQueue);
    const dead = await queueDepth(deadQueue);
    peakBacklog = Math.max(peakBacklog, work.total);
    noteDead(dead.total);
    const t = `t+${Math.round((Date.now() - started) / 1000)}s`;
    console.log(`  ${t}  work=${work.total} dead=${dead.total}`);
    await pollBreakers(t);
  }

  const callsAtEnd = await callTotals();
  const auditAtEnd = await audit(API_ID);
  const faultMs = Date.now() - started;
  limits.faultOn = false;
  console.log(`\n== Restoring ==`);
  await setFailure(0);
  const restoredAt = Date.now();
  recovery.restoredAt = restoredAt;

  let work = await queueDepth(workQueue);
  while (work.total > 0 && Date.now() - restoredAt < DRAIN_TIMEOUT_MS) {
    await sleep(POLL_MS);
    work = await queueDepth(workQueue);
    noteDead((await queueDepth(deadQueue)).total);
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
    noteDead(now);
    await pollBreakers(`+${Math.round((Date.now() - restoredAt) / 1000)}s`);
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
  if (STATUS === undefined && CAPACITY === 0) {
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
  const callsAfter = await callTotals();
  if (callsBefore && callsAfter) {
    const d = (k) => Math.round((callsAfter[k] ?? 0) - (callsBefore[k] ?? 0));
    const shown = d("ok") + d("failed") + d("open") + d("throttled");
    console.log(
      `  calls this incident: ${d("ok")} ok, ${d("failed")} failed (the third party was hit and failed), ` +
        `${d("open")} refused locally by an open breaker, ${d("throttled")} answered 429 (slow down)` +
        (shown > 0 ? ` — ${Math.round((100 * d("failed")) / shown)}% of all calls were real failures` : ""),
    );
  }
  if (auditAtStart && auditAtEnd) {
    // The upstream's own count of distinct messages it answered 200 — exact,
    // where a Prometheus counter read lags by up to a scrape.
    const ok = auditAtEnd.processed - auditAtStart.processed;
    const ceiling = CAPACITY > 0 && DELAY_MS > 0 ? ` (the third party's ceiling is ${Math.round((CAPACITY * 1000) / DELAY_MS)}/s)` : "";
    console.log(`  goodput while the fault was on: ${(ok / (faultMs / 1000)).toFixed(0)} successful calls/s${ceiling}`);
  }
  if (limits.samples.length > 0) {
    const tail = limits.samples.slice(-Math.max(1, Math.floor(limits.samples.length / 2)));
    console.log(
      `  fleet concurrency limit (sum over replicas): started at ${limits.start}, ${limits.min} at least, ` +
        `${(tail.reduce((a, b) => a + b, 0) / tail.length).toFixed(1)} on average over the later half of the fault, ` +
        `back to 90% of the start ${limits.recoveredAt === undefined ? "never observed" : `${(limits.recoveredAt / 1000).toFixed(1)}s after restore`}`,
    );
  }
  if (verdictOpenTicks.total > 0) {
    console.log(`  fleet verdict was OPEN on ${verdictOpenTicks.open}/${verdictOpenTicks.total} polled ticks`);
  }
  const sec = (ms) => (ms === undefined ? "never observed" : `${(ms / 1000).toFixed(1)}s`);
  console.log(
    `  recovery, seconds after restore: first replica closed ${sec(recovery.firstReplicaClosedAt)}, ` +
      `fleet verdict closed ${sec(recovery.verdictClosedAt)}, last replica closed ${sec(recovery.lastReplicaClosedAt)}, ` +
      `first redrive move ${sec(recovery.firstMoveAt)}`,
  );
  if (lag.firstReplicaOpenAt !== undefined && lag.verdictOpenAt !== undefined) {
    const deltaMs = lag.verdictOpenAt - lag.firstReplicaOpenAt;
    console.log(
      deltaMs >= 0
        ? `  fleet verdict lagged the first replica to open by ${(deltaMs / 1000).toFixed(1)}s`
        : `  fleet verdict opened ${(-deltaMs / 1000).toFixed(1)}s BEFORE any single replica did (threshold crossed early)`,
    );
  } else {
    console.log(`  fleet verdict lag: no data — is ${PROMETHEUS} evaluating infra/monitoring/rules.yml?`);
  }
};

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
