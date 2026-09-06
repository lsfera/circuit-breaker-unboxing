/**
 * What does this cost at a size nobody has run it at?
 *
 * Every number in this repo up to now came from three APIs and three replicas.
 * That is enough to prove the properties and useless for predicting anything,
 * so this drives a running aggregator and reports what actually moves as the
 * fleet grows: tick rate, poll duration, how long `/api/state` takes to build,
 * the size of what it returns, and the number of Prometheus series — the last
 * because cardinality is the cost that shows up in someone else's system
 * rather than this one.
 *
 * Usage, against an aggregator started with `--source=sim --apis=N --replicas=R`:
 *
 *   node infra/scale-probe.mjs http://127.0.0.1:8088 20
 *
 * (second argument: how many seconds to sample; default 20)
 *
 * It measures, it does not assert. What it is for is filling in the "Measured
 * limits" table in the README with numbers rather than adjectives.
 */

const BASE = process.argv[2] ?? "http://127.0.0.1:8088";
const SECONDS = Number(process.argv[3] ?? 20);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const scrape = async () => {
  const t0 = performance.now();
  const res = await fetch(`${BASE}/metrics`);
  const body = await res.text();
  return { body, ms: performance.now() - t0 };
};

/** Prometheus text: every line that is not a comment and not blank is one series. */
const seriesCount = (body) =>
  body.split("\n").filter((l) => l && !l.startsWith("#")).length;

const counter = (body, name) => {
  const line = body.split("\n").find((l) => l.startsWith(`${name} `) || l.startsWith(`${name}{`));
  return line ? Number(line.slice(line.lastIndexOf(" ") + 1)) : null;
};

/** Timers arrive as summary quantiles; the count and sum are what a rate needs. */
const timerSum = (body, name) => {
  const line = body.split("\n").find((l) => l.startsWith(`${name}_sum`));
  return line ? Number(line.slice(line.lastIndexOf(" ") + 1)) : null;
};
const timerCount = (body, name) => {
  const line = body.split("\n").find((l) => l.startsWith(`${name}_count`));
  return line ? Number(line.slice(line.lastIndexOf(" ") + 1)) : null;
};

const timed = async (path) => {
  const t0 = performance.now();
  const res = await fetch(`${BASE}${path}`);
  const body = await res.text();
  return { ms: performance.now() - t0, bytes: Buffer.byteLength(body) };
};

const first = await scrape();
const stateFirst = await timed("/api/state");
await sleep(SECONDS * 1000);
const last = await scrape();
const stateLast = await timed("/api/state");

const ticks0 = counter(first.body, "egress_aggregator_ticks_total");
const ticks1 = counter(last.body, "egress_aggregator_ticks_total");
const pollSum0 = timerSum(first.body, "egress_fleet_poll_duration_ms");
const pollSum1 = timerSum(last.body, "egress_fleet_poll_duration_ms");
const pollN0 = timerCount(first.body, "egress_fleet_poll_duration_ms");
const pollN1 = timerCount(last.body, "egress_fleet_poll_duration_ms");

const report = {
  sampledSeconds: SECONDS,
  ticksPerSecond:
    ticks0 !== null && ticks1 !== null ? +((ticks1 - ticks0) / SECONDS).toFixed(2) : null,
  meanPollMs:
    pollSum0 !== null && pollN1 !== null && pollN1 > pollN0
      ? +((pollSum1 - pollSum0) / (pollN1 - pollN0)).toFixed(2)
      : null,
  prometheusSeries: seriesCount(last.body),
  metricsScrapeMs: +last.ms.toFixed(1),
  metricsBytes: Buffer.byteLength(last.body),
  apiStateMs: +((stateFirst.ms + stateLast.ms) / 2).toFixed(1),
  apiStateBytes: stateLast.bytes,
};

console.log(JSON.stringify(report, null, 2));
