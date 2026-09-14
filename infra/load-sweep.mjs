/**
 * Where does the data plane stop keeping up?
 *
 *   node infra/load-sweep.mjs
 *   node infra/load-sweep.mjs --rates=200,800,3200 --hold=60 --settle=10
 *
 * Steps the producer's fixed arrival rate against the running compose stack
 * (`RATE_PER_SECOND=N docker compose up -d --no-deps rmq-producer`) and, for
 * each step, measures over a window: what was actually published, what the
 * fleet completed, which way the work queue moved, whether the circuit left
 * CLOSED, broker memory and flow control, and each container's CPU against its
 * limit and how often the kernel throttled it. Between steps the producer is
 * stopped and the backlog drains, and the drain rate with no arrivals is the
 * fleet's consumption ceiling measured directly.
 *
 * Unlike infra/instrument.mjs, throttling is not a reason to discard the run:
 * finding which container saturates first is the point. A step is "held" when
 * the producer reached its rate, the queue did not grow, and the circuit stayed
 * CLOSED, with failed calls under --max-failed-pct (5%). The sweep stops after two steps in a row that did not hold, and always
 * restores the producer to its compose default.
 */

import http from "node:http";
import dns from "node:dns/promises";
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const hit = args.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const RATES = flag("rates", "200,400,800,1600,3200,6400,12800").split(",").map(Number);
const HOLD = Number(flag("hold", 60));
const SETTLE = Number(flag("settle", 10));
const MAX_FAILED_PCT = Number(flag("max-failed-pct", 5));
const DRAIN_TIMEOUT = Number(flag("drain-timeout", 180));
const PROJECT = flag("project", process.env.COMPOSE_PROJECT_NAME ?? "workspace");
const PROMETHEUS = flag("prometheus", "http://prometheus:9090");
const RABBIT = flag("rabbit", "http://guest:guest@rabbitmq:15672");
const PRODUCER = flag("producer", "http://rmq-producer:9464");
const WORK_QUEUE = "payments-provider.work";
const DEAD_QUEUE = "payments-provider.work.dead";
const OUT = flag("out", `history/runs/load-sweep-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

// ---- sources ---------------------------------------------------------------

const SOCKET = process.env.DOCKER_HOST?.startsWith("unix://") ? process.env.DOCKER_HOST.slice(7) : "/var/run/docker.sock";
const docker = (path) =>
  new Promise((resolve, reject) => {
    http
      .get({ socketPath: SOCKET, path }, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (d) => (body += d));
        res.on("end", () => {
          try {
            resolve(JSON.parse(body));
          } catch {
            reject(new Error(`${path}: ${body.slice(0, 200)}`));
          }
        });
      })
      .on("error", reject);
  });

const rabbit = async (path) => {
  const url = new URL(`${RABBIT}${path}`);
  const auth = Buffer.from(`${decodeURIComponent(url.username)}:${decodeURIComponent(url.password)}`).toString("base64");
  url.username = url.password = "";
  const res = await fetch(url, { headers: { authorization: `Basic ${auth}` }, signal: AbortSignal.timeout(5000) });
  return res.json();
};

const queueDepth = async (name) => {
  const q = await rabbit(`/api/queues/%2F/${encodeURIComponent(name)}?columns=messages,messages_ready,messages_unacknowledged`);
  return q.messages ?? 0;
};

const promInstant = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).catch(() => null);
  if (!res?.ok) return null;
  const v = (await res.json()).data?.result?.[0]?.value?.[1];
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

/** Sum of every sample of `name` in Prometheus text whose labels contain all of `match`. */
const sumMetric = (text, name, match = {}) =>
  text
    .split("\n")
    .filter((l) => l.startsWith(`${name}{`) || l.startsWith(`${name} `))
    .filter((l) => Object.entries(match).every(([k, v]) => l.includes(`${k}="${v}"`)))
    .reduce((n, l) => n + Number(l.slice(l.lastIndexOf(" ") + 1)), 0);

const scrape = (url) =>
  fetch(url, { signal: AbortSignal.timeout(3000) })
    .then((r) => r.text())
    .catch(() => "");

const fleetCounters = async () => {
  const ips = await dns.resolve4("rmq-daemon").catch(() => []);
  const texts = await Promise.all(ips.map((ip) => scrape(`http://${ip}:9464/metrics`)));
  const all = texts.join("\n");
  return {
    daemons: texts.filter(Boolean).length,
    ok: sumMetric(all, "egress_daemon_calls_total", { outcome: "ok" }),
    failed: sumMetric(all, "egress_daemon_calls_total", { outcome: "failed" }),
    
  };
};

const producerPublished = async () => sumMetric(await scrape(`${PRODUCER}/metrics`), "egress_producer_published_total");

const projectContainers = async () =>
  (await docker("/containers/json"))
    .filter((c) => c.Labels?.["com.docker.compose.project"] === PROJECT)
    .map((c) => ({ id: c.Id, name: c.Names[0].slice(1).replace(`${PROJECT}-`, "") }));

/** One cumulative reading per container; two of them make a window. */
const cpuSnapshot = async () => {
  const cs = await projectContainers();
  const entries = await Promise.all(
    cs.map(async (c) => {
      const [s, d] = await Promise.all([docker(`/containers/${c.id}/stats?stream=false&one-shot=true`), docker(`/containers/${c.id}/json`)]);
      return [
        c.name,
        {
          at: Date.parse(s.read),
          used: s.cpu_stats?.cpu_usage?.total_usage ?? 0,
          periods: s.cpu_stats?.throttling_data?.periods ?? 0,
          throttled: s.cpu_stats?.throttling_data?.throttled_periods ?? 0,
          memory: (s.memory_stats?.usage ?? 0) - (s.memory_stats?.stats?.inactive_file ?? 0),
          memoryLimit: d.HostConfig.Memory || null,
          limitCpus: d.HostConfig.NanoCpus ? d.HostConfig.NanoCpus / 1e9 : null,
          restarts: d.RestartCount ?? 0,
          oomKilled: d.State?.OOMKilled === true,
          startedAt: d.State?.StartedAt,
        },
      ];
    }),
  );
  return new Map(entries);
};

const cpuWindow = (a, b) =>
  [...b]
    .filter(([name]) => a.has(name))
    .map(([name, end]) => {
      const start = a.get(name);
      const wall = (end.at - start.at) / 1000;
      const cpus = wall > 0 ? Math.max(0, end.used - start.used) / 1e9 / wall : 0;
      const periods = end.periods - start.periods;
      const throttled = end.throttled - start.throttled;
      return {
        container: name,
        cpus: +cpus.toFixed(3),
        limitCpus: end.limitCpus,
        ofLimit: end.limitCpus ? +(cpus / end.limitCpus).toFixed(3) : null,
        throttledPct: periods > 0 ? +((throttled / periods) * 100).toFixed(1) : 0,
        memoryMiB: +(end.memory / 2 ** 20).toFixed(0),
        memoryOfLimit: end.memoryLimit ? +(end.memory / end.memoryLimit).toFixed(3) : null,
        restarted: end.restarts !== start.restarts || end.startedAt !== start.startedAt,
        oomKilled: end.oomKilled,
      };
    })
    .sort((x, y) => (y.ofLimit ?? 0) - (x.ofLimit ?? 0));

const brokerSnapshot = async () => {
  const [node] = await rabbit("/api/nodes?columns=mem_used,mem_limit,mem_alarm,disk_free_alarm,run_queue");
  const conns = await rabbit("/api/connections?columns=state,name");
  const states = {};
  for (const c of conns) states[c.state] = (states[c.state] ?? 0) + 1;
  return { memUsedMiB: +(node.mem_used / 2 ** 20).toFixed(0), memLimitMiB: +(node.mem_limit / 2 ** 20).toFixed(0), memAlarm: node.mem_alarm, diskAlarm: node.disk_free_alarm, runQueue: node.run_queue, connectionStates: states };
};

/** Least-squares slope of (seconds, depth), so one stale management sample does not decide it. */
const slope = (points) => {
  if (points.length < 2) return 0;
  const n = points.length;
  const mx = points.reduce((s, p) => s + p[0], 0) / n;
  const my = points.reduce((s, p) => s + p[1], 0) / n;
  const num = points.reduce((s, p) => s + (p[0] - mx) * (p[1] - my), 0);
  const den = points.reduce((s, p) => s + (p[0] - mx) ** 2, 0);
  return den ? num / den : 0;
};

// ---- control ---------------------------------------------------------------

const compose = (rate, ...argv) => {
  const r = spawnSync("docker", ["compose", ...argv], {
    env: rate === null ? { ...process.env, RATE_PER_SECOND: "" } : { ...process.env, RATE_PER_SECOND: String(rate) },
    encoding: "utf8",
  });
  if (r.status !== 0) throw new Error(`docker compose ${argv.join(" ")}: ${r.stderr}`);
};

const waitFor = async (check, timeoutMs) => {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (await check().catch(() => false)) return true;
    await sleep(1000);
  }
  return false;
};

/** Stops arrivals and measures how fast the fleet empties the queue with nothing coming in. */
const drain = async () => {
  compose(200, "stop", "rmq-producer");
  const t0 = performance.now();
  const start = await queueDepth(WORK_QUEUE);
  const samples = [[0, start]];
  const emptied = await waitFor(async () => {
    const d = await queueDepth(WORK_QUEUE);
    samples.push([(performance.now() - t0) / 1000, d]);
    return d === 0;
  }, DRAIN_TIMEOUT * 1000);
  const seconds = (performance.now() - t0) / 1000;
  // Management stats refresh every 5s, so only the part of the curve still above zero measures a rate.
  const falling = samples.filter(([, d], i) => d > 0 || i === samples.findIndex(([, x]) => x === 0));
  return { backlog: start, seconds: +seconds.toFixed(1), emptied, rate: start > 1000 ? +(-slope(falling)).toFixed(0) : null };
};

const step = async (rate) => {
  process.stdout.write(`\n== ${rate}/s ==\n`);
  compose(rate, "up", "-d", "--no-deps", "rmq-producer");
  if (!(await waitFor(async () => (await scrape(`${PRODUCER}/metrics`)).includes("egress_producer_published_total"), 60_000))) {
    return { rate, error: "producer never served metrics" };
  }
  await sleep(SETTLE * 1000);

  const wall0 = Date.now();
  const mono0 = performance.now();
  const [cpu0, fleet0, pub0, dead0] = await Promise.all([cpuSnapshot(), fleetCounters(), producerPublished(), queueDepth(DEAD_QUEUE)]);
  const depth = [];
  const broker = [];
  while (performance.now() - mono0 < HOLD * 1000) {
    const t = (performance.now() - mono0) / 1000;
    const [d, b] = await Promise.all([queueDepth(WORK_QUEUE), brokerSnapshot()]);
    depth.push([t, d]);
    broker.push(b);
    await sleep(2000);
  }
  const [cpu1, fleet1, pub1, dead1] = await Promise.all([cpuSnapshot(), fleetCounters(), producerPublished(), queueDepth(DEAD_QUEUE)]);
  const seconds = (performance.now() - mono0) / 1000;
  const suspended = Math.abs((Date.now() - wall0) / 1000 - seconds) > 5;

  const w = `${Math.round(seconds)}s`;
  const prom = {
    circuitMax: await promInstant(`max_over_time(max(egress_daemon_circuit_state)[${w}:2s])`),
    targetFractionMin: await promInstant(`min_over_time(max(egress_daemon_target_fraction)[${w}:2s])`),
    daemonsPullingMin: await promInstant(`min_over_time(sum(rabbitmq_detailed_queue_consumers{queue="${WORK_QUEUE}"})[${w}:2s])`),
    inFlightMax: await promInstant(`max_over_time(sum(rabbitmq_detailed_queue_messages_unacked{queue="${WORK_QUEUE}"})[${w}:2s])`),
    envoyP99Ms: await promInstant(
      `histogram_quantile(0.99, sum by (le) (rate(envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="payments-provider"}[${w}])))`,
    ),
    envoyP50Ms: await promInstant(
      `histogram_quantile(0.5, sum by (le) (rate(envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="payments-provider"}[${w}])))`,
    ),
    envoyPendingOverflow: await promInstant(`sum(increase(envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="payments-provider"}[${w}]))`),
    envoyEjections: await promInstant(`sum(increase(envoy_cluster_outlier_detection_ejections_enforced_total{envoy_cluster_name="payments-provider"}[${w}]))`),
    envoy5xx: await promInstant(`sum(increase(envoy_cluster_upstream_rq_xx{envoy_cluster_name="payments-provider",envoy_response_code_class="5"}[${w}]))`),
  };

  const published = (pub1 - pub0) / seconds;
  const ok = (fleet1.ok - fleet0.ok) / seconds;
  const failed = (fleet1.failed - fleet0.failed) / seconds;
  const growth = slope(depth);
  const containers = cpuWindow(cpu0, cpu1);
  const worstBroker = {
    memUsedMiBMax: Math.max(...broker.map((b) => b.memUsedMiB)),
    memLimitMiB: broker[0]?.memLimitMiB,
    memAlarm: broker.some((b) => b.memAlarm),
    flowConnectionsMax: Math.max(...broker.map((b) => (b.connectionStates.flow ?? 0) + (b.connectionStates.blocked ?? 0) + (b.connectionStates.blocking ?? 0))),
    runQueueMax: Math.max(...broker.map((b) => b.runQueue)),
  };

  const reasons = [];
  if (published < rate * 0.95) reasons.push(`producer reached ${published.toFixed(0)}/s of ${rate}/s`);
  if (growth > Math.max(5, rate * 0.002)) reasons.push(`work queue grew ${growth.toFixed(0)} msg/s`);
  if ((prom.circuitMax ?? 0) > 0) reasons.push(`circuit left CLOSED (max state ${prom.circuitMax})`);
  if (worstBroker.memAlarm) reasons.push("broker memory alarm");
  if (failed > Math.max(1, (ok + failed) * (MAX_FAILED_PCT / 100))) reasons.push(`${failed.toFixed(1)} failed calls/s`);
  for (const c of containers) {
    if (c.oomKilled) reasons.push(`${c.container} OOM-killed`);
    else if (c.restarted && c.container !== "rmq-producer-1") reasons.push(`${c.container} restarted`);
  }

  const result = {
    rate,
    seconds: +seconds.toFixed(1),
    suspended,
    held: reasons.length === 0 && !suspended,
    reasons,
    publishedPerSecond: +published.toFixed(1),
    completedOkPerSecond: +ok.toFixed(1),
    failedPerSecond: +failed.toFixed(2),
    deadQueueGrowth: dead1 - dead0,
    workQueue: { start: depth[0]?.[1] ?? null, end: depth.at(-1)?.[1] ?? null, slopePerSecond: +growth.toFixed(1) },
    daemonsScraped: fleet1.daemons,
    prometheus: prom,
    broker: worstBroker,
    saturated: containers.filter((c) => (c.ofLimit ?? 0) >= 0.8 || c.throttledPct >= 5),
    containers,
  };

  console.log(
    `  published ${result.publishedPerSecond}/s · ok ${result.completedOkPerSecond}/s · failed ${result.failedPerSecond}/s · ` +
      `queue ${result.workQueue.start}→${result.workQueue.end} (${result.workQueue.slopePerSecond}/s) · circuit max ${prom.circuitMax} · ` +
      `in-flight max ${prom.inFlightMax} · envoy p50/p99 ${prom.envoyP50Ms?.toFixed?.(1)}/${prom.envoyP99Ms?.toFixed?.(1)} ms`,
  );
  console.log(`  broker ${worstBroker.memUsedMiBMax}/${worstBroker.memLimitMiB} MiB, flow-controlled connections max ${worstBroker.flowConnectionsMax}`);
  for (const c of containers.slice(0, 6)) {
    console.log(`  ${c.container.padEnd(22)} ${c.cpus.toFixed(2)} cpus of ${c.limitCpus} (${((c.ofLimit ?? 0) * 100).toFixed(0)}%) throttled ${c.throttledPct}%  ${c.memoryMiB} MiB`);
  }
  console.log(`  ${result.held ? "HELD" : `DID NOT HOLD: ${reasons.join("; ") || "host suspended during window"}`}`);
  return result;
};

// ---- run -------------------------------------------------------------------

const info = await docker("/info");
const record = { scenario: "load-sweep", startedAt: new Date().toISOString(), host: { cpus: info.NCPU, memory: info.MemTotal }, hold: HOLD, settle: SETTLE, steps: [] };
console.log(`load sweep: rates ${RATES.join(", ")}/s, ${HOLD}s hold, host ${info.NCPU} cpus`);

const save = () => {
  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, `${JSON.stringify(record, null, 2)}\n`);
};

let misses = 0;
try {
  const initial = await drain();
  console.log(`initial drain: ${initial.backlog} messages in ${initial.seconds}s`);
  for (const rate of RATES) {
    let result = await step(rate);
    if (result.suspended) {
      await drain();
      console.log("  host suspended during the window; repeating the step");
      result = await step(rate);
    }
    result.drainAfter = await drain();
    console.log(
      `  drain: ${result.drainAfter.backlog} messages in ${result.drainAfter.seconds}s` +
        (result.drainAfter.rate ? ` (${result.drainAfter.rate} msg/s with no arrivals)` : "") +
        (result.drainAfter.emptied ? "" : " — NOT EMPTIED"),
    );
    record.steps.push(result);
    save();
    misses = result.held ? 0 : misses + 1;
    if (misses >= 2) break;
  }
} finally {
  compose(null, "up", "-d", "--no-deps", "rmq-producer");
  record.endedAt = new Date().toISOString();
  const lastHeld = record.steps.filter((s) => s.held).at(-1);
  const firstMiss = record.steps.find((s) => !s.held);
  record.summary = { highestHeldRate: lastHeld?.rate ?? null, firstFailedRate: firstMiss?.rate ?? null, firstFailedReasons: firstMiss?.reasons ?? [] };
  save();
  console.log(`\nhighest rate held: ${lastHeld?.rate ?? "none"}/s · first that did not: ${firstMiss?.rate ?? "none"}/s`);
  console.log(`record: ${OUT}`);
}
