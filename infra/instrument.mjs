/**
 * What this stack costs, inside a declared envelope (ADR 014).
 *
 *   node infra/instrument.mjs                  # the demo scenario
 *   node infra/instrument.mjs idle --seconds=60
 *   node infra/instrument.mjs chaos:leader
 *   node infra/instrument.mjs demo --baseline docs/runs/baseline-demo-2026-09-12.json
 *
 * Samples Docker's stats stream per container per second over the Engine API
 * (which carries `throttling_data`), and exits non-zero when the run should not
 * be quoted: a service without limits, throttling past budget, an OOM kill, or a
 * failed scenario. Records land in docs/runs/. Needs the Docker socket.
 */

import http from "node:http";
import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, basename } from "node:path";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const args = process.argv.slice(2);
/** Both `--name=value` and `--name value`; boolean defaults mark flags that take no value. */
const flag = (name, fallback) => {
  const at = args.findIndex((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (at < 0) return fallback;
  const hit = args[at];
  if (hit.includes("=")) return hit.slice(hit.indexOf("=") + 1);
  if (typeof fallback === "boolean") return true;
  const next = args[at + 1];
  return next && !next.startsWith("--") ? next : true;
};
const SCENARIO = args.find((a) => !a.startsWith("--")) ?? "demo";
const SECONDS = Number(flag("seconds", 45));
const PROJECT = flag("project", process.env.COMPOSE_PROJECT_NAME ?? "workspace");
const PROMETHEUS = flag("prometheus", "http://prometheus:9090");
const BASELINE = flag("baseline", null);
/** % of scheduling periods throttled. Not zero: a quota is per 100 ms, so sub-second bursts are clipped at any headroom. */
const THROTTLE_BUDGET = Number(flag("throttle-budget", 1));
/** A near-idle container's one throttled period is not 100% pressure; 20 periods is 2 s runnable. */
const MIN_THROTTLE_PERIODS = Number(flag("min-throttle-periods", 20));
/** The one way to get a record out of an unbounded stack: how the limits in
 *  docker-compose.yml were sized in the first place. It stamps the record. */
const ALLOW_UNLIMITED = flag("allow-unlimited", false) === true;
const OUT = flag(
  "out",
  `docs/runs/${SCENARIO.replace(":", "-")}-${new Date().toISOString().replace(/[:.]/g, "-")}.json`,
);

// The Engine API over the socket.

const SOCKET = process.env.DOCKER_HOST?.startsWith("unix://")
  ? process.env.DOCKER_HOST.slice("unix://".length)
  : "/var/run/docker.sock";

const api = (path) =>
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

/** Open stats streams, closed when the run ends: Docker streams until the client hangs up. */
const streams = [];

const streamStats = (id, onSample) => {
  const req = http.get({ socketPath: SOCKET, path: `/containers/${id}/stats?stream=true` }, (res) => {
    let buffer = "";
    res.setEncoding("utf8");
    res.on("data", (chunk) => {
      buffer += chunk;
      let nl;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 1);
        if (line.trim()) {
          try {
            onSample(JSON.parse(line));
          } catch {
            /* a truncated frame at teardown is not worth failing a run over */
          }
        }
      }
    });
    // A killed container ends its own stream. That is information, not an
    // error: chaos scenarios do exactly that on purpose.
    res.on("end", () => {});
  });
  req.on("error", () => {});
  streams.push(req);
  return req;
};

// ---------------------------------------------------------------------------
// What is running, and what it is allowed to use.
// ---------------------------------------------------------------------------

const NANO = 1e9;

const containers = async () => {
  const all = await api("/containers/json");
  return all
    .filter((c) => c.Labels?.["com.docker.compose.project"] === PROJECT)
    .map((c) => ({
      id: c.Id,
      name: c.Names[0].replace(/^\//, ""),
      service: c.Labels["com.docker.compose.service"],
      image: c.Image,
    }));
};

const limitsOf = async (id) => {
  const detail = await api(`/containers/${id}/json`);
  return {
    cpus: detail.HostConfig.NanoCpus ? detail.HostConfig.NanoCpus / NANO : null,
    memory: detail.HostConfig.Memory || null,
    pids: detail.HostConfig.PidsLimit || null,
    restarts: detail.RestartCount ?? 0,
    oomKilled: detail.State?.OOMKilled === true,
    started: detail.State?.StartedAt ?? null,
  };
};

// ---------------------------------------------------------------------------
// One accumulator per container.
// ---------------------------------------------------------------------------

// Cores, not Docker's host-scaled ratio. Bucketed by second: summed per-container
// peaks overstate the stack, whose busiest second is a real instant.
const timeline = new Map();
const intoTimeline = (at, cpus, memory) => {
  const bucket = Math.floor(at / 1000);
  const slot = timeline.get(bucket) ?? { cpus: 0, memory: 0, containers: 0 };
  slot.cpus += cpus;
  slot.memory += memory;
  slot.containers++;
  timeline.set(bucket, slot);
};

const track = (name, service) => ({
  name,
  service,
  samples: 0,
  peakCpus: 0,
  peakMemory: 0,
  cpuSeconds: 0,
  memoryLimit: null,
  throttleBase: null,
  throttled: null,
  last: null,
  first: null,
  record(sample) {
    const at = Date.parse(sample.read);
    const used = sample.cpu_stats?.cpu_usage?.total_usage;
    if (!Number.isFinite(at) || used === undefined) return;
    const memory = (sample.memory_stats?.usage ?? 0) - (sample.memory_stats?.stats?.inactive_file ?? 0);
    this.memoryLimit = sample.memory_stats?.limit ?? this.memoryLimit;
    this.peakMemory = Math.max(this.peakMemory, memory);

    // Cumulative since start, so this run is the difference from the first sample.
    const throttle = sample.cpu_stats?.throttling_data;
    if (throttle) {
      this.throttleBase ??= { periods: throttle.periods, throttled: throttle.throttled_periods, nanos: throttle.throttled_time };
      this.throttled = {
        periods: Math.max(0, throttle.throttled_periods - this.throttleBase.throttled),
        ofPeriods: Math.max(0, throttle.periods - this.throttleBase.periods),
        nanos: Math.max(0, throttle.throttled_time - this.throttleBase.nanos),
      };
    }

    if (this.last) {
      const wall = (at - this.last.at) / 1000;
      // A restarted container's counter goes backwards. Skip that sample
      // rather than recording a negative second or an impossible peak.
      const cpu = (used - this.last.used) / NANO;
      if (wall > 0 && cpu >= 0) {
        this.cpuSeconds += cpu;
        this.peakCpus = Math.max(this.peakCpus, cpu / wall);
        this.samples++;
        intoTimeline(at, cpu / wall, memory);
      }
    } else {
      this.first = { at, used };
    }
    this.last = { at, used };
  },
});

// Scenarios: each is a child process this waits on.

const run = (command, argv, env) =>
  new Promise((resolve) => {
    const child = spawn(command, argv, { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", (d) => { output += d; process.stdout.write(`    │ ${d}`.replace(/\n(?!$)/g, "\n    │ ")); });
    child.stderr.on("data", (d) => { output += d; });
    child.on("close", (code) => resolve({ code, output }));
  });

const SCENARIOS = {
  /** Nothing injected: what the stack costs while it is only doing its job. */
  idle: async () => {
    process.stdout.write(`  idle for ${SECONDS}s\n`);
    await sleep(SECONDS * 1000);
    return { code: 0, output: "" };
  },
  /** The full incident, driven by the thing that already asserts it. */
  demo: () =>
    run("node", ["packages/demo/src/driver.ts"], {
      AGGREGATOR: "http://aggregator:8088,http://aggregator-2:8088",
      FAILURE_MODE: "envoy",
      FLAKY_UPSTREAM: "http://flaky-upstream",
      PROMETHEUS,
    }),
  "chaos:leader": () => run("node", ["infra/chaos.mjs", "leader"], {}),
  "chaos:prober": () => run("node", ["infra/chaos.mjs", "prober"], {}),
};

// What the system was doing while it cost that.

const promRange = async (query, startMs, endMs) => {
  const url =
    `${PROMETHEUS}/api/v1/query_range?query=${encodeURIComponent(query)}` +
    `&start=${Math.floor(startMs / 1000)}&end=${Math.ceil(endMs / 1000)}&step=5`;
  const res = await fetch(url).catch(() => null);
  if (!res?.ok) return null;
  const body = await res.json();
  const values = (body.data?.result ?? []).flatMap((s) => s.values.map((v) => Number(v[1])));
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0) return null;
  return { max: Math.max(...finite), min: Math.min(...finite), last: finite.at(-1) };
};

const DOMAIN = {
  ticks_per_second: "sum(rate(egress_aggregator_ticks_total[1m]))",
  // The per-queue series comes from rabbitmq_prometheus' detailed endpoint —
  // plain `rabbitmq_queue_messages` is the broker total and carries no queue
  // label, which reads as "the backlog never moved" on a stack with two queues.
  work_queue_depth: 'rabbitmq_detailed_queue_messages{queue="payments-provider.work"}',
  dead_letter_depth: 'rabbitmq_detailed_queue_messages{queue="payments-provider.work.dead"}',
  target_fraction: "max(egress_daemon_target_fraction)",
  daemons_pulling: 'sum(rabbitmq_detailed_queue_consumers{queue="payments-provider.work"})',
  floor_held: "sum(egress_daemon_floor_held)",
  egress_calls_per_second: "sum(rate(egress_daemon_calls_total[1m]))",
};

// ---------------------------------------------------------------------------

const fmtBytes = (n) => `${(n / 2 ** 20).toFixed(0)} MiB`;
const pct = (a, b) => (b ? `${((a / b) * 100).toFixed(0)}%` : "—");

const main = async () => {
  const scenario = SCENARIOS[SCENARIO];
  if (!scenario) {
    console.error(`unknown scenario "${SCENARIO}" — one of: ${Object.keys(SCENARIOS).join(", ")}`);
    process.exit(2);
  }

  const info = await api("/info");
  const version = await api("/version");
  const host = {
    cpus: info.NCPU,
    memory: info.MemTotal,
    kernel: info.KernelVersion,
    arch: version.Arch,
    engine: version.Version,
  };

  const found = await containers();
  if (found.length === 0) {
    console.error(`no containers in compose project "${PROJECT}" — is the stack up?`);
    process.exit(2);
  }

  // The envelope, before anything is measured inside it.
  const limits = new Map();
  for (const c of found) limits.set(c.name, await limitsOf(c.id));
  const unlimited = [...limits].filter(([, l]) => !l.cpus || !l.memory).map(([n]) => n);

  console.log(
    `host ${host.cpus} cpus / ${fmtBytes(host.memory)} · engine ${host.engine} ${host.arch}\n` +
      `project ${PROJECT}: ${found.length} containers, ` +
      `${found.length - unlimited.length} within declared limits`,
  );
  if (unlimited.length > 0 && !ALLOW_UNLIMITED) {
    console.error(
      `\n${unlimited.length} container(s) have no cpu and memory limit, so this run is not\n` +
        `repeatable on another machine. Add deploy.resources.limits in docker-compose.yml,\n` +
        `or pass --allow-unlimited to size them in the first place:\n` +
        unlimited.map((n) => `  ${n}`).join("\n"),
    );
    process.exit(2);
  }

  // Attach before the scenario starts, so the pre-incident seconds are in the
  // window too — the ramp back is what costs, and it is only legible against
  // what the same containers cost while nothing was wrong.
  const tracked = new Map();
  const attach = (c) => {
    if (tracked.has(c.name)) return;
    const t = track(c.name, c.service);
    tracked.set(c.name, t);
    streamStats(c.id, (s) => t.record(s));
  };
  found.forEach(attach);

  // Containers that appear mid-run are instrumented too.
  const rescan = setInterval(() => {
    containers().then((cs) => cs.forEach(attach)).catch(() => {});
  }, 5000);

  console.log(`\n== ${SCENARIO} ==`);
  const startedAt = Date.now();
  await sleep(3000); // a few samples of the stack at rest, first
  const outcome = await scenario();
  await sleep(3000); // and a few after, so the tail of the ramp is in the window
  const endedAt = Date.now();
  clearInterval(rescan);
  for (const req of streams) req.destroy();

  const after = new Map();
  for (const c of await containers()) after.set(c.name, await limitsOf(c.id));

  const domain = {};
  for (const [key, query] of Object.entries(DOMAIN)) {
    domain[key] = await promRange(query, startedAt, endedAt);
  }

  const services = [...tracked.values()]
    .filter((t) => t.samples > 0)
    .map((t) => {
      const limit = limits.get(t.name) ?? after.get(t.name) ?? {};
      const end = after.get(t.name);
      const before = limits.get(t.name);
      return {
        container: t.name,
        service: t.service,
        samples: t.samples,
        peakCpus: Number(t.peakCpus.toFixed(3)),
        meanCpus: Number((t.cpuSeconds / ((endedAt - startedAt) / 1000)).toFixed(3)),
        cpuSeconds: Number(t.cpuSeconds.toFixed(1)),
        peakMemory: t.peakMemory,
        limitCpus: limit.cpus ?? null,
        limitMemory: limit.memory ?? null,
        throttledPeriods: t.throttled?.periods ?? 0,
        throttledOf: t.throttled?.ofPeriods ?? 0,
        oomKilled: end?.oomKilled === true,
        restarted: end && before ? end.restarts - before.restarts : 0,
      };
    })
    .sort((a, b) => b.peakCpus - a.peakCpus);

  // ---- the report -------------------------------------------------------

  const head = ["container", "cpus peak", "of limit", "mem peak", "of limit", "throttled"];
  const rows = services.map((s) => [
    s.container.replace(`${PROJECT}-`, ""),
    `${s.peakCpus.toFixed(2)} (${s.meanCpus.toFixed(2)})`,
    pct(s.peakCpus, s.limitCpus),
    fmtBytes(s.peakMemory),
    pct(s.peakMemory, s.limitMemory),
    s.throttledPeriods > 0 ? `${s.throttledPeriods}/${s.throttledOf}` : "—",
  ]);
  const widths = head.map((h, i) => Math.max(h.length, ...rows.map((r) => r[i].length)));
  const line = (cells) => cells.map((c, i) => (i === 0 ? c.padEnd(widths[i]) : c.padStart(widths[i]))).join("  ");
  console.log(`\n${line(head)}\n${widths.map((w) => "─".repeat(w)).join("  ")}`);
  for (const r of rows) console.log(line(r));
  console.log(
    `\npeak is the highest one-second window; the number in brackets is the mean over ` +
      `${((endedAt - startedAt) / 1000).toFixed(0)}s.`,
  );

  // Only seconds in which every tracked container reported, so the stack peak
  // is not a second in which half of them happened not to have been sampled.
  const full = [...timeline.values()].filter((slot) => slot.containers >= services.length);
  const stack = {
    peakCpus: full.length > 0 ? Number(Math.max(...full.map((s) => s.cpus)).toFixed(2)) : null,
    peakMemory: full.length > 0 ? Math.max(...full.map((s) => s.memory)) : null,
    limitCpus: Number(services.reduce((n, s) => n + (s.limitCpus ?? 0), 0).toFixed(2)),
    limitMemory: services.reduce((n, s) => n + (s.limitMemory ?? 0), 0),
    seconds: full.length,
  };
  if (stack.peakCpus !== null) {
    console.log(
      `\nthe stack, in its busiest single second: ${stack.peakCpus.toFixed(2)} cpus and ` +
        `${fmtBytes(stack.peakMemory)} across ${services.length} containers — ` +
        `against ${stack.limitCpus.toFixed(2)} cpus and ${fmtBytes(stack.limitMemory)} of declared ceilings.`,
    );
  }

  const measured = Object.entries(domain).filter(([, v]) => v !== null);
  if (measured.length > 0) {
    console.log("\nwhat it was doing, from Prometheus over the same window:");
    for (const [key, v] of measured) {
      console.log(`  ${key.padEnd(24)} max ${v.max.toFixed(2).padStart(10)}   final ${v.last.toFixed(2)}`);
    }
  }

  // ---- was this a run worth quoting? ------------------------------------

  const problems = [];
  if (unlimited.length > 0) problems.push(`${unlimited.length} container(s) ran without limits`);
  const throttling = [];
  for (const s of services) {
    if (s.throttledPeriods > 0) {
      const share = (s.throttledPeriods / Math.max(1, s.throttledOf)) * 100;
      const belowFloor = s.throttledOf < MIN_THROTTLE_PERIODS;
      const line =
        `${s.container} throttled in ${s.throttledPeriods} of ${s.throttledOf} periods (${share.toFixed(1)}%)` +
        (belowFloor ? ` — under the ${MIN_THROTTLE_PERIODS}-period floor, not judged` : "");
      throttling.push(line);
      if (!belowFloor && share > THROTTLE_BUDGET) problems.push(`${line} — over the ${THROTTLE_BUDGET}% budget`);
    }
    if (s.oomKilled) problems.push(`${s.service} was OOM-killed`);
  }
  if (throttling.length > 0) {
    console.log(`\nthe kernel clipped a sub-second burst in ${throttling.length} container(s); the budget is ${THROTTLE_BUDGET}% of periods:`);
    for (const line of throttling) console.log(`  ${line}`);
  }
  if (outcome.code !== 0) problems.push(`the ${SCENARIO} scenario exited ${outcome.code}`);

  const record = {
    scenario: SCENARIO,
    startedAt: new Date(startedAt).toISOString(),
    seconds: Number(((endedAt - startedAt) / 1000).toFixed(1)),
    host,
    project: PROJECT,
    limitsDeclared: unlimited.length === 0,
    scenarioExit: outcome.code,
    throttleBudget: THROTTLE_BUDGET,
    minThrottlePeriods: MIN_THROTTLE_PERIODS,
    services,
    stack,
    domain,
    problems,
  };
  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, `${JSON.stringify(record, null, 2)}\n`);
  console.log(`\nrecord: ${OUT}`);

  if (BASELINE) compare(record, JSON.parse(readFileSync(BASELINE, "utf8")));

  if (problems.length > 0) {
    console.error(`\nnot a repeatable run:\n${problems.map((p) => `  ${p}`).join("\n")}`);
    process.exit(1);
  }
  console.log("\nevery container stayed inside its declared limits");
};

/** Two records compared by shape: a service that doubled is a regression, not a different laptop. */

/** Folded by service: which daemon replica runs the redrive differs between runs. */
const byService = (services) => {
  const folded = new Map();
  for (const s of services) {
    const prior = folded.get(s.service);
    folded.set(s.service, {
      replicas: (prior?.replicas ?? 0) + 1,
      peakCpus: Math.max(prior?.peakCpus ?? 0, s.peakCpus),
      peakMemory: Math.max(prior?.peakMemory ?? 0, s.peakMemory),
      cpuSeconds: (prior?.cpuSeconds ?? 0) + s.cpuSeconds,
    });
  }
  return folded;
};

const compare = (now, was) => {
  console.log(
    `\nagainst ${basename(BASELINE)} (${was.host.cpus} cpus, ${was.host.arch}, ${was.startedAt.slice(0, 10)}):`,
  );
  if (was.scenario !== now.scenario) {
    console.log(`  different scenario (${was.scenario}) — not comparable`);
    return;
  }

  // Percentages of something too small to matter are noise dressed as signal:
  // grafana idling at 0.004 cpus and 0.0008 the next time is an 80% fall and
  // means nothing at all. Below these, a service has no reading to compare.
  const CPU_FLOOR = 0.05;
  const MEMORY_FLOOR = 32 * 2 ** 20;
  const MOVED = 25;

  const before = byService(was.services);
  let said = false;
  for (const [service, s] of byService(now.services)) {
    const b = before.get(service);
    if (!b) continue;
    const parts = [];
    if (b.replicas !== s.replicas) parts.push(`${b.replicas} → ${s.replicas} replicas`);
    const cpu = ((s.peakCpus - b.peakCpus) / b.peakCpus) * 100;
    const memory = ((s.peakMemory - b.peakMemory) / b.peakMemory) * 100;
    if (b.peakCpus >= CPU_FLOOR && Math.abs(cpu) >= MOVED) parts.push(`peak cpu ${cpu >= 0 ? "+" : ""}${cpu.toFixed(0)}%`);
    if (b.peakMemory >= MEMORY_FLOOR && Math.abs(memory) >= MOVED) parts.push(`peak mem ${memory >= 0 ? "+" : ""}${memory.toFixed(0)}%`);
    if (parts.length === 0) continue;
    console.log(`  ${service.padEnd(18)} ${parts.join("   ")}`);
    said = true;
  }
  if (!said) console.log("  nothing moved by more than a quarter");
};

await main();
