/**
 * What this stack costs, measured automatically, inside an envelope that makes
 * two runs comparable.
 *
 *   node infra/instrument.mjs                  # the demo scenario, the default
 *   node infra/instrument.mjs idle --seconds=60
 *   node infra/instrument.mjs chaos:leader
 *   node infra/instrument.mjs demo --baseline history/runs/baseline-demo-2026-09-12.json
 *
 * Every resource number this repo has published so far came from a human
 * reading `docker stats` on one machine, on a stack with no limits on it. Both
 * halves of that are the problem. The reading is a moment rather than a run,
 * and an unbounded container is sized by whatever the host had spare — so the
 * same scenario on a 16-core workstation and a 4-core laptop produce numbers
 * that cannot be compared, and neither of them is wrong.
 *
 * So two things here, and the second is what makes the first mean anything:
 *
 * 1. **Instrumentation.** Docker's own stats stream, one sample per second per
 *    container, over the Engine API rather than the CLI — the API carries the
 *    cgroup counters `docker stats` formats away, including the one that says
 *    whether the measurement is trustworthy at all (`throttling_data`).
 * 2. **An envelope.** Every service in docker-compose.yml declares
 *    `deploy.resources.limits`, and this refuses to produce a record from a
 *    stack that does not. A number measured against an unbounded container is
 *    a number about the host.
 *
 * It exits non-zero when the run was not repeatable — a service without
 * limits, a container the kernel throttled past its budget, an OOM kill, or a
 * scenario that failed — because those are the conditions under which the
 * numbers it printed should not be quoted. Records land in history/runs/.
 *
 * Runs from the devcontainer against `docker compose up -d`: it needs the
 * Docker socket and it reaches the stack by service name, exactly like
 * infra/chaos.mjs.
 */

import http from "node:http";
import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, basename } from "node:path";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const args = process.argv.slice(2);
/**
 * `--name=value` and `--name value` both, because the second is what anyone
 * types and silently taking `true` for the path they meant crashes after the
 * run rather than before it — three minutes of stack time already paid for. A
 * boolean default marks the flags that take no value, so a scenario name
 * standing after one is not eaten as its argument.
 */
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
/**
 * How much CPU throttling a run may contain and still be worth quoting, as a
 * percentage of the container's scheduling periods.
 *
 * Not zero, and that took a measurement to accept. The cgroup quota is
 * enforced per 100ms period, so a container whose busiest *second* is 0.23
 * cpus can still exceed a 0.5 cpu quota inside one of those periods — the
 * traffic generator did, five times in 428, against a ceiling more than twice
 * its measured peak. Sub-second bursts are what a quota clips first, and no
 * headroom short of "no limit at all" removes them entirely.
 *
 * So the question is not whether the kernel ever intervened but whether it
 * intervened enough to be what the numbers are about. Under a percent, with
 * the scenario still passing, it is not.
 */
const THROTTLE_BUDGET = Number(flag("throttle-budget", 1));
/**
 * Below this many observed periods, a throttle share is noise rather than
 * signal and is not flagged as a budget violation, whatever the percentage.
 *
 * `throttledOf` is the number of CFS periods the container was even
 * scheduled in during the window; a near-idle service (alertmanager in the
 * demo scenario, say) can have only one or two, so a single throttle inside
 * a tiny burst reads as 100% throttled. At the default 100ms period, 20
 * periods is 2 seconds of runnable time — enough that a percentage over it
 * says something about sustained pressure rather than one unlucky period.
 */
const MIN_THROTTLE_PERIODS = Number(flag("min-throttle-periods", 20));
/** The one way to get a record out of an unbounded stack: how the limits in
 *  docker-compose.yml were sized in the first place. It stamps the record. */
const ALLOW_UNLIMITED = flag("allow-unlimited", false) === true;
const OUT = flag(
  "out",
  `history/runs/${SCENARIO.replace(":", "-")}-${new Date().toISOString().replace(/[:.]/g, "-")}.json`,
);

// ---------------------------------------------------------------------------
// The Engine API, over the socket. No dependency, and it carries more than the
// CLI prints.
// ---------------------------------------------------------------------------

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

/**
 * Docker's stats stream: newline-delimited JSON, one object per second, pushed
 * rather than polled. Polling `docker stats --no-stream` in a loop costs a
 * process per sample and still samples on *our* clock; this samples on the
 * engine's, and the object carries the timestamp it was taken at.
 */
/** Every open stats stream, so the process can end when the run does — Docker
 *  streams until the client hangs up, and an unclosed one keeps node's event
 *  loop alive long after the report has been printed. */
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

/**
 * CPU as cores, from the cumulative counter and the sample's own wall clock —
 * not from Docker's `system_cpu_usage` ratio, which is scaled by the number of
 * CPUs the *host* has and therefore reports a different number for identical
 * work on a bigger machine. Cores used is the unit a limit is written in, so
 * it is the unit the measurement has to be in for the two to be comparable.
 */
/**
 * Summing each container's peak overstates the stack badly: the daemon that
 * runs the redrive and the broker that feeds it do not peak in the same
 * second. Samples are bucketed by the second they were taken in, so the
 * stack's own peak is a real instant rather than an arithmetic one — and that
 * is the number that answers "what machine does this fit on".
 */
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

    // The cgroup's throttle counters are cumulative since the container
    // started, so what this run did is the difference against the first sample
    // taken. Nothing else here says whether the envelope distorted the
    // measurement, and an unnoticed throttle makes every number below a
    // property of the limit rather than of the work.
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

// ---------------------------------------------------------------------------
// Scenarios. Each one is a child process this waits on, so what is being
// measured is a thing that already exists rather than a re-implementation.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The domain side of the same run. Container CPU says what it cost; these say
// what it was doing while it cost that, which is the half that makes a number
// worth keeping.
// ---------------------------------------------------------------------------

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

  // Containers that appear during the run — the one-shot demo container, a
  // replacement for one a chaos scenario killed — are instrumented too. This
  // is the difference between measuring the stack and measuring a list of
  // containers written down before it started.
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

/**
 * Two records of the same scenario, side by side. This is what the envelope
 * buys: on a different machine the absolute numbers still move — a faster core
 * does the same work in less CPU time — but the shape has to hold, and a
 * service that has doubled is a regression rather than a different laptop.
 */

/**
 * Folded by service, not by container. `rmq-daemon` is five containers and
 * comparing them positionally compares nothing: which replica runs the redrive
 * is the broker's choice and differs between runs, so replica 1 against
 * replica 1 reports a 75% swing on two runs that were identical. The fleet's
 * busiest replica against the other run's busiest replica is the comparison
 * that means something.
 */
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
