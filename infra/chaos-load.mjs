/**
 * Chaos under load spikes: process, network and flaky-service faults, each
 * injected at the start of a traffic spike, each run judged first on
 * correctness — no message lost, dead-letter queue empty — and then on how the
 * circuit breaker behaved.
 *
 *   node infra/chaos-load.mjs                                    # every fault × every profile
 *   node infra/chaos-load.mjs --profiles=low --faults=flaky-full-cycle,kill-broker
 *   node infra/chaos-load.mjs --profiles=high --monkey=600       # random faults for ten minutes
 *   node infra/chaos-load.mjs --list
 *
 * A profile is a base rate and a spike rate. All traffic comes from one forked
 * publisher (infra/chaos-publisher.mjs) — the compose producer is stopped for the
 * run — and a spike is a rate change: it begins the instant a fault is injected
 * (and at every step of a journey) and repeats, on for --spike-on seconds and off
 * for --spike-off, until the stack has recovered.
 *
 * Correctness is judged per message, not from broker counters: the management
 * API's publish/ack totals are summed from channel stats that lose their last
 * collection interval whenever a channel closes, and every producer, redrive
 * pass and retired consumer closes channels. Instead every message carries
 * `x-idempotency-key: <run>:<n>`; the publisher reports exactly which n the
 * broker confirmed, the flaky upstream reports which n it answered 200, and
 *
 *   lost = confirmed ∧ ¬processed ∧ ¬(still in the work or dead-letter queue)
 *
 * must be empty, as must the dead-letter queue once the scenario has drained.
 * The same audit counts duplicate calls exactly.
 *
 * The breaker is read from the aggregators' event tape (`/api/events/stream`),
 * not by polling, because a HALF_OPEN that succeeds lasts under a second. Every
 * transition is checked against the reducer's table and its timers.
 */

import http from "node:http";
import { execFile, fork, spawnSync } from "node:child_process";
import { promisify } from "node:util";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const exec = promisify(execFile);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const hit = args.find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return fallback;
  return hit.includes("=") ? hit.slice(name.length + 3) : true;
};

const PROFILES = {
  low: { rate: 200, spike: 3000 },
  high: { rate: 3000, spike: 9000 },
  "near-ceiling": { rate: 8000, spike: 16000 },
};

const SETTLE = Number(flag("settle", 10));
const FAULT_SECONDS = Number(flag("fault-seconds", 30));
const HOLD = Number(flag("hold", 10));
const SPIKE_ON = Number(flag("spike-on", 15));
const SPIKE_OFF = Number(flag("spike-off", 15));
const RECOVERY_TIMEOUT = Number(flag("recovery-timeout", 180));
const DRAIN_TIMEOUT = Number(flag("drain-timeout", 300));
const DEAD_LETTER_TIMEOUT = Number(flag("dead-letter-timeout", 120));
const FLEET_SIZE = Number(flag("fleet", 5));
const MONKEY = Number(flag("monkey", 0));
const SUSPEND_TOLERANCE = Number(flag("suspend-tolerance", 5));
const PURGE = flag("purge-dead-letters", false) === true;
const PROJECT = process.env.COMPOSE_PROJECT_NAME ?? "workspace";
const RABBIT = "http://rabbitmq:15672";
const RABBIT_AUTH = `Basic ${Buffer.from("guest:guest").toString("base64")}`;
const WORK = "payments-provider.work";
const DEAD = "payments-provider.work.dead";
const PARKED = "payments-provider.work.parked";
const API = "payments-provider";
const AGGREGATORS = [
  { container: `${PROJECT}-aggregator-1`, url: "http://aggregator:8088" },
  { container: `${PROJECT}-aggregator-2-1`, url: "http://aggregator-2:8088" },
];
const PAYMENT_PORTS = [8080, 8081, 8082, 8083, 8084, 8085];
const OUT = flag("out", `history/runs/chaos-load-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

/** The reducer's table (packages/domain/src/Breaker.ts), as edges with the reasons each may carry. */
const LEGAL = {
  "CLOSED→DEGRADED": ["OUTLIER_EJECTION", "THRESHOLD_OVERFLOW"],
  "CLOSED→OPEN": ["OUTLIER_EJECTION", "ALL_ENDPOINTS_EJECTED"],
  "DEGRADED→CLOSED": ["HEALTHY"],
  "DEGRADED→OPEN": ["OUTLIER_EJECTION", "ALL_ENDPOINTS_EJECTED"],
  "OPEN→HALF_OPEN": ["OPEN_TIMEOUT_ELAPSED"],
  "HALF_OPEN→CLOSED": ["PROBE_SUCCEEDED"],
  "HALF_OPEN→OPEN": ["PROBE_FAILED"],
  "HALF_OPEN→DEGRADED": ["OUTLIER_EJECTION", "THRESHOLD_OVERFLOW"],
};
/** DEFAULT_CONFIG in packages/domain/src/Model.ts; the aggregator ticks every 250ms. */
const TIMERS = { minStateMs: 3000, openMs: 4000, maxOpenMs: 16000, tickSlackMs: 400 };

// ---- reading the system ----------------------------------------------------

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

const text = (url) =>
  fetch(url, { signal: AbortSignal.timeout(2000) })
    .then((r) => r.text())
    .catch(() => null);
const json = (url, init = {}) =>
  fetch(url, { ...init, signal: AbortSignal.timeout(5000) })
    .then((r) => (r.ok ? r.json() : null))
    .catch(() => null);

const one = (body, name, match = {}) => {
  const line = (body ?? "")
    .split("\n")
    .find(
      (l) =>
        (l.startsWith(`${name}{`) || l.startsWith(`${name} `)) &&
        Object.entries(match).every(([k, v]) => l.includes(`${k}="${v}"`)),
    );
  return line ? Number(line.slice(line.lastIndexOf(" ") + 1)) : null;
};

const queue = async (name) => {
  const q = await json(`${RABBIT}/api/queues/%2F/${encodeURIComponent(name)}`, { headers: { authorization: RABBIT_AUTH } });
  if (!q) return null;
  const s = q.message_stats ?? {};
  return { depth: q.messages ?? 0, unacked: q.messages_unacknowledged ?? 0, consumers: q.consumers ?? 0, publish: s.publish ?? 0, ack: s.ack ?? 0, redeliver: s.redeliver ?? 0 };
};

const projectContainers = async () =>
  (await docker("/containers/json?all=true"))
    .filter((c) => c.Labels?.["com.docker.compose.project"] === PROJECT)
    .map((c) => ({ id: c.Id, name: c.Names[0].slice(1), service: c.Labels["com.docker.compose.service"] }));

const inspect = (id) => docker(`/containers/${id}/json`);

const daemons = async () =>
  Promise.all(
    (await projectContainers())
      .filter((c) => c.service === "rmq-daemon")
      .map(async (c) => {
        const d = await inspect(c.id);
        return { name: c.name, id: d.Config.Hostname, running: d.State.Running && !d.State.Paused, ip: d.NetworkSettings.Networks?.devcontainer?.IPAddress || null };
      }),
  );

const DAEMON_STATE = ["CLOSED", "DEGRADED", "OPEN", "HALF_OPEN"];

const fleet = async () =>
  Promise.all(
    (await daemons()).map(async (d) => {
      const body = d.ip && d.running ? await text(`http://${d.ip}:9464/metrics`) : null;
      const m = { daemon: d.id };
      const circuit = one(body, "egress_daemon_circuit_state", m);
      return {
        ...d,
        reachable: body !== null,
        circuit,
        state: DAEMON_STATE[circuit] ?? null,
        floor: one(body, "egress_daemon_floor_held", m),
        ok: one(body, "egress_daemon_calls_total", { ...m, outcome: "ok" }),
        failed: one(body, "egress_daemon_calls_total", { ...m, outcome: "failed" }),
        gaps: one(body, "egress_daemon_control_gaps_total", m),
        duplicates: one(body, "egress_daemon_control_duplicates_total", m),
      };
    }),
  );

const aggregators = () =>
  Promise.all(
    AGGREGATORS.map(async (a) => {
      const [metrics, state, subscriber] = await Promise.all([text(`${a.url}/metrics`), json(`${a.url}/api/state`), json(`${a.url}/api/subscriber`)]);
      const api = state?.apis?.find((x) => x.apiId === API);
      return {
        ...a,
        leader: one(metrics, "egress_aggregator_is_leader") === 1,
        state: api?.state ?? null,
        sequence: api?.sequence ?? null,
        received: subscriber?.received ?? null,
        subscriberDuplicates: subscriber?.duplicates ?? null,
      };
    }),
  );

const restarts = async () => {
  const out = new Map();
  for (const c of await projectContainers()) {
    const d = await inspect(c.id);
    out.set(c.name, { restarts: d.RestartCount ?? 0, startedAt: d.State.StartedAt, oom: d.State.OOMKilled === true });
  }
  return out;
};

/**
 * Every state change the aggregators publish for this API, deduplicated by
 * sequence. Both instances are read, because only the leader publishes and a
 * scenario may kill it; the standby's stream is already open when it takes over.
 */
const tape = (() => {
  const events = [];
  const seen = new Set();
  let running = true;
  const controllers = [];
  const follow = async (url) => {
    while (running) {
      const ctl = new AbortController();
      controllers.push(ctl);
      try {
        const res = await fetch(`${url}/api/events/stream`, { signal: ctl.signal });
        let buffer = "";
        for await (const chunk of res.body) {
          buffer += Buffer.from(chunk).toString("utf8");
          let at;
          while ((at = buffer.indexOf("\n\n")) >= 0) {
            const frame = buffer.slice(0, at);
            buffer = buffer.slice(at + 2);
            const data = frame.split("\n").find((l) => l.startsWith("data: "));
            if (!data) continue;
            const e = JSON.parse(data.slice(6));
            if (e.type !== "egress.circuit.state_changed" || e.data?.apiId !== API || seen.has(e.data.sequence)) continue;
            seen.add(e.data.sequence);
            events.push({ sequence: e.data.sequence, from: e.data.previousState, to: e.data.state, reason: e.data.reason, at: Date.parse(e.time), receivedAt: Date.now() });
            events.sort((a, b) => a.sequence - b.sequence);
          }
        }
      } catch {
        // A killed aggregator ends its stream; reconnect.
      }
      await sleep(1000);
    }
  };
  AGGREGATORS.forEach((a) => follow(a.url));
  return {
    events,
    between: (from, to) => events.filter((e) => e.receivedAt >= from && e.receivedAt <= to),
    stop: () => {
      running = false;
      controllers.forEach((c) => c.abort());
    },
  };
})();

// ---- acting on the system --------------------------------------------------

const cleanups = [];
const onCleanup = (fn) => cleanups.push(fn);
const runCleanups = async () => {
  while (cleanups.length) await cleanups.pop()().catch((e) => console.log(`  cleanup failed: ${e.message}`));
};

const composeProducer = (argv, rate) => {
  const env = { ...process.env };
  if (rate === null) delete env.RATE_PER_SECOND;
  else env.RATE_PER_SECOND = String(rate);
  const r = spawnSync("docker", ["compose", ...argv], { env, encoding: "utf8" });
  if (r.status !== 0) throw new Error(`docker compose ${argv.join(" ")}: ${r.stderr}`);
};
const producerOn = (rate) => composeProducer(["up", "-d", "--no-deps", "rmq-producer"], rate);
const producerOff = () => composeProducer(["stop", "rmq-producer"], null);

/**
 * All traffic for a scenario comes from one forked publisher (infra/chaos-publisher.mjs),
 * so the harness knows exactly which messages the broker confirmed. A spike is a
 * rate change: `kick` starts one now, and they repeat --spike-on/--spike-off until paused.
 */
const load = (profile, runId) => {
  const child = fork(new URL("./chaos-publisher.mjs", import.meta.url), [], {
    env: { ...process.env, RUN_ID: runId, QUEUE: WORK, API_ID: API },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr.on("data", (d) => (stderr = (stderr + d).slice(-600)));
  const stats = { spikes: 0, disconnectedSeconds: 0, last: null };
  child.on("message", (m) => {
    if (m.type !== "stats") return;
    stats.last = m;
    if (!m.connected) stats.disconnectedSeconds++;
  });
  const exited = new Promise((r) => child.on("exit", (code) => r(code)));
  const setRate = (rate) => child.connected && child.send({ type: "rate", rate });
  let on = false;
  let active = false;
  let closed = false;
  let toggleAt = 0;
  setRate(profile.rate);
  const loop = (async () => {
    while (!closed) {
      if (active && Date.now() >= toggleAt) {
        on = !on;
        toggleAt = Date.now() + (on ? SPIKE_ON : SPIKE_OFF) * 1000;
        setRate(on ? profile.spike : profile.rate);
        if (on) stats.spikes++;
      }
      await sleep(250);
    }
  })();
  return {
    stats,
    kick: async () => {
      active = true;
      on = true;
      toggleAt = Date.now() + SPIKE_ON * 1000;
      stats.spikes++;
      setRate(profile.spike);
    },
    pause: async () => {
      active = false;
      on = false;
      setRate(0);
    },
    /** Stops publishing, waits for outstanding confirms, returns the confirmed bitmap. */
    finish: async () => {
      closed = true;
      active = false;
      await loop;
      const final = new Promise((r) => child.on("message", (m) => m.type === "final" && r(m)));
      if (child.connected) child.send({ type: "stop" });
      const result = await Promise.race([final, exited.then((code) => ({ error: `publisher exited ${code}: ${stderr.trim()}` }))]);
      child.kill();
      return result;
    },
    kill: async () => void child.kill("SIGKILL"),
  };
};

const bitsOf = (b64) => Buffer.from(b64 ?? "", "base64");
const hasBit = (buf, i) => i >> 3 < buf.length && (buf[i >> 3] & (1 << (i & 7))) !== 0;

/** Idempotency keys of this run's messages sitting in a queue, read without consuming them. */
const keysIn = async (name, runId, depth) => {
  if (!depth) return new Set();
  const res = await fetch(`${RABBIT}/api/queues/%2F/${encodeURIComponent(name)}/get`, {
    method: "POST",
    headers: { authorization: RABBIT_AUTH, "content-type": "application/json" },
    body: JSON.stringify({ count: depth, ackmode: "ack_requeue_true", encoding: "auto", truncate: 1000 }),
  }).catch(() => null);
  const msgs = res?.ok ? await res.json() : [];
  return new Set(
    msgs
      .map((m) => m.properties?.headers?.["x-idempotency-key"])
      .filter((k) => typeof k === "string" && k.startsWith(`${runId}:`))
      .map((k) => Number(k.slice(runId.length + 1))),
  );
};

/** Tell each payments endpoint how to behave; endpoints not listed are healthy. */
const setUpstream = async (ports, behaviour) => {
  for (const port of PAYMENT_PORTS) {
    await fetch(`http://flaky-upstream:${port}/__fail`, { method: "POST", body: JSON.stringify(ports.includes(port) ? behaviour : {}) }).catch(() => {});
  }
};
const healUpstream = () => setUpstream([], {});

const waitUntil = async (predicate, timeoutMs, everyMs = 1000) => {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (await predicate().catch(() => false)) return Date.now() - t0;
    await sleep(everyMs);
  }
  return null;
};

const brokerHealthy = async () => {
  const r = await exec("docker", ["inspect", "-f", "{{.State.Health.Status}}", `${PROJECT}-rabbitmq-1`]).catch(() => null);
  return r?.stdout.trim() === "healthy";
};

const pick = (xs) => xs[Math.floor(Math.random() * xs.length)];

// ---- the network, from inside a container's own namespace --------------------

const NETSHOOT = "nicolaka/netshoot:v0.14";
const ipsOf = async (container) =>
  Object.values((await inspect(container)).NetworkSettings.Networks ?? {})
    .map((n) => n.IPAddress)
    .filter(Boolean);
const inNetns = (container, script) => exec("docker", ["run", "--rm", "--net", `container:${container}`, "--cap-add", "NET_ADMIN", NETSHOOT, "sh", "-c", script]);
const IFACES = "for i in $(ls /sys/class/net | grep -v '^lo$'); do";

/** Every container here sits on two networks, so a rule must match both of a peer's addresses. */
const netem = async (from, to, ports, spec) => {
  const dsts = (await Promise.all(to.map(ipsOf))).flat();
  const filters = dsts.flatMap((ip) => ports.map((p) => `tc filter add dev $i parent 1: protocol ip prio 1 u32 match ip dst ${ip}/32 match ip dport ${p} 0xffff flowid 1:3;`)).join(" ");
  await Promise.all(
    from.map((c) => inNetns(c, `${IFACES} tc qdisc add dev $i root handle 1: prio bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 && tc qdisc add dev $i parent 1:3 handle 30: netem ${spec} && ${filters} done`)),
  );
  return () => Promise.all(from.map((c) => inNetns(c, `${IFACES} tc qdisc del dev $i root 2>/dev/null; done; true`).catch(() => {})));
};

const firewall = async (from, to, ports, action) => {
  const dsts = (await Promise.all(to.map(ipsOf))).flat();
  const target = action === "reset" ? "REJECT --reject-with tcp-reset" : "DROP";
  const rules = dsts.flatMap((ip) => ports.map((p) => `iptables -A CHAOS -p tcp -d ${ip} --dport ${p} -j ${target};`)).join(" ");
  const setup = `iptables -N CHAOS 2>/dev/null; iptables -C OUTPUT -j CHAOS 2>/dev/null || iptables -I OUTPUT -j CHAOS; ${rules}`;
  const teardown = "iptables -F CHAOS 2>/dev/null; iptables -D OUTPUT -j CHAOS 2>/dev/null; iptables -X CHAOS 2>/dev/null; true";
  const apply = () => Promise.all(from.map((c) => inNetns(c, setup)));
  const heal = () => Promise.all(from.map((c) => inNetns(c, teardown).catch(() => {})));
  await apply();
  return { heal, apply };
};

const ENVOYS = ["envoy-00", "envoy-01", "envoy-02"].map((s) => `${PROJECT}-${s}-1`);
const UPSTREAM = [`${PROJECT}-flaky-upstream-1`];
const BROKER = [`${PROJECT}-rabbitmq-1`];
const REDIS = [`${PROJECT}-redis-1`];
const daemonContainers = async () => (await daemons()).filter((d) => d.running).map((d) => d.name);
const leaderContainer = async () => {
  const leader = (await aggregators()).find((a) => a.leader);
  if (!leader) throw new Error("no leader");
  return leader.container;
};

// ---- faults ------------------------------------------------------------------

/** Registers the heal as it is built, so a crash of this script still undoes the fault. */
const fault = (expect, build) => async (ctx) => {
  const built = await build(ctx);
  onCleanup(built.heal);
  return { victims: [], expect, ...built };
};

const withOutage = (detail, cut) =>
  fault("fleet-open", async () => {
    const { heal, what } = await cut();
    await setUpstream(PAYMENT_PORTS, { rate: 1 });
    return { detail: `${what} ${detail}`, heal: async () => (await healUpstream(), await heal()) };
  });

/**
 * A journey drives the flaky upstream through a sequence of behaviours and waits,
 * after each, for the transitions that behaviour must produce, in order. Every
 * step starts a fresh spike. `quiet` asserts no transition at all for that long.
 */
const journey = (steps, { forbid = [] } = {}) =>
  fault("journey", async () => ({
    detail: `${steps.length} steps`,
    forbid,
    heal: healUpstream,
    during: async (_ms, ctx) => {
      const results = [];
      for (const step of steps) {
        await ctx.spikes.kick();
        const startedAt = Date.now();
        await (step.set === "heal" ? healUpstream() : setUpstream(step.set[0], step.set[1]));
        const row = { step: step.label, startedAt, edges: [], quiet: null };
        results.push(row);
        let cursor = startedAt;
        for (const want of step.expect ?? []) {
          let hit = null;
          await waitUntil(async () => {
            hit = tape.events.find((e) => e.receivedAt >= cursor && e.to === want.to && (!want.from || e.from === want.from) && (!want.reason || e.reason === want.reason));
            return !!hit;
          }, (step.within ?? 60) * 1000, 250);
          const label = `${want.from ?? "*"}→${want.to}${want.reason ? ` (${want.reason})` : ""}`;
          row.edges.push({ want: label, afterSeconds: hit ? +((hit.receivedAt - startedAt) / 1000).toFixed(1) : null, got: hit ? `${hit.from}→${hit.to} (${hit.reason})` : null });
          console.log(`    ${step.label}: ${label} ${hit ? `after ${row.edges.at(-1).afterSeconds}s` : `NOT SEEN within ${step.within ?? 60}s`}`);
          if (!hit) return results;
          cursor = hit.receivedAt;
        }
        if (step.quiet) {
          await sleep(step.quiet * 1000);
          const noisy = tape.events.filter((e) => e.receivedAt >= startedAt);
          row.quiet = { seconds: step.quiet, transitions: noisy.map((e) => `${e.from}→${e.to} (${e.reason})`) };
          console.log(`    ${step.label}: ${noisy.length === 0 ? "quiet" : `NOT quiet: ${row.quiet.transitions.join(", ")}`} for ${step.quiet}s`);
        }
        if (step.hold) await sleep(step.hold * 1000);
      }
      return results;
    },
  }));

const HALF = PAYMENT_PORTS.slice(0, 3);
const killAndStart = (target, detail) => ({ victims: [target], detail, heal: () => exec("docker", ["start", target]) });

const FAULTS = {
  none: fault("closed", async () => ({ heal: async () => {} })),

  // Processes.
  "kill-daemon": fault("closed", async () => {
    const target = pick((await fleet()).filter((d) => d.running && d.floor !== 1));
    await exec("docker", ["kill", target.name]);
    return killAndStart(target.name, target.id);
  }),
  "kill-floor": fault("closed", async () => {
    const target = (await fleet()).find((d) => d.floor === 1);
    if (!target) throw new Error("no daemon holds the floor");
    await exec("docker", ["kill", target.name]);
    return killAndStart(target.name, target.id);
  }),
  "partition-daemon": fault("closed", async () => {
    const target = pick((await fleet()).filter((d) => d.running));
    const networks = [`${PROJECT}_default`, "devcontainer"];
    for (const n of networks) await exec("docker", ["network", "disconnect", n, target.name]);
    return {
      detail: target.id,
      heal: async () => {
        for (const n of networks) {
          await exec("docker", ["network", "connect", "--alias", "rmq-daemon", "--alias", target.id, n, target.name]).catch((e) => {
            if (!String(e.stderr).includes("already exists")) throw e;
          });
        }
      },
    };
  }),
  "kill-leader": fault("closed", async () => {
    const leader = await leaderContainer();
    await exec("docker", ["kill", leader]);
    return killAndStart(leader, leader);
  }),
  "kill-redis": fault("closed", async () => {
    await exec("docker", ["kill", REDIS[0]]);
    return killAndStart(REDIS[0]);
  }),
  "pause-envoy": fault("closed", async () => {
    const name = pick(ENVOYS);
    await exec("docker", ["pause", name]);
    return {
      detail: name,
      heal: () =>
        exec("docker", ["unpause", name]).catch((e) => {
          if (!String(e.stderr).includes("not paused")) throw e;
        }),
    };
  }),
  "kill-broker": fault("closed", async () => {
    await exec("docker", ["kill", BROKER[0]]);
    return {
      victims: BROKER,
      resetsBrokerStats: true,
      heal: async () => {
        await exec("docker", ["start", BROKER[0]]);
        await waitUntil(brokerHealthy, 120_000, 2000);
      },
    };
  }),

  // The network.
  "net-upstream-latency": fault("closed", async () => ({ detail: "+300ms ±50ms Envoy → payments", heal: await netem(ENVOYS, UPSTREAM, PAYMENT_PORTS, "delay 300ms 50ms") })),
  "net-upstream-timeout": fault("open", async () => ({ detail: "+3000ms Envoy → payments", heal: await netem(ENVOYS, UPSTREAM, PAYMENT_PORTS, "delay 3000ms") })),
  "net-upstream-loss": fault("observe", async () => ({ detail: "30% loss Envoy → payments", heal: await netem(ENVOYS, UPSTREAM, PAYMENT_PORTS, "loss 30%") })),
  "net-upstream-blackhole": fault("open", async () => ({ detail: "Envoy → payments dropped", heal: (await firewall(ENVOYS, UPSTREAM, PAYMENT_PORTS, "drop")).heal })),
  "net-upstream-reset": fault("open", async () => ({ detail: "Envoy → payments reset", heal: (await firewall(ENVOYS, UPSTREAM, PAYMENT_PORTS, "reset")).heal })),
  "net-upstream-flap": fault("flap", async () => {
    const fw = await firewall(ENVOYS, UPSTREAM, PAYMENT_PORTS, "drop");
    let blocked = true;
    return {
      // Outages long enough to trip the breaker (5s did not), healthy windows shorter than relapseHoldMs.
      detail: "Envoy → payments dropped 10s on / 6s off",
      heal: fw.heal,
      during: async (ms) => {
        const until = Date.now() + ms;
        while (Date.now() < until) {
          await sleep(blocked ? 10_000 : 6000);
          await (blocked ? fw.heal() : fw.apply());
          blocked = !blocked;
        }
      },
    };
  }),
  "net-egress-blackhole": fault("protect", async () => ({ detail: "daemons → Envoy :10000 dropped", heal: (await firewall(await daemonContainers(), ENVOYS, [10000], "drop")).heal })),
  "net-egress-latency": fault("protect", async () => ({ detail: "+2500ms daemons → Envoy", heal: await netem(await daemonContainers(), ENVOYS, [10000], "delay 2500ms") })),
  "net-telemetry-one": fault("closed", async () => ({ detail: "envoy-00 cannot push stats", heal: (await firewall([ENVOYS[0]], AGGREGATORS.map((a) => a.container), [9900], "drop")).heal })),
  "net-telemetry-all": fault("closed", async () => ({ detail: "no Envoy can push stats", heal: (await firewall(ENVOYS, AGGREGATORS.map((a) => a.container), [9900], "drop")).heal })),
  "net-telemetry-all+outage": fault("observe", async () => {
    const fw = await firewall(ENVOYS, AGGREGATORS.map((a) => a.container), [9900], "drop");
    await setUpstream(PAYMENT_PORTS, { rate: 1 });
    return { detail: "upstream down while no Envoy can report it", heal: async () => (await healUpstream(), await fw.heal()) };
  }),
  "net-control-partition+outage": withOutage("cut from the broker while the upstream fails", async () => {
    const leader = await leaderContainer();
    return { what: leader, heal: (await firewall([leader], BROKER, [5672], "drop")).heal };
  }),
  "net-lease-partition": fault("closed", async () => {
    const leader = await leaderContainer();
    return { detail: `${leader} cut from Redis`, heal: (await firewall([leader], REDIS, [6379], "drop")).heal };
  }),
  "net-lease-partition+outage": withOutage("cut from Redis while the upstream fails", async () => {
    const leader = await leaderContainer();
    return { what: leader, heal: (await firewall([leader], REDIS, [6379], "drop")).heal };
  }),
  "net-broker-latency": fault("closed", async () => ({ detail: "+200ms ±50ms daemons → broker", heal: await netem(await daemonContainers(), BROKER, [5672], "delay 200ms 50ms") })),

  // The flaky service, walked through every edge of the breaker's table.
  "flaky-degrade-recover": journey(
    [
      { label: "half the endpoints fail", set: [HALF, { rate: 1 }], expect: [{ from: "CLOSED", to: "DEGRADED" }], within: 45, hold: 10 },
      { label: "healed", set: "heal", expect: [{ from: "DEGRADED", to: "CLOSED", reason: "HEALTHY" }], within: 90 },
    ],
    { forbid: ["OPEN"] },
  ),
  "flaky-full-cycle": journey([
    {
      label: "every endpoint fails",
      set: [PAYMENT_PORTS, { rate: 1 }],
      expect: [
        { to: "OPEN" },
        { from: "OPEN", to: "HALF_OPEN", reason: "OPEN_TIMEOUT_ELAPSED" },
        { from: "HALF_OPEN", to: "OPEN", reason: "PROBE_FAILED" },
        { from: "OPEN", to: "HALF_OPEN", reason: "OPEN_TIMEOUT_ELAPSED" },
      ],
      within: 90,
    },
    { label: "healed", set: "heal", expect: [{ from: "HALF_OPEN", to: "CLOSED", reason: "PROBE_SUCCEEDED" }], within: 120 },
  ]),
  "flaky-degrade-escalate": journey([
    { label: "half fail", set: [HALF, { rate: 1 }], expect: [{ from: "CLOSED", to: "DEGRADED" }], within: 45, hold: 5 },
    { label: "all fail", set: [PAYMENT_PORTS, { rate: 1 }], expect: [{ from: "DEGRADED", to: "OPEN" }], within: 45 },
    { label: "half recover", set: [HALF, { rate: 1 }], expect: [{ from: "HALF_OPEN", to: "DEGRADED" }], within: 120, hold: 5 },
    { label: "healed", set: "heal", expect: [{ from: "DEGRADED", to: "CLOSED", reason: "HEALTHY" }], within: 90 },
  ]),
  "flaky-reset-storm": journey([
    { label: "every connection reset", set: [PAYMENT_PORTS, { rate: 1, mode: "reset" }], expect: [{ from: "CLOSED", to: "OPEN" }], within: 45 },
    { label: "healed", set: "heal", expect: [{ to: "CLOSED" }], within: 120 },
  ]),
  "flaky-hang": journey([
    { label: "every request hangs", set: [PAYMENT_PORTS, { rate: 1, mode: "hang" }], expect: [{ to: "OPEN" }], within: 60 },
    { label: "healed", set: "heal", expect: [{ to: "CLOSED" }], within: 120 },
  ]),
  "flaky-slow-ok": journey([{ label: "+700ms, inside every timeout", set: [PAYMENT_PORTS, { delayMs: 700 }], quiet: 30 }, { label: "healed", set: "heal" }]),
  "flaky-slow-healthcheck": journey([{ label: "+1500ms, inside the daemon's 2s budget", set: [PAYMENT_PORTS, { delayMs: 1500 }], quiet: 30 }, { label: "healed", set: "heal" }]),
  "flaky-slow-past-timeout": journey([
    { label: "+2500ms, past the daemon's 2s budget", set: [PAYMENT_PORTS, { delayMs: 2500 }], expect: [{ to: "OPEN" }], within: 60 },
    { label: "healed", set: "heal", expect: [{ to: "CLOSED" }], within: 120 },
  ]),
  "flaky-intermittent": journey([{ label: "30% of requests fail everywhere", set: [PAYMENT_PORTS, { rate: 0.3 }], hold: 40 }, { label: "healed", set: "heal" }]),
  "flaky-one-endpoint": journey(
    [
      { label: "one endpoint fails", set: [[8080], { rate: 1 }], expect: [{ from: "CLOSED", to: "DEGRADED" }], within: 45, hold: 10 },
      { label: "healed", set: "heal", expect: [{ from: "DEGRADED", to: "CLOSED" }], within: 90 },
    ],
    { forbid: ["OPEN"] },
  ),
  "flaky-overflow": journey([
    { label: "+900ms everywhere under a spike", set: [PAYMENT_PORTS, { delayMs: 900 }], expect: [{ to: "DEGRADED", reason: "THRESHOLD_OVERFLOW" }], within: 45 },
    { label: "healed", set: "heal" },
  ]),
};

// ---- one scenario ----------------------------------------------------------

/** The stack is whole again: one leader, circuit CLOSED everywhere, full fleet pulling, one floor. */
const whole = async () => {
  const [aggs, ds, work] = await Promise.all([aggregators(), fleet(), queue(WORK)]);
  const leaders = aggs.filter((a) => a.leader);
  const reasons = [];
  if (leaders.length !== 1) reasons.push(`${leaders.length} leaders`);
  if (leaders[0] && leaders[0].state !== "CLOSED") reasons.push(`circuit ${leaders[0].state}`);
  const up = ds.filter((d) => d.reachable);
  if (up.length !== FLEET_SIZE) reasons.push(`${up.length}/${FLEET_SIZE} daemons reachable`);
  if (up.some((d) => d.circuit !== 0)) reasons.push("a daemon's circuit is not CLOSED");
  // Who is pulling is the broker's to say: the work queue's consumer count.
  if ((work?.consumers ?? 0) !== FLEET_SIZE) reasons.push(`${work?.consumers ?? 0} consumers on the work queue`);
  const floors = up.reduce((n, d) => n + (d.floor ?? 0), 0);
  if (floors !== 1) reasons.push(`${floors} floors held`);
  return { ok: reasons.length === 0, reasons };
};

const purge = (name) =>
  fetch(`${RABBIT}/api/queues/%2F/${encodeURIComponent(name)}/contents`, { method: "DELETE", headers: { authorization: RABBIT_AUTH } }).catch(() => null);

/** Work drained, then dead letters given their chance to be redriven. */
const drainQueues = async () => {
  const drainedMs = await waitUntil(async () => {
    const q = await queue(WORK);
    return q && q.depth === 0 && q.unacked === 0;
  }, DRAIN_TIMEOUT * 1000, 2000);
  const deadEmptyMs = await waitUntil(async () => {
    const [w, d] = await Promise.all([queue(WORK), queue(DEAD)]);
    return w && d && d.depth === 0 && w.depth === 0 && w.unacked === 0;
  }, DEAD_LETTER_TIMEOUT * 1000, 2000);
  const [work, dead, parked] = await Promise.all([queue(WORK), queue(DEAD), queue(PARKED)]);
  return { drainedMs, deadEmptyMs, work, dead, parked: parked ?? { depth: 0, unacked: 0 } };
};

/** One row per second: the fleet's circuit, calls made that second, queue depths. */
const observe = () => {
  const seen = { maxDepth: 0, maxDeadDepth: 0, leaderlessSeconds: 0, splitBrainSeconds: 0, floorlessSeconds: 0, stalledSeconds: 0 };
  const timeline = [];
  let running = true;
  let prev = null;
  const loop = (async () => {
    while (running) {
      const t = Date.now();
      const [aggs, ds, q, dq] = await Promise.all([aggregators(), fleet(), queue(WORK), queue(DEAD)]).catch(() => [[], [], null, null]);
      const leaders = aggs.filter((a) => a.leader);
      if (leaders.length === 0) seen.leaderlessSeconds++;
      if (leaders.length > 1) seen.splitBrainSeconds++;
      const up = ds.filter((d) => d.reachable);
      if (ds.length && up.reduce((n, d) => n + (d.floor ?? 0), 0) === 0) seen.floorlessSeconds++;
      if (q) seen.maxDepth = Math.max(seen.maxDepth, q.depth);
      if (dq) seen.maxDeadDepth = Math.max(seen.maxDeadDepth, dq.depth);
      const ok = ds.reduce((n, d) => n + (d.ok ?? 0), 0);
      const calls = ds.reduce((n, d) => n + (d.ok ?? 0) + (d.failed ?? 0), 0);
      if (prev && ok === prev.ok && (q?.depth ?? 0) > 0) seen.stalledSeconds++;
      timeline.push({ at: t, fleet: up.map((d) => d.state), calls: prev && calls >= prev.calls ? calls - prev.calls : null });
      prev = { ok, calls };
      await sleep(Math.max(0, 1000 - (Date.now() - t)));
    }
  })();
  return async () => {
    running = false;
    await loop;
    return { seen, timeline };
  };
};

const counterDelta = (before, after, key) => {
  const b = new Map(before.map((d) => [d.id, d[key]]));
  let total = 0;
  for (const d of after) {
    if (d[key] === null) continue;
    const was = b.get(d.id);
    total += was === undefined || was === null || d[key] < was ? d[key] : d[key] - was;
  }
  return total;
};

/** How long the breaker had been in `event.from` when it left, from the event that put it there. */
const dwellBefore = (event) => {
  const prior = tape.events.find((e) => e.sequence === event.sequence - 1);
  return prior ? event.at - prior.at : null;
};

const runScenario = async (profileName, faultNames) => {
  const profile = PROFILES[profileName];
  const label = `${faultNames.length === 1 ? faultNames[0] : `monkey(${faultNames.join(",")})`} @ ${profileName}`;
  console.log(`\n== ${label} ==`);
  const result = { profile: profileName, faults: [], checks: [], startedAt: new Date().toISOString() };
  // Wall clock minus monotonic clock: grows only if the host slept, which freezes every container and makes every wall-clock timeout here meaningless.
  const clockSkew = () => Date.now() - performance.timeOrigin - performance.now();
  const skewAtStart = clockSkew();
  const check = (kind, ok, what, detail = "") => {
    result.checks.push({ kind, ok, what, detail });
    console.log(`  ${ok ? "PASS" : "FAIL"}  ${kind === "correctness" ? "[correct] " : ""}${what}${detail ? ` — ${detail}` : ""}`);
  };

  const inherited = await drainQueues();
  // A previous scenario's leftovers are that scenario's failure, already recorded; carried over they would fail this one too.
  for (const [name, q] of [[DEAD, inherited.dead], [PARKED, inherited.parked]]) {
    if (q.depth === 0) continue;
    await purge(name);
    result.purgedAtStart = { ...(result.purgedAtStart ?? {}), [name]: q.depth };
    console.log(`  purged ${q.depth} messages left on ${name} by an earlier scenario`);
  }
  const [aggsBefore, fleetBefore, restartsBefore] = await Promise.all([aggregators(), fleet(), restarts()]);
  const leaderBefore = aggsBefore.find((a) => a.leader);
  const scenarioStart = Date.now();
  const runId = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  result.runId = runId;
  const spikes = load(profile, runId);
  onCleanup(spikes.kill);
  const stopObserving = observe();
  await sleep(SETTLE * 1000);

  const victims = new Set();
  for (const name of faultNames) {
    await spikes.kick();
    const injectedAt = Date.now();
    const injected = await FAULTS[name]({ spikes }).catch((e) => ({ error: e.message }));
    if (injected.error) {
      check("harness", false, `inject ${name}`, injected.error);
      continue;
    }
    injected.victims.forEach((v) => victims.add(v));
    console.log(`  injected ${name}${injected.detail ? ` (${injected.detail})` : ""} under a ${profile.spike}/s spike`);
    const journeyResult = injected.during ? await injected.during(FAULT_SECONDS * 1000, { spikes }) : await sleep(FAULT_SECONDS * 1000);
    await injected.heal();
    cleanups.splice(cleanups.lastIndexOf(injected.heal), 1);
    const healedAt = Date.now();
    let lastReasons = [];
    const recoveredMs = await waitUntil(async () => {
      const w = await whole();
      lastReasons = w.reasons;
      return w.ok;
    }, RECOVERY_TIMEOUT * 1000);
    const entry = {
      fault: name,
      expect: injected.expect,
      detail: injected.detail ?? null,
      forbid: injected.forbid ?? [],
      injectedAt,
      healedAt,
      recoveredAt: recoveredMs === null ? null : healedAt + recoveredMs,
      recoveredSeconds: recoveredMs === null ? null : +(recoveredMs / 1000).toFixed(1),
      stuckOn: recoveredMs === null ? lastReasons : [],
      journey: Array.isArray(journeyResult) ? journeyResult : null,
    };
    result.faults.push(entry);
    check("recovery", recoveredMs !== null, `the stack recovered after ${name}`, recoveredMs === null ? `still: ${lastReasons.join("; ")}` : `${entry.recoveredSeconds}s after heal`);
    if (faultNames.length > 1) await sleep(pick([5, 10, 20]) * 1000);
  }

  await sleep(HOLD * 1000);
  await spikes.pause();
  const final = await spikes.finish();
  cleanups.splice(cleanups.indexOf(spikes.kill), 1);
  const after = await drainQueues();
  const { seen, timeline } = await stopObserving();
  const [aggsAfter, fleetAfter, restartsAfter] = await Promise.all([aggregators(), fleet(), restarts()]);
  const scenarioEnd = Date.now();
  result.traffic = { spikes: spikes.stats.spikes, publisherDisconnectedSeconds: spikes.stats.disconnectedSeconds };

  // ---- correctness: every confirmed message processed or dead-lettered, DLQ empty ----
  if (final.error) {
    check("correctness", false, "the publisher reported what it confirmed", final.error);
  } else {
    const auditRes = await json(`http://flaky-upstream:8080/__audit?run=${runId}`);
    const processed = bitsOf(auditRes?.bits);
    const confirmed = bitsOf(final.bits);
    const [inDead, inWork, inParked] = await Promise.all([
      keysIn(DEAD, runId, after.dead.depth),
      keysIn(WORK, runId, after.work.depth),
      keysIn(PARKED, runId, after.parked.depth),
    ]);
    let lost = 0;
    let processedUnconfirmed = 0;
    const lostSample = [];
    for (let i = 0; i < final.sent; i++) {
      const c = hasBit(confirmed, i);
      const p = hasBit(processed, i);
      if (c && !p && !inDead.has(i) && !inWork.has(i) && !inParked.has(i)) {
        lost++;
        if (lostSample.length < 10) lostSample.push(`${runId}:${i}`);
      }
      if (p && !c) processedUnconfirmed++;
    }
    await fetch(`http://flaky-upstream:8080/__audit?run=${runId}`, { method: "DELETE" }).catch(() => {});
    result.messages = {
      sent: final.sent,
      confirmed: final.confirmed,
      unconfirmed: final.sent - final.confirmed,
      processed: auditRes?.processed ?? 0,
      duplicateCalls: auditRes?.duplicates ?? 0,
      processedUnconfirmed,
      lost,
      lostSample,
      deadLetterDepthAtEnd: after.dead.depth,
      parkedFromThisRun: inParked.size,
      maxDeadLetterDepth: seen.maxDeadDepth,
      workDepthAtEnd: after.work.depth,
      drainedSeconds: after.drainedMs === null ? null : +(after.drainedMs / 1000).toFixed(1),
    };
    check(
      "correctness",
      lost === 0 && auditRes !== null,
      "no message lost",
      `confirmed ${final.confirmed} of ${final.sent}, processed ${auditRes?.processed ?? "?"} (+${auditRes?.duplicates ?? "?"} duplicate calls), dead-lettered ${inDead.size}, lost ${lost}${lostSample.length ? ` e.g. ${lostSample.slice(0, 3).join(", ")}` : ""}`,
    );
  }
  check("correctness", after.dead.depth === 0, "dead-letter queue empty at the end", `${after.dead.depth} left (peak ${seen.maxDeadDepth})`);
  // Nothing injected here is poison, so anything parked is healthy work taken out of circulation.
  check("correctness", after.parked.depth === 0, "nothing parked as poison", `${after.parked.depth} parked`);
  check("correctness", after.drainedMs !== null, "the work queue drained once arrivals stopped", after.drainedMs === null ? `depth ${after.work.depth}, unacked ${after.work.unacked}` : `${(after.drainedMs / 1000).toFixed(1)}s`);

  // ---- the breaker ------------------------------------------------------------
  const events = tape.between(scenarioStart, scenarioEnd);
  result.transitions = events.map((e) => ({ sequence: e.sequence, edge: `${e.from}→${e.to}`, reason: e.reason, at: new Date(e.at).toISOString(), dwellMs: dwellBefore(e) }));
  const illegal = events.filter((e) => !LEGAL[`${e.from}→${e.to}`]?.includes(e.reason));
  check("breaker", illegal.length === 0, "every transition is an edge of the reducer's table", illegal.map((e) => `${e.from}→${e.to} (${e.reason})`).join(", "));
  const broken = events.filter((e, i) => i > 0 && (e.sequence !== events[i - 1].sequence + 1 || e.from !== events[i - 1].to));
  check("breaker", broken.length === 0, "the transitions form one unbroken chain", broken.map((e) => `seq ${e.sequence} from ${e.from}`).join(", "));
  const early = events.filter((e) => {
    const dwell = dwellBefore(e);
    if (dwell === null) return false;
    if (e.from === "OPEN") return dwell < TIMERS.openMs - TIMERS.tickSlackMs;
    if (e.from === "CLOSED" || e.from === "DEGRADED") return dwell < TIMERS.minStateMs - TIMERS.tickSlackMs;
    return false;
  });
  check("breaker", early.length === 0, "no state was left before its timer allowed", early.map((e) => `${e.from}→${e.to} after ${dwellBefore(e)}ms`).join(", "));
  // After a failed probe the next OPEN waits twice as long, up to the cap.
  const backoffs = events.filter((e) => e.from === "OPEN" && e.to === "HALF_OPEN").map((e) => ({ dwell: dwellBefore(e), enteredBy: tape.events.find((x) => x.sequence === e.sequence - 1) }));
  const shortBackoff = backoffs.filter((b, i) => i > 0 && b.enteredBy?.reason === "PROBE_FAILED" && b.dwell !== null && backoffs[i - 1].dwell !== null && b.dwell < Math.min(backoffs[i - 1].dwell * 2, TIMERS.maxOpenMs) - 2 * TIMERS.tickSlackMs);
  check("breaker", shortBackoff.length === 0, "a failed probe doubled the next OPEN", backoffs.map((b) => `${b.dwell}ms`).join(" → "));
  // Every settled state that held for at least 8s reached the whole fleet within 5s.
  const unheard = events
    .filter((e, i) => e.to !== "HALF_OPEN" && (events[i + 1]?.receivedAt ?? scenarioEnd) - e.receivedAt >= 8000)
    .filter((e) => !timeline.some((r) => r.at >= e.receivedAt && r.at <= e.receivedAt + 5000 && r.fleet.length > 0 && r.fleet.every((s) => s === e.to)));
  check("breaker", unheard.length === 0, "every daemon followed each settled state within 5s", unheard.map((e) => `${e.to} at seq ${e.sequence}`).join(", "));
  // While OPEN, once in-flight calls have had their 2s timeout, nothing should be called.
  const openWindows = events.filter((e) => e.to === "OPEN").map((e) => [e.receivedAt + 3000, events.find((x) => x.sequence === e.sequence + 1)?.receivedAt ?? scenarioEnd]);
  const openRows = timeline.filter((r) => r.calls !== null && openWindows.some(([a, b]) => r.at >= a && r.at <= b));
  if (openRows.length >= 3) {
    const rate = openRows.reduce((n, r) => n + r.calls, 0) / openRows.length;
    check("breaker", rate <= 5, "the fleet stopped calling the third party while OPEN", `${rate.toFixed(1)} calls/s over ${openRows.length}s`);
  }

  for (const f of result.faults) {
    const window = events.filter((e) => e.receivedAt >= f.injectedAt && e.receivedAt <= (f.recoveredAt ?? f.healedAt));
    const path = window.map((e) => `${e.from}→${e.to}`).join(", ") || "no transition";
    const tag = `[${f.fault}]`;
    const reached = (to) => window.some((e) => e.to === to);
    if (f.expect === "closed") check("breaker", window.length === 0, `${tag} no trip on a fault the third party never saw`, path);
    if (f.expect === "open") check("breaker", reached("OPEN"), `${tag} the breaker opened`, path);
    if (f.expect === "fleet-open") {
      const heard = timeline.some((r) => r.at >= f.injectedAt && r.at <= f.healedAt && r.fleet.length > 0 && r.fleet.every((s) => s === "OPEN" || s === "HALF_OPEN"));
      check("breaker", heard, `${tag} the fleet heard OPEN through the partition`, path);
    }
    if (f.expect === "flap") {
      // Without this a flap the breaker never felt passes the next check vacuously.
      check("breaker", window.some((e) => e.from === "CLOSED"), `${tag} the flap tripped the breaker`, path);
      // The first incident may close once; a relapse must then hold until the link is stable.
      const recloses = window.filter((e) => e.to === "CLOSED" && e.receivedAt < f.healedAt).length;
      check("breaker", recloses <= 1, `${tag} closed at most once while the link was still flapping`, path);
    }
    for (const step of f.journey ?? []) {
      for (const edge of step.edges) check("breaker", edge.got !== null, `${tag} ${step.step}: ${edge.want}`, edge.got ? `${edge.got} after ${edge.afterSeconds}s` : "not seen");
      if (step.quiet) check("breaker", step.quiet.transitions.length === 0, `${tag} ${step.step}: no transition for ${step.quiet.seconds}s`, step.quiet.transitions.join(", "));
    }
    for (const state of f.forbid) check("breaker", !reached(state), `${tag} never ${state}`, path);
  }

  // ---- coordination -------------------------------------------------------------
  check("coordination", seen.splitBrainSeconds === 0, "never two leaders at once", `${seen.splitBrainSeconds}s`);
  const survivor = aggsAfter.find((a) => a.leader);
  const sameAgg = survivor && aggsBefore.find((a) => a.container === survivor.container);
  check("coordination", !!survivor && !!leaderBefore && survivor.sequence >= leaderBefore.sequence, "the published sequence never went backwards", `${leaderBefore?.sequence} -> ${survivor?.sequence}`);
  if (sameAgg && survivor.received >= sameAgg.received) {
    check("coordination", survivor.subscriberDuplicates === sameAgg.subscriberDuplicates, "no control event delivered twice", `${sameAgg.subscriberDuplicates} -> ${survivor.subscriberDuplicates}`);
  }
  const gaps = counterDelta(fleetBefore, fleetAfter, "gaps");
  const dups = counterDelta(fleetBefore, fleetAfter, "duplicates");
  check("coordination", gaps === 0 && dups === 0, "no daemon saw a gap or duplicate on circuit.control", `${gaps} gaps, ${dups} duplicates`);
  const ending = await whole();
  check("recovery", ending.ok, "the stack ended whole", ending.reasons.join("; "));

  const collateral = [];
  for (const [name, now] of restartsAfter) {
    const was = restartsBefore.get(name);
    if (now.oom) collateral.push(`${name} OOM-killed`);
    if (!was || victims.has(name)) continue;
    if (now.restarts !== was.restarts || now.startedAt !== was.startedAt) collateral.push(`${name} restarted`);
  }
  check("recovery", collateral.length === 0, "nothing crashed that was not a victim", collateral.join("; "));

  result.observed = seen;
  result.endedAt = new Date().toISOString();
  result.suspendedSeconds = +((clockSkew() - skewAtStart) / 1000).toFixed(1);
  result.correct = result.checks.filter((c) => c.kind === "correctness").every((c) => c.ok);
  result.passed = result.checks.every((c) => c.ok);
  console.log(`  transitions: ${result.transitions.map((t) => t.edge).join(", ") || "none"} · spikes ${spikes.stats.spikes} · max depth ${seen.maxDepth} · leaderless ${seen.leaderlessSeconds}s · stalled ${seen.stalledSeconds}s`);
  return result;
};

// ---- run -------------------------------------------------------------------

if (flag("list", false) === true) {
  console.log(`profiles: ${Object.entries(PROFILES).map(([k, v]) => `${k} (${v.rate}/s, spikes to ${v.spike}/s)`).join(", ")}`);
  for (const name of Object.keys(FAULTS)) console.log(`  ${name}`);
  process.exit(0);
}

const PROFILE_LIST = flag("profiles", Object.keys(PROFILES).join(",")).split(",");
const FAULT_LIST = flag("faults", Object.keys(FAULTS).join(",")).split(",");
for (const p of PROFILE_LIST) if (!PROFILES[p]) throw new Error(`unknown profile ${p}`);
for (const f of FAULT_LIST) if (!FAULTS[f]) throw new Error(`unknown fault ${f}`);

const record = { scenario: "chaos-load", startedAt: new Date().toISOString(), settle: SETTLE, faultSeconds: FAULT_SECONDS, spikeOn: SPIKE_ON, spikeOff: SPIKE_OFF, runs: [] };
const save = () => {
  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, `${JSON.stringify(record, null, 2)}\n`);
};

const shutdown = async (code) => {
  await runCleanups();
  producerOn(null);
  tape.stop();
  process.exit(code);
};
process.on("SIGINT", () => shutdown(130));
process.on("SIGTERM", () => shutdown(143));

await healUpstream();
// The harness is the only publisher while it runs, so it knows exactly what was confirmed.
producerOff();
const start = await whole();
if (!start.ok) {
  console.error(`the stack is not whole before starting: ${start.reasons.join("; ")}`);
  await shutdown(2);
}
const deadAtStart = await queue(DEAD);
if (deadAtStart.depth > 0) {
  if (!PURGE) {
    console.error(`${DEAD} holds ${deadAtStart.depth} messages before the run; every scenario would fail on them. Pass --purge-dead-letters to clear them first.`);
    await shutdown(2);
  }
  await purge(DEAD);
  record.purgedDeadLetters = deadAtStart.depth;
  console.log(`purged ${deadAtStart.depth} pre-existing dead letters`);
}

try {
  for (const profile of PROFILE_LIST) {
    if (MONKEY > 0) {
      const count = Math.max(1, Math.round(MONKEY / (FAULT_SECONDS + 40)));
      const pool = FAULT_LIST.filter((f) => f !== "none" && !f.startsWith("flaky-"));
      record.runs.push(await runScenario(profile, Array.from({ length: count }, () => pick(pool))));
      save();
      continue;
    }
    for (const name of FAULT_LIST) {
      let run = await runScenario(profile, [name]);
      if (run.suspendedSeconds > SUSPEND_TOLERANCE) {
        console.log(`  the host was suspended for ${run.suspendedSeconds}s during this scenario; its results are void — repeating it`);
        record.voided = [...(record.voided ?? []), run];
        run = await runScenario(profile, [name]);
      }
      record.runs.push(run);
      save();
    }
  }
} finally {
  await runCleanups();
  producerOn(null);
  tape.stop();
  record.endedAt = new Date().toISOString();
  const edges = new Set(record.runs.flatMap((r) => r.transitions ?? []).map((t) => t.edge));
  record.edgeCoverage = Object.fromEntries(Object.keys(LEGAL).map((edge) => [edge, edges.has(edge)]));
  save();
}

console.log("\n== summary ==");
for (const r of record.runs) {
  const failed = r.checks.filter((c) => !c.ok);
  const rec = r.faults.map((f) => (f.recoveredSeconds === null ? "∞" : `${f.recoveredSeconds}s`)).join(",");
  console.log(
    `${r.faults.map((f) => f.fault).join("+").padEnd(28).slice(0, 60)} ${r.profile.padEnd(13)} ${r.correct ? "CORRECT  " : "INCORRECT"} ${r.passed ? "PASS" : "FAIL"}  ` +
      `${r.suspendedSeconds > SUSPEND_TOLERANCE ? `SUSPENDED ${r.suspendedSeconds}s ` : ""}recovery ${rec.padEnd(7)} lost ${r.messages?.lost ?? "n/a"} dup ${r.messages?.duplicateCalls ?? "n/a"} dlq ${r.messages?.deadLetterDepthAtEnd ?? "n/a"}` +
      (failed.length ? `  ← ${failed.map((c) => c.what).join("; ")}` : ""),
  );
}
console.log(`edges covered: ${Object.entries(record.edgeCoverage).map(([e, ok]) => `${ok ? "✓" : "✗"} ${e}`).join("  ")}`);
console.log(`record: ${OUT}`);
process.exit(record.runs.every((r) => r.correct) ? 0 : 1);
