/**
 * What the chaos drivers share: reading the broker, Prometheus, the fake third party and the replicas' own logs,
 * and the forked publisher. `chaos-breaker.mjs` drives one dependency's breaker; `chaos-app.mjs` drives the
 * consumer application with both of its consumers and dependencies.
 */
import { execFile, fork } from "node:child_process";
import { createRequire } from "node:module";
import { promisify } from "node:util";
import { levelName } from "../packages/rmq/src/DelayedDelivery.ts";

export const exec = promisify(execFile);
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const args = process.argv.slice(2);
export const flag = (name, fallback) => {
  const hit = args.find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  return hit ? (hit.includes("=") ? hit.slice(name.length + 3) : true) : fallback;
};

export const BROKER = process.env.BROKER ?? "amqp://guest:guest@rabbitmq:5672";
export const FLAKY = process.env.FLAKY_UPSTREAM ?? "http://flaky-upstream:8080";
export const PROMETHEUS = process.env.PROMETHEUS ?? "http://prometheus:9090";
export const PROJECT = process.env.COMPOSE_PROJECT_NAME ?? "workspace";
export const MANAGEMENT = process.env.RABBITMQ_MANAGEMENT ?? "http://guest:guest@rabbitmq:15672";
/** The application's name, which prefixes its dependencies' queues (`<app>.<dependency>.probe-permit`). */
export const APP = process.env.APP_NAME ?? "consumer";

export const amqp = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("amqplib");

// ---- the broker --------------------------------------------------------------

/** One passive-declare per read, on a channel of its own: a failed one closes the channel it arrives on. */
export const queueInfo = async (name) => {
  const conn = await amqp.connect(BROKER);
  try {
    const ch = await conn.createChannel();
    ch.on("error", () => {});
    const q = await ch.checkQueue(name);
    return { ready: q.messageCount, consumers: q.consumerCount };
  } finally {
    await conn.close().catch(() => {});
  }
};
export const tryQueueInfo = (name) => queueInfo(name).catch(() => undefined);

/** Tokens waiting in the delay chain, summed over every level. */
export const tokensInChain = async () => {
  const conn = await amqp.connect(BROKER).catch(() => undefined);
  if (!conn) return undefined;
  try {
    let total = 0;
    for (let n = 0; n < 17; n++) {
      const ch = await conn.createChannel();
      ch.on("error", () => {});
      total += (await ch.checkQueue(levelName(n)).catch(() => ({ messageCount: 0 }))).messageCount;
    }
    return total;
  } finally {
    await conn.close().catch(() => {});
  }
};

const management = async (path) => {
  const url = new URL(`${MANAGEMENT}/api/queues/%2F/${path}`);
  const auth = `Basic ${Buffer.from(`${url.username}:${url.password}`).toString("base64")}`;
  url.username = url.password = "";
  return fetch(url, { headers: { authorization: auth } }).then((r) => r.json());
};

export const permitQueueFor = (dependency) => `${APP}.${dependency}.probe-permit`;

/** Ready plus held: `x-max-length` counts only ready, so a duplicate hides behind a held token. */
export const permitTokens = async (queue) => {
  const q = await management(queue);
  return { ready: q.messages_ready, held: q.messages_unacknowledged };
};

/** The container RabbitMQ has made the single active consumer of a redrive trigger. */
export const activeRedriver = async (triggerQueue) => {
  const q = await management(triggerQueue);
  const ip = q.consumer_details?.find((c) => c.active)?.channel_details?.peer_host;
  const containers = await consumerContainers();
  const ips = await Promise.all(
    containers.map((c) =>
      exec("docker", ["inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", c]).then((r) =>
        r.stdout.trim().split(" "),
      ),
    ),
  );
  return containers.find((_, i) => ips[i].includes(ip));
};

// ---- Prometheus and the third party -----------------------------------------------

/**
 * One gauge reading per replica for `dependency`'s breaker, from Prometheus (scraped every 2s, so a little
 * behind). Without a dependency, every breaker gauge — for a design that labels none.
 */
export const breakerStates = async (dependency) => {
  const selector = dependency === undefined ? "" : `{dependency="${dependency}"}`;
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(`egress_consumer_breaker_state${selector}`)}`, {
    signal: AbortSignal.timeout(2000),
  }).catch(() => undefined);
  const body = res?.ok ? await res.json() : undefined;
  return body?.status === "success" ? body.data.result.map((r) => Number(r.value[1])) : undefined;
};

export const promSum = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).catch(() => undefined);
  const body = res?.ok ? await res.json() : undefined;
  return Number(body?.data?.result?.[0]?.value?.[1] ?? 0);
};

export const setFailure = (body) =>
  fetch(`${FLAKY}/__fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  }).catch(() => {});
export const audit = (run) => fetch(`${FLAKY}/__audit?run=${encodeURIComponent(run)}`).then((r) => r.json());

export const consumerContainers = async () =>
  (await exec("docker", ["ps", "-a", "--filter", `name=${PROJECT}-rmq-consumer`, "--format", "{{.Names}}"])).stdout
    .split("\n")
    .filter(Boolean)
    .sort();

export const isSet = (bitmap, n) => (bitmap[n >> 3] & (1 << (n & 7))) !== 0;
export const decodeBits = (b64) => Buffer.from(b64 ?? "", "base64");

// ---- the breakers, read back from the replicas' own logs ---------------------

/**
 * `consumer: breaker <dependency> <phase>`, or `consumer: breaker <phase>` from a design with one unnamed breaker
 * (article 3's cockatiel logs "opened").
 */
const LINE =
  /^\[(\d\d:\d\d:\d\d\.\d+)\].*consumer: (up —|breaker (?:([a-z0-9-]+) (?=closed|open|half-open))?(closed|open for (\d+)s \(attempt (\d+)\)|opened|open|half-open))/;
/** Cockatiel logs no half-open, and logs "opened" again when a probe fails, so its only rule is open ↔ closed. */
export const LEGAL = {
  held: { closed: ["open"], open: ["half-open"], "half-open": ["closed", "open"] },
  cockatiel: { closed: ["open"], open: ["open", "closed"] },
};

/** Which design the running fleet is: only the held breaker names a wake queue in its startup line. */
export const detectDesign = async () => {
  const [first] = await consumerContainers();
  const { stdout, stderr } = await exec("docker", ["logs", first]);
  return (stdout + stderr).includes("wake=") ? "held" : "cockatiel";
};

/** The phase sequence one replica logged for `dependency` since `since`, restarting at each process start. */
export const transitionsOf = async (container, since, { dependency, design = "held" } = {}) => {
  const { stdout, stderr } = await exec("docker", ["logs", "--since", since, container], { maxBuffer: 1 << 26 });
  const holds = [];
  const violations = [];
  let previous;
  let transitions = 0;
  let openings = 0;
  (stdout + stderr)
    .split("\n")
    .map((l) => l.match(LINE))
    .filter(Boolean)
    .forEach((m) => {
      const [, , head, named, phase, seconds, attempt] = m;
      if (head === "up —") return void (previous = undefined);
      if (dependency !== undefined && named !== undefined && named !== dependency) return;
      const name = phase.startsWith("open") ? "open" : phase;
      // "open" is logged twice per opening: the phase, then the hold it chose. Count the phase only.
      if (name === "open" && seconds !== undefined) return void holds.push({ seconds: Number(seconds), attempt: Number(attempt) });
      transitions++;
      if (name === "open") openings++;
      if (previous !== undefined && !LEGAL[design][previous].includes(name)) violations.push(`${previous} → ${name}`);
      previous = name;
    });
  return { container, dependency, transitions, openings, holds, violations };
};

/** The container whose most recent phase for `dependency` is `phase`, if any — the one a scenario may kill. */
export const containerIn = async (phase, dependency) => {
  const PHASE = /breaker (?:([a-z0-9-]+) )?(closed|opened|open|half-open)$/;
  for (const c of await consumerContainers()) {
    const { stdout, stderr } = await exec("docker", ["logs", "--tail", "60", c]).catch(() => ({ stdout: "", stderr: "" }));
    const phases = (stdout + stderr)
      .split("\n")
      .flatMap((l) => {
        const m = l.match(PHASE);
        return m && (dependency === undefined || m[1] === undefined || m[1] === dependency) ? [m[2]] : [];
      })
      .map((p) => (p === "opened" ? "open" : p));
    if (phases.at(-1) === phase) return c;
  }
  return undefined;
};

// ---- load and time ------------------------------------------------------------

/** A forked `chaos-publisher.mjs` onto `<api>.work`, at a rate changed over IPC; `stop` returns its confirmed bitmap. */
export const startPublisher = (run, api, rate, format = "json") => {
  const child = fork(new URL("./chaos-publisher.mjs", import.meta.url).pathname, [], {
    env: { ...process.env, RUN_ID: run, QUEUE: `${api}.work`, API_ID: api, AMQP_URL: BROKER, FORMAT: format },
  });
  const setRate = (r) => child.send({ type: "rate", rate: r });
  const stop = () =>
    new Promise((resolve) => {
      child.on("message", (m) => m.type === "final" && resolve(m));
      child.send({ type: "stop" });
    });
  setRate(rate);
  return { rate: setRate, stop };
};

export const waitFor = async (done, timeoutS) => {
  const end = Date.now() + timeoutS * 1000;
  while (Date.now() < end) {
    if (await done().catch(() => false)) return true;
    await sleep(1000);
  }
  return false;
};

/** Grows only while the host is suspended: wall clock minus the monotonic clock. */
export const skewMs = () => Date.now() - performance.timeOrigin - performance.now();
