/**
 * What the console's live stream costs, and what each way of shrinking it
 * would actually save — measured against a running aggregator rather than
 * estimated from one frame. The numbers in
 * docs/decisions/015-the-console-at-a-thousand-apis.md come from this.
 *
 *   node packages/aggregator/src/main.ts \
 *     --source=sim --apis=1000 --replicas=10 --port=8098 --no-webhook \
 *     --coordination=redis://redis:6379 &
 *   node scripts/measure-console-stream.mjs http://127.0.0.1:8098 --pid=<aggregator pid>
 *
 * Three parts:
 *
 * 1. **Bytes.** Holds a real `/api/stream` connection through three phases —
 *    quiet, 5% of APIs failing, 25% — records every frame, and replays the
 *    recording through each candidate encoding. Every candidate is computed
 *    from the same recorded bytes, so the comparison is between strategies
 *    rather than between runs.
 * 2. **The control loop.** Opens 0 to 100 concurrent streams and reads the
 *    tick rate from /metrics while they drain, because the stream is served by
 *    the same process that runs the breaker. With `--pid`, also that process's
 *    CPU and RSS.
 * 3. **Compressor memory**, per connection, since a compressed SSE response
 *    holds one compressor per browser for as long as the browser is open.
 */

import zlib from "node:zlib";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

const args = process.argv.slice(2);
const BASE = args.find((a) => !a.startsWith("--")) ?? "http://127.0.0.1:8098";
const PID = args.find((a) => a.startsWith("--pid="))?.slice(6) ?? null;
const SECONDS = Number(args.find((a) => a.startsWith("--seconds="))?.slice(10) ?? 20);
/** `--parts=2` to re-run only the control-loop measurement — the one a change
 *  to how frames are served has to move. */
const PARTS = new Set((args.find((a) => a.startsWith("--parts="))?.slice(8) ?? "1,2,3").split(","));
const PAGE = 50;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const bytes = (s) => Buffer.byteLength(s);
const fmt = (n) => (n >= 1e6 ? `${(n / 1e6).toFixed(2)} MB/s` : n >= 1e3 ? `${(n / 1e3).toFixed(1)} KB/s` : `${n.toFixed(0)} B/s`);

// ---------------------------------------------------------------------------
// The synthetic fleet flatters every compressor, so it is made less synthetic.
// ---------------------------------------------------------------------------

/**
 * `--apis=1000` produces `synthetic-000` to `synthetic-999`, every one with six
 * endpoints, the same timestamps and sequence 0. A single frame of that
 * gzips 82:1; the same frame with ids, endpoint counts, timestamps and
 * sequences varied the way a real fleet varies them gzips 19:1. Quoting the
 * first would overstate what compression buys by four times.
 *
 * So each API gets a fixed identity derived from its synthetic id — a name, an
 * endpoint count, clock offsets — applied identically to every frame. That
 * adds the entropy a real fleet has without inventing change: a field that did
 * not move between two frames still does not, and one that did still does, so
 * every delta below measures the churn the fleet actually produced.
 */
const WORDS = ["payments", "shipping", "tax", "fraud", "ledger", "identity", "search", "pricing", "catalog", "notify", "loyalty", "quotes"];
const REGIONS = ["eu-west", "us-east", "ap-south"];
const identity = new Map();
const identityOf = (syntheticId) => {
  const known = identity.get(syntheticId);
  if (known) return known;
  const h = createHash("sha256").update(syntheticId).digest();
  const made = {
    apiId: `${WORDS[h[0] % WORDS.length]}-${h.subarray(1, 4).toString("hex")}.${REGIONS[h[4] % REGIONS.length]}`,
    extraEndpoints: h[5] % 18,
    clock: h.readUInt32BE(6) % 86_400_000,
    sequence: h.readUInt16BE(10) % 5000,
  };
  identity.set(syntheticId, made);
  return made;
};
const realistic = (frame) => ({
  ...frame,
  specs: frame.specs.map((s) => ({ ...s, apiId: identityOf(s.apiId).apiId })),
  apis: frame.apis.map((a) => {
    const id = identityOf(a.apiId);
    return {
      ...a,
      apiId: id.apiId,
      sequence: a.sequence + id.sequence,
      totalEndpoints: a.totalEndpoints + id.extraEndpoints,
      healthyEndpoints: a.healthyEndpoints + id.extraEndpoints,
      observedSince: a.observedSince - id.clock,
      changedAt: a.changedAt - id.clock,
      replicas: a.replicas.map((r) => ({ ...r, total: r.total + id.extraEndpoints, healthy: r.healthy + id.extraEndpoints })),
    };
  }),
});

// ---------------------------------------------------------------------------
// Part 1: bytes.
// ---------------------------------------------------------------------------

/** SSE frames off one connection. Enough for this server's own framing, which
 *  is all this has to read; subscriber.ts uses the real decoder. */
const recordStream = async (seconds) => {
  const controller = new AbortController();
  const res = await fetch(`${BASE}/api/stream`, { signal: controller.signal });
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const frames = [];
  let buffer = "";
  const until = Date.now() + seconds * 1000;
  try {
    while (Date.now() < until) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end;
      while ((end = buffer.indexOf("\n\n")) >= 0) {
        const raw = buffer.slice(0, end + 2);
        buffer = buffer.slice(end + 2);
        const event = /^event: ?(.*)$/m.exec(raw)?.[1] ?? "message";
        const data = raw.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.replace(/^data: ?/, "")).join("\n");
        frames.push({ event, data, at: Date.now() });
      }
    }
  } finally {
    controller.abort();
  }
  return frames;
};

/**
 * One compressor per connection, flushed after every frame — which is how a
 * compressed SSE response has to behave, since a browser must receive each
 * event when it is sent rather than when a buffer fills.
 *
 * CPU is read when the stream ends rather than after the writes return: zlib
 * compresses on the libuv threadpool, so the writes come back long before the
 * work is done, and timing them measures nothing.
 */
const compressed = (make, flushKind, frames) =>
  new Promise((resolve) => {
    const z = make();
    let out = 0;
    z.on("data", (chunk) => (out += chunk.length));
    const t0 = process.cpuUsage();
    for (const f of frames) {
      z.write(f);
      z.flush(flushKind);
    }
    z.end(() => {
      const cpu = process.cpuUsage(t0);
      resolve({ out, cpuMs: (cpu.user + cpu.system) / 1000 });
    });
  });

const gzip = (frames) => compressed(() => zlib.createGzip({ level: 6 }), zlib.constants.Z_SYNC_FLUSH, frames);

/** Window 2^22 = 4 MiB, larger than a whole frame at a thousand APIs — the
 *  point of measuring it: a window that reaches the previous frame encodes an
 *  unchanged API as a back-reference to itself. Part 3 is what that costs. */
const BROTLI_WIDE = {
  params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 5, [zlib.constants.BROTLI_PARAM_LGWIN]: 22 },
};
const brotli = (frames) =>
  compressed(() => zlib.createBrotliCompress(BROTLI_WIDE), zlib.constants.BROTLI_OPERATION_FLUSH, frames);

const sse = (name, value) => `event: ${name}\ndata: ${JSON.stringify(value)}\n\n`;
const RANK = { OPEN: 0, HALF_OPEN: 1, DEGRADED: 2, CLOSED: 3 };

/**
 * The candidate encodings of the console's state channel, each as the list of
 * SSE frames it would put on the wire for the recorded run. The event tape is
 * accounted separately: it is the same in every scheme, and folding it in
 * would make "counts only" look like it costs something.
 */
const strategies = (states) => {
  const out = { full: [], apiDelta: [], fieldDelta: [], attention: [], attentionDelta: [] };
  let prior = new Map();
  let priorAttention = new Map();
  let priorCounts = "";
  const churn = { changed: 0, replicasOnly: 0 };

  states.forEach((frame, i) => {
    const { apis, specs, ...rest } = frame;
    const current = new Map(apis.map((a) => [a.apiId, a]));
    const text = new Map(apis.map((a) => [a.apiId, JSON.stringify(a)]));
    const changed = i === 0 ? apis : apis.filter((a) => prior.get(a.apiId)?.text !== text.get(a.apiId));
    if (i > 0) {
      churn.changed += changed.length;
      churn.replicasOnly += changed.filter((a) => {
        const was = prior.get(a.apiId).api;
        return JSON.stringify({ ...was, replicas: 0, votes: 0 }) === JSON.stringify({ ...a, replicas: 0, votes: 0 });
      }).length;
    }

    out.full.push(sse("state", frame));

    // Every API whose object changed, whole. Nothing at all on a tick where
    // nothing moved — a keep-alive comment every 15s is the server's job then.
    out.apiDelta.push(i === 0 ? sse("snapshot", frame) : changed.length ? sse("patch", { ...rest, apis: changed }) : "");

    // Only the fields that changed, per API.
    out.fieldDelta.push(
      i === 0
        ? sse("snapshot", frame)
        : changed.length
          ? sse("patch", {
              ...rest,
              apis: changed.map((a) => {
                const was = prior.get(a.apiId).api;
                return Object.fromEntries(
                  Object.entries(a).filter(([k, v]) => k === "apiId" || JSON.stringify(v) !== JSON.stringify(was[k])),
                );
              }),
            })
          : "",
    );

    // What a console at a thousand APIs can put on a screen: a count by state,
    // and the APIs that need attention — everything not CLOSED, worst first,
    // capped. Healthy APIs are a number, not a card each.
    const counts = apis.reduce((m, a) => ((m[a.state] = (m[a.state] ?? 0) + 1), m), {});
    const needing = apis
      .filter((a) => a.state !== "CLOSED")
      .sort((a, b) => RANK[a.state] - RANK[b.state] || b.changedAt - a.changedAt)
      .slice(0, PAGE);
    out.attention.push(sse("state", { ...rest, counts, attention: needing }));

    // The same view, sending only what entered, changed or left it.
    const nowAttention = new Map(needing.map((a) => [a.apiId, text.get(a.apiId)]));
    const upserts = needing.filter((a) => priorAttention.get(a.apiId) !== text.get(a.apiId));
    const removed = [...priorAttention.keys()].filter((id) => !nowAttention.has(id));
    const countsMoved = JSON.stringify(counts) !== priorCounts;
    out.attentionDelta.push(
      i === 0
        ? sse("snapshot", { ...rest, counts, attention: needing })
        : upserts.length || removed.length || countsMoved
          ? sse("patch", { counts, upserts, removed })
          : "",
    );

    prior = new Map(apis.map((a) => [a.apiId, { api: a, text: text.get(a.apiId) }]));
    priorAttention = nowAttention;
    priorCounts = JSON.stringify(counts);
  });
  return { out, churn };
};

const phase = async (label, setup) => {
  await setup();
  await sleep(4000); // let the fleet reach the state being measured
  const frames = await recordStream(SECONDS);
  const seconds = (frames.at(-1).at - frames[0].at) / 1000;
  const perSecond = (n) => n / seconds;
  const states = frames.filter((f) => f.event === "state").map((f) => realistic(JSON.parse(f.data)));
  const tape = frames.filter((f) => f.event === "cloudevent").reduce((n, f) => n + bytes(sse("cloudevent", f.data)), 0);
  const { out, churn } = strategies(states);
  const attentionNow = states.at(-1).apis.filter((a) => a.state !== "CLOSED").length;

  console.log(`\n== ${label} ==`);
  console.log(
    `  ${states.length} state frames in ${seconds.toFixed(1)}s · ${states[0].apis.length} APIs · ` +
      `${attentionNow} not CLOSED at the end · ` +
      `${(churn.changed / Math.max(1, states.length - 1)).toFixed(1)} APIs changed per frame, ` +
      `${churn.changed ? Math.round((churn.replicasOnly / churn.changed) * 100) : 0}% of those in the replica strip alone`,
  );
  console.log(`  event tape, the same in every scheme: ${fmt(perSecond(tape))} raw`);
  console.log(
    `  ${"state channel".padEnd(26)}${"raw".padStart(12)}${"gzip".padStart(12)}${"cpu ms/s".padStart(10)}` +
      `${"brotli 4M".padStart(12)}${"cpu ms/s".padStart(10)}${"on connect".padStart(12)}`,
  );
  const rows = [
    ["full frame (today)", out.full],
    ["per-API delta", out.apiDelta],
    ["per-field delta", out.fieldDelta],
    [`attention ${PAGE} + counts`, out.attention],
    [`attention ${PAGE}, as deltas`, out.attentionDelta],
  ];
  // Steady state only. Every delta scheme opens with a whole snapshot, and
  // amortising one 1.1 MB frame over a twenty-second window reports 56 KB/s
  // for a quiet fleet in which nothing is being sent at all. What connecting
  // costs is printed on its own line instead, since it is paid once per
  // browser rather than per second.
  //
  // Compression still runs over the whole list, snapshot first, because that
  // is what the compressor's window really contains; the snapshot's compressed
  // size is subtracted by compressing it alone.
  const measured = [];
  for (const [name, list] of rows) {
    const [first, ...rest] = list;
    const raw = rest.reduce((n, f) => n + bytes(f), 0);
    const gzAll = await gzip(list.filter(Boolean));
    const gzFirst = await gzip([first]);
    const brAll = await brotli(list.filter(Boolean));
    const brFirst = await brotli([first]);
    const row = {
      name,
      connect: bytes(first),
      raw: raw / seconds,
      gzip: Math.max(0, gzAll.out - gzFirst.out) / seconds,
      gzipCpu: gzAll.cpuMs / seconds,
      brotli: Math.max(0, brAll.out - brFirst.out) / seconds,
      brotliCpu: brAll.cpuMs / seconds,
    };
    measured.push(row);
    console.log(
      `  ${name.padEnd(26)}${fmt(row.raw).padStart(12)}${fmt(row.gzip).padStart(12)}` +
        `${row.gzipCpu.toFixed(1).padStart(10)}${fmt(row.brotli).padStart(12)}${row.brotliCpu.toFixed(1).padStart(10)}` +
        `${(row.connect >= 1e6 ? `${(row.connect / 1e6).toFixed(2)} MB` : `${(row.connect / 1e3).toFixed(1)} KB`).padStart(12)}`,
    );
  }
  return measured;
};

const setRates = async (share, rate) => {
  const state = await (await fetch(`${BASE}/api/state`)).json();
  const ids = state.apis.map((a) => a.apiId);
  const count = Math.round(ids.length * share);
  // Sequential in small batches: a thousand concurrent POSTs at the process
  // being measured is its own load test.
  for (let i = 0; i < ids.length; i += 50) {
    await Promise.all(
      ids.slice(i, i + 50).map((apiId, j) =>
        fetch(`${BASE}/api/failure`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ apiId, rate: i + j < count ? rate : 0 }),
        }),
      ),
    );
  }
};

if (PARTS.has("1")) {
  console.log("part 1 — what each encoding would put on the wire, per connected browser");
  await phase("quiet: every API healthy", () => setRates(0, 0));
  await phase("incident: 5% of APIs at 50% failure", () => setRates(0.05, 0.5));
  await phase("storm: 25% of APIs at 50% failure", () => setRates(0.25, 0.5));
  await setRates(0, 0);
}

// ---------------------------------------------------------------------------
// Part 2: what connected browsers cost the process that runs the breaker.
// ---------------------------------------------------------------------------

const counter = (body, name) => Number(body.split("\n").find((l) => l.startsWith(`${name} `) || l.startsWith(`${name}{`))?.split(" ").at(-1));
/** utime + stime from /proc, in ms, at the kernel's 100 Hz tick. */
const cpuOf = (pid) => {
  const fields = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
  return (Number(fields[11]) + Number(fields[12])) * 10;
};
const rssOf = (pid) => Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"))[1]) / 1024;

if (PARTS.has("2")) {
  console.log("\npart 2 — concurrent streams against the control loop's own cadence");
  const clients = [];
  const open = async () => {
    const c = new AbortController();
    const res = await fetch(`${BASE}/api/stream`, { signal: c.signal });
    const reader = res.body.getReader();
    (async () => {
      try {
        for (;;) if ((await reader.read()).done) break;
      } catch {}
    })();
    clients.push(c);
  };
  for (const k of [0, 1, 5, 20, 50, 100]) {
    while (clients.length < k) await open();
    await sleep(3000);
    const m0 = await (await fetch(`${BASE}/metrics`)).text();
    const c0 = PID ? cpuOf(PID) : 0;
    const t0 = Date.now();
    await sleep(10_000);
    const m1 = await (await fetch(`${BASE}/metrics`)).text();
    const secs = (Date.now() - t0) / 1000;
    const rate = (name) => (counter(m1, name) - counter(m0, name)) / secs;
    const ticks = rate("egress_aggregator_ticks_total");
    // Absent before frames were shared, so NaN there rather than a misleading 0.
    const built = rate("egress_console_frames_built_total");
    const process_ = PID ? `  process cpu ${((cpuOf(PID) - c0) / secs / 10).toFixed(0).padStart(3)}%  rss ${rssOf(PID).toFixed(0)} MiB` : "";
    const frames = Number.isFinite(built) ? `  frames built/s ${built.toFixed(2)}` : "";
    console.log(`  ${String(k).padStart(3)} streams  ticks/s ${ticks.toFixed(2)}${frames}${process_}`);
  }
  clients.forEach((c) => c.abort());
}

// ---------------------------------------------------------------------------
// Part 3: what a compressor holds for as long as a browser stays connected.
// ---------------------------------------------------------------------------

if (PARTS.has("3")) {
  console.log("\npart 3 — compressor memory per connection");
  const frame = Buffer.from(JSON.stringify(realistic(await (await fetch(`${BASE}/api/state`)).json())));
  /** `N` differs by compressor because gzip's state is small enough that forty of
   *  them vanish into RSS noise and read as zero, which is not the same as free. */
  const held = async (label, make, flushKind, N) => {
    global.gc?.();
    await sleep(300);
    const before = process.memoryUsage().rss;
    const zs = Array.from({ length: N }, () => {
      const z = make();
      z.resume();
      return z;
    });
    for (let k = 0; k < 3; k++) await Promise.all(zs.map((z) => new Promise((r) => { z.write(frame); z.flush(flushKind, r); })));
    global.gc?.();
    await sleep(300);
    console.log(`  ${label.padEnd(30)} ${((process.memoryUsage().rss - before) / N / 2 ** 20).toFixed(2)} MiB each, ${N} held open`);
    zs.forEach((z) => z.end());
  };
  await held("gzip level 6, 32 KiB window", () => zlib.createGzip({ level: 6 }), zlib.constants.Z_SYNC_FLUSH, 1000);
  await held("brotli q5, 4 MiB window", () => zlib.createBrotliCompress(BROTLI_WIDE), zlib.constants.BROTLI_OPERATION_FLUSH, 40);
}
