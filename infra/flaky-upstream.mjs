// The upstreams Envoy's clusters point at — several per API, not one.
//
// One host per cluster is the shape this repo cannot demonstrate anything
// with: a replica's report is then either 0/1 or 1/1, so `healthy < total` is
// unreachable, no replica can ever vote DEGRADED from partial ejection, and
// `failure_percentage_*` (which needs failure_percentage_minimum_hosts) never
// evaluates at all. The endpoint counts below match the simulated fleet's in
// packages/aggregator/src/main.ts on purpose, so the two FleetSource layers
// tell the same story rather than merely producing the same record shape.
//
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0}'                  # 503s
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0,"status":422}'     # 422s: refused, not down
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"hang"}'    # never answers
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"reset"}'   # drops the connection
//   curl -X POST localhost:8080/__fail -d '{"delayMs":1500}'              # slow, still correct
//   for p in $(seq 8080 8085); do ... ; done                              # the whole cluster
//
// Every field is optional and a POST replaces the whole behaviour, so `{}`
// restores a healthy endpoint.
import { createServer, STATUS_CODES } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";

// 8086-8089 is deliberately skipped: the aggregator pair publishes 8088 and
// 8089, and a contiguous range through them collides on `docker compose up`.
const CLUSTERS = {
  "payments-provider": [8080, 8081, 8082, 8083, 8084, 8085],
  "shipping-rates": [8090, 8091, 8092, 8093],
  "tax-calc": [8094, 8095, 8096],
};

/**
 * Which messages were answered 200, by idempotency key `<run>:<n>`, so a test
 * can prove per message that nothing was lost and count exact duplicates:
 *
 *   curl 'localhost:8080/__audit?run=abc'            # bitmap of n, plus counts
 *   curl -X DELETE 'localhost:8080/__audit?run=abc'
 *   curl 'localhost:8080/__audit'                    # counts summed over every run
 *
 * Keys in any other shape are counted and otherwise ignored.
 */
const audits = new Map();
let foreignKeys = 0;
const recordProcessed = (key) => {
  const at = typeof key === "string" ? key.lastIndexOf(":") : -1;
  const n = at > 0 ? Number(key.slice(at + 1)) : NaN;
  if (!Number.isInteger(n) || n < 0) return void (key && foreignKeys++);
  const run = key.slice(0, at);
  const a = audits.get(run) ?? { bits: new Uint8Array(1 << 18), processed: 0, duplicates: 0, maxN: -1 };
  audits.set(run, a);
  const b = n >> 3;
  if (b >= a.bits.length) {
    const grown = new Uint8Array(Math.max(a.bits.length * 2, b + 1));
    grown.set(a.bits);
    a.bits = grown;
  }
  if (a.bits[b] & (1 << (n & 7))) a.duplicates++;
  else {
    a.bits[b] |= 1 << (n & 7);
    a.processed++;
  }
  a.maxN = Math.max(a.maxN, n);
};

const MODES = new Set(["error", "hang", "reset"]);
const HEALTHY = { rate: 0, mode: "error", delayMs: 0, status: 503 };
const behaviour = new Map();

const parse = (raw) => {
  const body = JSON.parse(raw || "{}");
  return {
    rate: Math.min(1, Math.max(0, Number(body.rate) || 0)),
    mode: MODES.has(body.mode) ? body.mode : "error",
    delayMs: Math.min(60_000, Math.max(0, Number(body.delayMs) || 0)),
    status: Number.isInteger(body.status) && body.status >= 400 && body.status <= 599 ? body.status : 503,
  };
};

/** Answers one request the way this endpoint is currently told to behave. */
const misbehave = async (b, req, res, ok, failure) => {
  if (b.delayMs > 0) await sleep(b.delayMs);
  if (Math.random() >= b.rate) return ok();
  // A hung request holds its socket until the client gives up; nothing is ever written.
  if (b.mode === "hang") return;
  if (b.mode === "reset") return req.socket.destroy();
  return failure();
};

for (const [cluster, ports] of Object.entries(CLUSTERS)) {
  for (const port of ports) {
    behaviour.set(port, HEALTHY);
    createServer(async (req, res) => {
      if (req.url === "/__fail" && req.method === "POST") {
        const chunks = [];
        for await (const c of req) chunks.push(c);
        behaviour.set(port, parse(Buffer.concat(chunks).toString()));
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ cluster, port, ...behaviour.get(port) }));
      }
      if (req.url.startsWith("/__audit")) {
        const run = new URL(req.url, "http://x").searchParams.get("run");
        if (run === null) {
          const all = [...audits.values()];
          res.writeHead(200, { "content-type": "application/json" });
          return res.end(
            JSON.stringify({
              runs: all.length,
              processed: all.reduce((sum, a) => sum + a.processed, 0),
              duplicates: all.reduce((sum, a) => sum + a.duplicates, 0),
              foreignKeys,
            }),
          );
        }
        if (req.method === "DELETE") audits.delete(run);
        const a = audits.get(run);
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(
          JSON.stringify({
            run,
            processed: a?.processed ?? 0,
            duplicates: a?.duplicates ?? 0,
            maxN: a?.maxN ?? -1,
            foreignKeys,
            bits: a && req.method !== "DELETE" ? Buffer.from(a.bits.subarray(0, Math.ceil((a.maxN + 1) / 8))).toString("base64") : "",
          }),
        );
      }
      const b = behaviour.get(port);

      // Active health checking samples the same failing service real traffic
      // does, so it fails at the same rate rather than being a separate
      // truth. That matters at partial failure rates: a deterministic health
      // endpoint would mark every host down at once and collapse DEGRADED
      // into OPEN, which is exactly the state this demo needs to be able to
      // reach. It also makes successful_active_health_check_uneject_host
      // real — a recovered upstream passes its next check and Envoy
      // un-ejects immediately, instead of waiting out base_ejection_time ×
      // ejection_count.
      if (req.url === "/__health") {
        return misbehave(
          b,
          req,
          res,
          () => (res.writeHead(200), res.end("ok")),
          () => (res.writeHead(503), res.end("unhealthy")),
        );
      }

      return misbehave(
        b,
        req,
        res,
        () => {
          if (cluster === "payments-provider") recordProcessed(req.headers["x-idempotency-key"]);
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ cluster, port, ok: true }));
        },
        () => (res.writeHead(b.status), res.end(STATUS_CODES[b.status])),
      );
    }).listen(port);
  }
  console.log(`flaky upstream: ${cluster} on ${ports.join(", ")}`);
}
