// Envoy's outlier detection only reacts to requests it actually sees — driving
// flaky-upstream's /__fail alone changes nothing until traffic is flowing
// through the egress listener. This sends a steady trickle through every
// replica so a failure injected via /__fail shows up as consecutive_5xx /
// failure_percentage within the ~1s outlier_detection interval, the same way
// production traffic would.
//
// This generator is configured with exactly one address — EGRESS_ADDR — the
// same one any real client would be given. It never hardcodes replica names:
// it resolves that one hostname over DNS and fans out to whatever comes
// back, one independent loop per (resolved replica, route) pair, mirroring
// "stateless replicas behind an L4 LB" so each replica samples on its own —
// exactly the disagreement the aggregator resolves. In docker-compose,
// "envoy" is a shared alias on all three envoy-* containers (see
// docker-compose.yml), so resolving it is what actually discovers them;
// nothing here needs to know there are three, or what they're called.
import { setTimeout as sleep } from "node:timers/promises";
import dns from "node:dns/promises";

const EGRESS_ADDR = process.env.EGRESS_ADDR ?? "http://envoy:10000";
const { hostname, port, protocol } = new URL(EGRESS_ADDR);

const ROUTES = ["/payments", "/shipping", "/tax"];
const INTERVAL_MS = Number(process.env.TRAFFIC_INTERVAL_MS ?? 20);

async function hit(base, path) {
  try {
    await fetch(base + path, { signal: AbortSignal.timeout(2000) });
  } catch {
    // Timeouts and connection failures are expected once a cluster is fully
    // ejected or a route returns direct_response — this generator only needs
    // to keep sending, not to succeed.
  }
}

async function loop(base, path) {
  for (;;) {
    await hit(base, path);
    await sleep(INTERVAL_MS);
  }
}

// Resolved once at startup — matches Envoy's own STRICT_DNS clusters, which
// also resolve once and refresh on an interval rather than per request. A
// production version of this generator would re-resolve periodically to
// pick up replicas added after startup; this prototype doesn't need to.
const addresses = await dns.resolve4(hostname);
if (addresses.length === 0) {
  throw new Error(`${hostname} resolved to no addresses`);
}
const replicas = addresses.map((ip) => `${protocol}//${ip}:${port}`);

for (const base of replicas) {
  for (const path of ROUTES) loop(base, path);
}

console.log(
  `traffic generator: ${EGRESS_ADDR} resolved to ${replicas.length} replica(s) x ` +
    `${ROUTES.length} routes, one request every ${INTERVAL_MS}ms per pair ` +
    `(${replicas.join(", ")})`,
);
