// Envoy's outlier detection only reacts to requests it actually sees — driving
// flaky-upstream's /__fail alone changes nothing until traffic is flowing
// through the egress listener. This sends a steady trickle through every
// replica so a failure injected via /__fail shows up as consecutive_5xx /
// failure_percentage within the ~1s outlier_detection interval, the same way
// production traffic would.
//
// One independent loop per (replica, route) pair, round-robin across replicas
// — mirroring "stateless replicas behind an L4 LB" — so each replica samples
// on its own, which is exactly the disagreement the aggregator resolves.
import { setTimeout as sleep } from "node:timers/promises";

const REPLICAS = (
  process.env.ENVOY_REPLICAS ??
  "http://envoy-00:10000,http://envoy-01:10000,http://envoy-02:10000"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

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

for (const base of REPLICAS) {
  for (const path of ROUTES) loop(base, path);
}

console.log(
  `traffic generator: ${REPLICAS.length} replicas x ${ROUTES.length} routes, ` +
    `one request every ${INTERVAL_MS}ms per pair (${REPLICAS.join(", ")})`,
);
