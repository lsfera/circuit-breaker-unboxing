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
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0}'   # one endpoint
//   for p in $(seq 8080 8085); do ... ; done               # the whole cluster
import { createServer } from "node:http";

// 8086-8089 is deliberately skipped: the aggregator pair publishes 8088 and
// 8089, and a contiguous range through them collides on `docker compose up`.
const CLUSTERS = {
  "payments-provider": [8080, 8081, 8082, 8083, 8084, 8085],
  "shipping-rates": [8090, 8091, 8092, 8093],
  "tax-calc": [8094, 8095, 8096],
};

const rate = new Map();

for (const [cluster, ports] of Object.entries(CLUSTERS)) {
  for (const port of ports) {
    rate.set(port, 0);
    createServer(async (req, res) => {
      if (req.url === "/__fail" && req.method === "POST") {
        const chunks = [];
        for await (const c of req) chunks.push(c);
        const { rate: r } = JSON.parse(Buffer.concat(chunks).toString() || "{}");
        rate.set(port, Math.min(1, Math.max(0, Number(r) || 0)));
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ cluster, port, rate: rate.get(port) }));
      }

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
        if (Math.random() < rate.get(port)) {
          res.writeHead(503);
          return res.end("unhealthy");
        }
        res.writeHead(200);
        return res.end("ok");
      }

      if (Math.random() < rate.get(port)) {
        res.writeHead(503);
        return res.end("upstream unavailable");
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ cluster, port, ok: true }));
    }).listen(port);
  }
  console.log(`flaky upstream: ${cluster} on ${ports.join(", ")}`);
}
