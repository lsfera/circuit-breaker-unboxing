// Three upstreams with independently controllable failure rates, so the Envoy
// path can be driven exactly like the simulator.
//   curl -X POST localhost:8080/__fail -d '{"rate":1.0}'
import { createServer } from "node:http";

const rate = new Map();

for (const port of [8080, 8081, 8082]) {
  rate.set(port, 0);
  createServer(async (req, res) => {
    if (req.url === "/__fail" && req.method === "POST") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const { rate: r } = JSON.parse(Buffer.concat(chunks).toString() || "{}");
      rate.set(port, Math.min(1, Math.max(0, Number(r) || 0)));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ port, rate: rate.get(port) }));
    }
    if (Math.random() < rate.get(port)) {
      res.writeHead(503);
      return res.end("upstream unavailable");
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ port, ok: true }));
  }).listen(port);
  console.log(`flaky upstream on :${port}`);
}
