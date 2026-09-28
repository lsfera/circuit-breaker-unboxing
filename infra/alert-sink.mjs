/**
 * The other end of Alertmanager's webhook: prints one line per alert and keeps the last hundred at /alerts. Swap the
 * receiver in infra/monitoring/alertmanager.yml for a real integration and this goes away.
 */

import { createServer } from "node:http";

const PORT = Number(process.env.PORT ?? 9095);
const seen = [];

const line = (alert) => {
  const { labels = {}, annotations = {}, status } = alert;
  const where = labels.instance ? ` [${labels.instance}]` : "";
  const api = labels.apiId ? ` api=${labels.apiId}` : "";
  const dependency = labels.dependency ? ` dependency=${labels.dependency}` : "";
  return (
    `${status === "resolved" ? "RESOLVED" : "FIRING  "} ` +
    `${labels.severity ?? "?"}/${labels.alertname ?? "?"}${api}${dependency}${where} — ` +
    `${annotations.summary ?? ""}` +
    (annotations.runbook_url ? ` (runbook: ${annotations.runbook_url})` : "")
  );
};

createServer((req, res) => {
  if (req.method === "GET" && req.url?.startsWith("/alerts")) {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ count: seen.length, recent: seen.slice(-100) }));
    return;
  }
  if (req.method !== "POST") {
    res.writeHead(405).end();
    return;
  }
  let body = "";
  req.on("data", (chunk) => (body += chunk));
  req.on("end", () => {
    try {
      const payload = JSON.parse(body);
      for (const alert of payload.alerts ?? []) {
        const text = line(alert);
        console.log(text);
        seen.push({ at: new Date().toISOString(), text, labels: alert.labels });
        if (seen.length > 100) seen.shift();
      }
    } catch (err) {
      console.log(`could not read an alert payload: ${String(err)}`);
    }
    // Always 200: Alertmanager retries a failed webhook, and a sink that
    // rejects what it cannot parse would turn one malformed notification into
    // a permanent retry loop.
    res.writeHead(200).end();
  });
}).listen(PORT, () => console.log(`alert sink listening on ${PORT}`));
