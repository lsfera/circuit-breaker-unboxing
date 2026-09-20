/**
 * Records the Grafana dashboard through one incident: a video of the whole run
 * and a screenshot at each moment worth looking at.
 *
 *   npm i --no-save --prefix /tmp/pw playwright-core
 *   PLAYWRIGHT_CORE=/tmp/pw/node_modules/playwright-core/index.mjs \
 *   CHROMIUM=~/.cache/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-linux-arm64/chrome-headless-shell \
 *   node infra/capture-incident.mjs [--out=docs/media] [--outage=45]
 *
 * playwright-core is deliberately not a dependency of this repo; this is a
 * tool for the article, not part of the system. Assumes `docker compose up -d`
 * and the compose producer running, and reaches the services by name.
 * Screenshots: steady, mid-outage, restored, recovered.
 */
import { mkdirSync, readdirSync, renameSync } from "node:fs";
import { homedir } from "node:os";

const flag = (name, fallback) => process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
const OUT = flag("out", "docs/media");
const OUTAGE_S = Number(flag("outage", 45));
const GRAFANA = process.env.GRAFANA ?? "http://grafana:3000";
const FLAKY = process.env.FLAKY_UPSTREAM ?? "http://flaky-upstream:8080";
const PROMETHEUS = process.env.PROMETHEUS ?? "http://prometheus:9090";
const DASHBOARD = `${GRAFANA}/d/in-process-breaker/in-process-breaker-e28094-five-not-one?kiosk&from=now-4m&to=now&refresh=1s`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const { chromium } = await import(process.env.PLAYWRIGHT_CORE ?? "playwright-core");
const executablePath =
  process.env.CHROMIUM ??
  `${homedir()}/.cache/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-linux-arm64/chrome-headless-shell`;

const setFailure = (body) =>
  fetch(`${FLAKY}/__fail`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

const allClosed = async () => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=egress_consumer_breaker_state`);
  const states = (await res.json()).data.result.map((r) => Number(r.value[1]));
  return states.length >= 5 && states.every((s) => s === 0);
};

mkdirSync(OUT, { recursive: true });
const size = { width: 1600, height: 1250 };
const browser = await chromium.launch({ executablePath });
const context = await browser.newContext({ viewport: size, recordVideo: { dir: OUT, size: { width: 1200, height: 938 } } });
const page = await context.newPage();
const t0 = Date.now();
const shot = async (name) => {
  await page.screenshot({ path: `${OUT}/${name}.png` });
  console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  ${name}.png`);
};

await page.goto(DASHBOARD, { waitUntil: "networkidle" });
await sleep(15_000);
await shot("1-steady");

await setFailure({ rate: 1 });
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  outage begins`);
await sleep((OUTAGE_S * 1000) / 2);
await shot("2-mid-outage");
await sleep((OUTAGE_S * 1000) / 2);

await setFailure({});
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  third party restored`);
await sleep(4000);
await shot("3-restored");

const deadline = Date.now() + 180_000;
while (Date.now() < deadline && !(await allClosed().catch(() => false))) await sleep(1000);
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  every breaker closed`);
// The dashboard reads Prometheus (scraped every 2s) and refreshes every 5s: give it time to show it.
await sleep(20_000);
await shot("4-recovered");

const video = page.video();
await context.close();
await browser.close();
renameSync(await video.path(), `${OUT}/incident.webm`);
console.log(`wrote ${readdirSync(OUT).join(", ")}`);
