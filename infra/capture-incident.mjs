/**
 * Records the Grafana dashboard through one incident: a video of the whole run
 * and a screenshot at each moment worth looking at.
 *
 *   npm i --no-save --prefix /tmp/pw playwright-core
 *   PLAYWRIGHT_CORE=/tmp/pw/node_modules/playwright-core/index.mjs \
 *   CHROMIUM=~/.cache/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-linux-arm64/chrome-headless-shell \
 *   node infra/capture-incident.mjs [--out=docs/media] [--outage=12] [--steady=6] [--tail=6] [--window=1m] [--clientError=10] [--rate=1] [--capacity=0 --delayMs=100] [--height=1900]
 *
 * playwright-core is deliberately not a dependency of this repo; this is a
 * tool for the article, not part of the system. Assumes `docker compose up -d`
 * and the compose producer running, and reaches the services by name.
 * Screenshots: steady, mid-outage, restored, recovered, client-error.
 *
 * The client_error phase is this branch's own addition (02-rabbitmq-only-
 * breaker's original has no classify/client_error concept): a 503 outage alone
 * never puts a second line on "Failed and refused calls, by HTTP status," and
 * never shows client_error's whole point — the third party answering, refusing
 * one specific request, and every breaker staying CLOSED through it.
 */
import { mkdirSync, readdirSync, renameSync } from "node:fs";
import { homedir } from "node:os";

const flag = (name, fallback) => process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
const OUT = flag("out", "docs/media");
const OUTAGE_S = Number(flag("outage", 12));
const STEADY_S = Number(flag("steady", 6));
const TAIL_S = Number(flag("tail", 6));
const CLIENT_ERROR_S = Number(flag("clientError", 10));
// Share of calls that fail during the outage; below 1 it is a partial failure.
const RATE = Number(flag("rate", 1));
// A third party that is full, not broken: serves CAPACITY at once, DELAY_MS each, and 429s the rest.
const CAPACITY = Number(flag("capacity", 0));
const DELAY_MS = Number(flag("delayMs", 100));
const GRAFANA = process.env.GRAFANA ?? "http://grafana:3000";
const FLAKY = process.env.FLAKY_UPSTREAM ?? "http://flaky-upstream:8080";
const PROMETHEUS = process.env.PROMETHEUS ?? "http://prometheus:9090";
// Must be shorter than the quiet time before the run, or the previous incident shows at the left edge.
const WINDOW = flag("window", "1m");
const DASHBOARD = `${GRAFANA}/d/in-process-breaker/in-process-breaker-e28094-five-not-one?kiosk&from=now-${WINDOW}&to=now&refresh=1s`;
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
// Tall enough for this branch's own panel count (10, vs. 02-rabbitmq-only-breaker's 8) so the
// redrive/parked-queue/status panels aren't below the fold; screenshots use fullPage regardless.
const size = { width: 1600, height: Number(flag("height", 1900)) };
const browser = await chromium.launch({ executablePath });
const context = await browser.newContext({ viewport: size, recordVideo: { dir: OUT, size: { width: 1200, height: Math.round(size.height * 0.75) } } });
const page = await context.newPage();
const t0 = Date.now();
const shot = async (name) => {
  await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true });
  console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  ${name}.png`);
};

await page.goto(DASHBOARD, { waitUntil: "networkidle" });
await sleep(STEADY_S * 1000);
await shot("1-steady");

await setFailure(CAPACITY > 0 ? { capacity: CAPACITY, delayMs: DELAY_MS } : { rate: RATE });
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  outage begins`);
await sleep((OUTAGE_S * 1000) / 2);
await shot("2-mid-outage");
await sleep((OUTAGE_S * 1000) / 2);

await setFailure({});
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  third party restored`);
await sleep(3000);
await shot("3-restored");

const deadline = Date.now() + 180_000;
while (Date.now() < deadline && !(await allClosed().catch(() => false))) await sleep(1000);
console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  every breaker closed`);
// The dashboard reads Prometheus (scraped every 2s) and refreshes every 5s: give it time to show it.
await sleep(TAIL_S * 1000);
await shot("4-recovered");

// client_error: the third party is up and refuses this specific request (422), not down. classify
// (Breaker.ts) counts this as a cockatiel success, so no breaker trips — this phase needs no wait for
// recovery the way the outage above does, since there is nothing here for a breaker to recover from.
// --clientError=0 skips the phase.
if (CLIENT_ERROR_S > 0) {
  await setFailure({ rate: 1, status: 422 });
  console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  injecting client_error (422)`);
  await sleep(CLIENT_ERROR_S * 1000);
  await shot("5-client-error");

  await setFailure({});
  console.log(`t+${Math.round((Date.now() - t0) / 1000)}s  client_error cleared`);
  await sleep(TAIL_S * 1000);
}

const video = page.video();
await context.close();
await browser.close();
renameSync(await video.path(), `${OUT}/incident.webm`);
console.log(`wrote ${readdirSync(OUT).join(", ")}`);
