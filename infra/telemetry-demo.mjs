#!/usr/bin/env node
// Generate a short, visible failure-and-recovery trace sequence in the running demo stack.
//
//   node infra/telemetry-demo.mjs
//   WINDOW_MS=5000 RECOVERY_MS=5000 node infra/telemetry-demo.mjs
//
// Assumes `docker compose up -d` is already running with telemetry enabled. This resets the fake upstream to
// healthy before the demo and again afterward, so do not run it during another injected-failure scenario.

const FLAKY_UPSTREAM = process.env.FLAKY_UPSTREAM;
const WINDOW_MS = Number(process.env.WINDOW_MS ?? "3000");
const RECOVERY_MS = Number(process.env.RECOVERY_MS ?? "3000");
const TRACE_UI = process.env.TRACE_UI ?? "http://localhost:3001/explore";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const findFlakyUpstream = async () => {
  const candidates = FLAKY_UPSTREAM
    ? [FLAKY_UPSTREAM]
    : ["http://flaky-upstream:8080", "http://localhost:8080"];
  const errors = [];

  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}/__audit?run=%2A`, { signal: AbortSignal.timeout(2000) });
      if (!response.ok) {
        errors.push(`${candidate}: HTTP ${response.status}`);
        continue;
      }
      const audit = await response.json();
      if (audit.run === "*" && Number.isSafeInteger(audit.processed)) return candidate;
      errors.push(`${candidate}: response was not the fake-upstream audit endpoint`);
    } catch (error) {
      errors.push(`${candidate}: ${error.message}`);
    }
  }

  throw new Error(
    `Could not reach the fake upstream (${errors.join("; ")}). Set FLAKY_UPSTREAM to its reachable URL.`
  );
};

const duration = (value, name) => {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer number of milliseconds; received ${value}`);
  }
  return value;
};

const postFailure = async (upstream, behavior) => {
  const response = await fetch(`${upstream}/__fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(behavior),
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok) {
    throw new Error(`Could not configure the fake upstream (${response.status} ${response.statusText})`);
  }
};

const main = async () => {
  const windowMs = duration(WINDOW_MS, "WINDOW_MS");
  const recoveryMs = duration(RECOVERY_MS, "RECOVERY_MS");
  const upstream = await findFlakyUpstream();
  let runError;

  console.log("== Telemetry demo ==");
  console.log("Requires the compose stack with EXPOSE_TELEMETRY=true and OTEL_EXPORTER_OTLP_ENDPOINT configured.");
  console.log(`Fake upstream: ${upstream}`);
  console.log(`LGTM Grafana (open Explore for traces): ${TRACE_UI}`);
  console.log("Look for rmq-producer / work.publish, then consumer / work.process, payments-api.charge and sql.execute.");

  try {
    await postFailure(upstream, {});
    console.log("\n== Generating failed payment spans ==");
    await postFailure(upstream, { rate: 1, status: 503 });
    const failedUntil = Date.now() + windowMs;
    while (Date.now() < failedUntil) {
      const remaining = Math.max(0, failedUntil - Date.now());
      console.log(`  failing upstream for another ${(remaining / 1000).toFixed(1)}s`);
      await sleep(Math.min(1000, remaining));
    }

    console.log("\n== Restoring payments and capturing recovery spans ==");
  } catch (error) {
    runError = error;
  } finally {
    try {
      await postFailure(upstream, {});
    } catch (error) {
      throw new Error(`Failed to restore ${upstream} to healthy behavior: ${error.message}`, { cause: error });
    }
  }

  if (runError) throw runError;
  const recoveryUntil = Date.now() + recoveryMs;
  while (Date.now() < recoveryUntil) {
    const remaining = Math.max(0, recoveryUntil - Date.now());
    console.log(`  healthy traffic for another ${(remaining / 1000).toFixed(1)}s`);
    await sleep(Math.min(1000, remaining));
  }

  console.log("\nDemo complete. Search recent traces in Grafana Explore using the service and span names above.");
};

main().catch((error) => {
  console.error(`Telemetry demo failed: ${error.message}`);
  process.exitCode = 1;
});
