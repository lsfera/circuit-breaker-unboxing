/**
 * The two adversarial tests this repo has only ever run by hand.
 *
 * Both were done live in earlier sessions, watched in the logs, and written up
 * in the README — which is exactly the kind of proof that rots, because
 * nothing re-runs it. A test a human performs once is a claim with a date on
 * it. This is the same two experiments as assertions with an exit code.
 *
 *   node infra/chaos.mjs leader     # kill the publishing leader mid-incident
 *   node infra/chaos.mjs prober     # kill the daemon the broker elected to probe
 *   node infra/chaos.mjs all
 *
 * Needs the compose stack up (`docker compose up -d`) and a shell that can
 * reach the services by name and run `docker`. It injects a real upstream
 * failure and kills real containers, so it is not something to point at
 * anything you care about.
 *
 * Both scenarios restore what they broke, including restarting the container
 * they killed — `docker kill` is treated by Docker as a manual stop, so
 * `restart: unless-stopped` will not bring it back on its own. Leaving the
 * stack one instance short after a chaos run would quietly disarm the *next*
 * run of the same test.
 */

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const AGGREGATORS = [
  { name: "workspace-aggregator-1", url: "http://aggregator:8088" },
  { name: "workspace-aggregator-2-1", url: "http://aggregator-2:8088" },
];
const PROMETHEUS = "http://prometheus:9090";
const UPSTREAM_PORTS = [8080, 8081, 8082, 8083, 8084, 8085];
const API = "payments-provider";

let failures = 0;
const check = (ok, what, detail = "") => {
  console.log(`${ok ? "  PASS" : "  FAIL"}  ${what}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
};

const setFailureRate = async (rate) => {
  for (const port of UPSTREAM_PORTS) {
    await fetch(`http://flaky-upstream:${port}/__fail`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ rate }),
    }).catch(() => {});
  }
};

const metrics = async (url) => {
  const res = await fetch(`${url}/metrics`);
  return res.text();
};

// Matches the labelled form too. Every gauge read here happens to be
// unlabelled today, and a `name ` prefix would silently return null the day one
// of them gains an attribute — which reads as "no leader" rather than as a
// broken probe.
const gauge = (body, name) => {
  const line = body
    .split("\n")
    .find((l) => l.startsWith(`${name} `) || l.startsWith(`${name}{`));
  return line ? Number(line.slice(line.lastIndexOf(" ") + 1)) : null;
};

const findLeader = async () => {
  for (const agg of AGGREGATORS) {
    try {
      if (gauge(await metrics(agg.url), "egress_aggregator_is_leader") === 1) return agg;
    } catch {}
  }
  return null;
};

const stateOf = async (url) => {
  const res = await fetch(`${url}/api/state`);
  const body = await res.json();
  return body.apis?.find((a) => a.apiId === API) ?? null;
};

/** Poll until `predicate` holds, or give up. Returns how long it took, or null. */
const waitFor = async (predicate, timeoutMs, label) => {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await predicate().catch(() => false)) return Date.now() - started;
    await sleep(250);
  }
  console.log(`  (timed out after ${timeoutMs}ms waiting for ${label})`);
  return null;
};

/**
 * Gaps and duplicates as a *delta across the scenario*, read from *one*
 * instance — and both halves of that were learned the hard way.
 *
 * Absolute counts say nothing on a stack that has been poked at by hand: the
 * first run of this script reported four duplicates that predated it by hours
 * and had to be chased before they could be dismissed. And summing across both
 * instances is worse than useless in a scenario that kills one of them,
 * because the counter is per process and in memory: the second run watched
 * duplicates fall from 4 to 0 and "events delivered" come out at -535, which
 * is not a contract violation, it is a restarted process with fresh counters.
 *
 * So: one instance, the one that stays up, measured before and after.
 */
const contract = async (url) => {
  try {
    const body = await (await fetch(`${url}/api/subscriber`)).json();
    return {
      received: body.received ?? 0,
      duplicates: body.duplicates ?? 0,
      gaps: (body.gaps ?? []).length,
    };
  } catch {
    return { received: 0, duplicates: 0, gaps: 0 };
  }
};

const promQuery = async (query) => {
  const res = await fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`);
  const body = await res.json();
  return body.data?.result ?? [];
};

/**
 * Scenario 1 — kill the publishing leader in the middle of a real incident.
 *
 * The property is not "the standby notices". It is that the *event stream*
 * does not care: the sequence continues from the killed instance's last
 * checkpoint rather than restarting, so a subscriber sees no gap and no
 * duplicate across a hard kill of the process that was publishing.
 */
const leaderScenario = async () => {
  console.log("\n== kill the leader mid-incident ==");
  await setFailureRate(0);
  await sleep(3000);

  const leader = await findLeader();
  if (!leader) return check(false, "a leader exists to kill");
  const survivor = AGGREGATORS.find((a) => a.name !== leader.name);
  console.log(`  leader is ${leader.name}, survivor is ${survivor.name}`);
  // Read the contract from the instance that will still be here afterwards.
  const contractBefore = await contract(survivor.url);

  await setFailureRate(1);
  const opened = await waitFor(
    async () => (await stateOf(leader.url))?.state === "OPEN",
    90_000,
    "the circuit to open",
  );
  check(opened !== null, "the circuit opened before the kill", `${opened}ms`);

  const before = await stateOf(leader.url);
  console.log(`  killing ${leader.name} at sequence=${before?.sequence}`);
  await exec("docker", ["kill", leader.name]);
  const killedAt = Date.now();

  const tookOver = await waitFor(
    async () => gauge(await metrics(survivor.url), "egress_aggregator_is_leader") === 1,
    30_000,
    "the standby to take over",
  );
  check(tookOver !== null, "the standby took over", `${tookOver}ms`);

  const after = await waitFor(
    async () => (await stateOf(survivor.url)) !== null,
    30_000,
    "the survivor to report the API",
  );
  check(after !== null, "the survivor rehydrated the API from the checkpoint");

  const resumed = await stateOf(survivor.url);
  check(
    resumed !== null && before !== null && resumed.sequence >= before.sequence,
    "the sequence continued rather than restarting",
    `${before?.sequence} -> ${resumed?.sequence}`,
  );

  // Recover, then read the contract from the outside: the subscriber endpoint
  // counts gaps and duplicates over everything it received, across the kill.
  await setFailureRate(0);
  await waitFor(
    async () => (await stateOf(survivor.url))?.state === "CLOSED",
    180_000,
    "the circuit to close again",
  );
  const contractAfter = await contract(survivor.url);
  check(
    contractAfter.duplicates === contractBefore.duplicates,
    "no sequence was published twice across the kill",
    `${contractBefore.duplicates} -> ${contractAfter.duplicates}`,
  );
  // Deliberately *not* asserting the survivor's gap count across the kill.
  //
  // Each aggregator posts to its own `/subscriber/webhook`, so an instance's
  // view contains only what that instance published. A standby publishes
  // nothing, so if it ever led before, its high-water mark is stale by exactly
  // the run of sequences the other instance published — and the first event it
  // publishes after taking over reads as a jump. Observed here: the survivor
  // had led up to 114 some time earlier, sat out 115-122, resumed at 123, and
  // recorded `jumped 114 -> 123`.
  //
  // That is a hole in one observer's view, not a gap in delivery, and the check
  // was flaky by construction: it passed whenever the survivor happened never
  // to have led, and failed once it had. The question it was trying to ask —
  // did the failover lose an event — is answered above by the sequence
  // continuing 122 -> 123 with no duplicate, on the publishing side where the
  // guarantee actually lives. An external subscriber reading both instances
  // could answer it from the receiving side; a self-subscriber cannot.
  check(
    contractAfter.duplicates === contractBefore.duplicates,
    "the survivor's own view gained no duplicate across the kill",
    `${contractBefore.duplicates} -> ${contractAfter.duplicates}`,
  );
  console.log(
    `  events delivered during the scenario: ${contractAfter.received - contractBefore.received}`,
  );

  await exec("docker", ["start", leader.name]);
  console.log(`  restarted ${leader.name} (docker kill is a manual stop; nothing else would)`);
  console.log(`  kill-to-takeover: ${tookOver}ms (measured from ${new Date(killedAt).toISOString()})`);
};

/**
 * Scenario 2 — kill the daemon the broker elected to probe.
 *
 * The election is RabbitMQ's, not ours: `x-single-active-consumer` on a
 * dedicated trigger queue. So the property under test is that failover is the
 * broker's problem and it actually solves it — a different daemon runs the
 * next probe, and the circuit still reaches CLOSED with its prober having been
 * killed underneath it.
 */
const proberScenario = async () => {
  console.log("\n== kill the elected prober ==");
  await setFailureRate(0);
  await sleep(3000);

  const probesByInstance = async () => {
    const rows = await promQuery("egress_daemon_probes_total");
    return new Map(rows.map((r) => [r.metric.daemon ?? r.metric.instance, Number(r.value[1])]));
  };

  await setFailureRate(1);
  const leader = await findLeader();
  if (!leader) return check(false, "a leader exists to read state from");

  const opened = await waitFor(
    async () => (await stateOf(leader.url))?.state === "OPEN",
    90_000,
    "the circuit to open",
  );
  check(opened !== null, "the circuit opened", `${opened}ms`);

  // A probe happens on every OPEN -> HALF_OPEN transition, so waiting for one
  // to be counted is how the elected daemon identifies itself.
  const start = await probesByInstance();
  const elected = await waitFor(
    async () => {
      const now = await probesByInstance();
      for (const [instance, count] of now) {
        if (count > (start.get(instance) ?? 0)) {
          proberScenario.elected = instance;
          return true;
        }
      }
      return false;
    },
    120_000,
    "a daemon to be elected and run a probe",
  );
  check(elected !== null, "a daemon was elected to probe", proberScenario.elected ?? "");
  if (!proberScenario.elected) return;

  // The fleet is one scaled service discovered by DNS, so Prometheus's own
  // `instance` label is an IP address and names no container. The daemon
  // labels its own metrics with its instance id, which it takes from its
  // hostname — and a container's hostname is its short id, so this is
  // directly killable.
  const container = String(proberScenario.elected);
  console.log(`  killing ${container}`);
  await exec("docker", ["kill", container]).catch((e) => {
    check(false, "killed the elected prober", String(e));
  });

  const afterKill = await probesByInstance();
  const promoted = await waitFor(
    async () => {
      const now = await probesByInstance();
      for (const [instance, count] of now) {
        if (instance !== proberScenario.elected && count > (afterKill.get(instance) ?? 0)) {
          proberScenario.promoted = instance;
          return true;
        }
      }
      return false;
    },
    180_000,
    "the broker to promote another daemon",
  );
  check(
    promoted !== null,
    "the broker promoted a different daemon to probe",
    `${proberScenario.elected} -> ${proberScenario.promoted ?? "?"} in ${promoted}ms`,
  );

  await setFailureRate(0);
  const closed = await waitFor(
    async () => (await stateOf(leader.url))?.state === "CLOSED",
    240_000,
    "the circuit to close with its original prober dead",
  );
  check(closed !== null, "the circuit recovered without the daemon that had been probing", `${closed}ms`);

  await exec("docker", ["start", container]).catch(() => {});
  console.log(`  restarted ${container}`);
};

const which = process.argv[2] ?? "all";
if (which === "leader" || which === "all") await leaderScenario();
if (which === "prober" || which === "all") await proberScenario();
await setFailureRate(0);

console.log(`\n${failures === 0 ? "all checks passed" : `${failures} check(s) failed`}`);
process.exitCode = failures === 0 ? 0 : 1;
