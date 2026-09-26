/**
 * Chaos for the RabbitMQ-held breaker: real faults, injected under a traffic spike, each judged first on
 * correctness and then on what the breaker did.
 *
 *   node infra/chaos-breaker.mjs                      # every scenario
 *   node infra/chaos-breaker.mjs --scenarios=outage,kill-broker-while-open
 *   node infra/chaos-breaker.mjs --list
 *
 * Correctness is judged per message, not from broker counters: every message carries `message_id: <run>:<n>`,
 * the publisher reports which n the broker confirmed, the flaky upstream reports which n it answered 200, and
 * after the fleet drains `unprocessed = confirmed ∧ ¬processed` is either parked in the dead-letter queue or
 * lost. The bar is that nothing is lost and the dead-letter queue has not grown; the upstream's audit counts
 * exact duplicates too.
 *
 * Then the breaker: every replica's log is read back and each transition checked against the machine (closed →
 * open → half-open → closed | open), and the broker's own consumer count on the work queue is compared with the
 * number of replicas that are not open, which is the claim the design rests on. Every scenario also ends
 * with exactly one probe permit in the broker, and the dead-letter queue back where it started.
 *
 * Runs against either breaker design in this series, detected from the replicas' logs: `held` (this branch: the
 * open state is a token in the broker) or `cockatiel` (article 3: an in-process breaker, run by that branch's
 * compose file). Scenarios that only mean something for one design say so and are skipped for the other.
 *
 * Assumes `docker compose up -d`, a shell that reaches the services by name and can run `docker`. It kills real
 * containers and restarts the broker, so do not point it at anything you care about. The compose producer is
 * stopped for a run (all traffic comes from one forked publisher) and started again at the end, along with
 * anything a scenario killed.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { bits as delayBits, entryLevel, levelName, routingKey, DELIVERY_EXCHANGE, bindingKey } from "../packages/rmq/src/DelayedDelivery.ts";
import * as lib from "./chaos-lib.mjs";

const { amqp, audit, BROKER, consumerContainers, decodeBits, exec, flag, isSet, PROJECT, promSum, queueInfo, setFailure, sleep } = lib;
const { skewMs, tokensInChain, tryQueueInfo, waitFor } = lib;

const API = "payments-provider";
/** The breaker under test: the consumer application's third party. Article 3's single breaker has no name. */
const DEPENDENCY = "payments-api";
const WORK = `${API}.work`;
const DEAD = `${API}.work.dead`;
const REDRIVE_TRIGGER = `${API}.redrive-trigger`;

const BASE_RATE = Number(flag("rate", 200));
const SPIKE_RATE = Number(flag("spike", 1000));
const SETTLE_S = Number(flag("settle", 8));
const FAULT_S = Number(flag("fault-seconds", 40));
const RECOVERY_TIMEOUT_S = Number(flag("recovery-timeout", 240));
const DRAIN_TIMEOUT_S = Number(flag("drain-timeout", 120));
const OUT = flag("out", `history/runs/chaos-breaker-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

let DESIGN = "held";
const named = () => (DESIGN === "held" ? DEPENDENCY : undefined);
const breakerStates = () => lib.breakerStates(named());
const permitQueue = () => (DESIGN === "held" ? lib.permitQueueFor(DEPENDENCY) : `${API}.probe-permit`);
const permitTokens = () => lib.permitTokens(permitQueue());
const activeRedriver = () => lib.activeRedriver(REDRIVE_TRIGGER);
const transitionsOf = (container, since) => lib.transitionsOf(container, since, { dependency: named(), design: DESIGN });
const containerIn = (phase) => lib.containerIn(phase, named());
const openContainer = () => containerIn("open");
const detectDesign = lib.detectDesign;
const startPublisher = (run) => lib.startPublisher(run, API, BASE_RATE);

// ---- a run -------------------------------------------------------------------

const compose = (...a) => exec("docker", ["compose", ...a]);

/** Poll once a second for `seconds`, recording what the broker and the fleet are doing. */
const observe = async (series, t0, seconds, until = () => false) => {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end && !until()) {
    const [work, dead, states, tokens] = await Promise.all([
      tryQueueInfo(WORK),
      tryQueueInfo(DEAD),
      breakerStates(),
      tokensInChain().catch(() => undefined),
    ]);
    const count = (s) => states?.filter((x) => x === s).length ?? null;
    series.push({
      t: Math.round((Date.now() - t0) / 1000),
      work: work?.ready ?? null,
      consumers: work?.consumers ?? null,
      dead: dead?.ready ?? null,
      tokens: tokens ?? null,
      closed: count(0),
      open: count(1),
      halfOpen: count(2),
    });
    await sleep(1000);
  }
};

const allClosed = async () => {
  const states = await breakerStates();
  return states !== undefined && states.length >= 5 && states.every((s) => s === 0);
};

/**
 * @param fault {{ inject: () => Promise<void>, during?: (ctx) => Promise<void>, restore: () => Promise<void> }}
 */
const scenario = async (name, fault) => {
  const started = new Date();
  const skewAtStart = skewMs();
  const run = `${name.replace(/[^a-z]/g, "").slice(0, 6)}${Date.now().toString(36)}`;
  console.log(`\n== ${name} (run ${run}) ==`);
  await setFailure({});
  await exec("docker", ["stop", `${PROJECT}-rmq-producer-1`]).catch(() => {});
  await waitFor(allClosed, 60);
  const deadBefore = (await queueInfo(DEAD)).ready;
  const series = [];
  const t0 = Date.now();
  const publisher = startPublisher(run);

  await observe(series, t0, SETTLE_S);
  publisher.rate(SPIKE_RATE);
  const faultAt = Math.round((Date.now() - t0) / 1000);
  console.log(`  t+${faultAt}s  fault injected, ${BASE_RATE} → ${SPIKE_RATE}/s`);
  await fault.inject();
  const ctx = { series, t0, run };
  const during = fault.during?.(ctx) ?? Promise.resolve();
  await observe(series, t0, FAULT_S);
  await during;
  await fault.restore();
  publisher.rate(BASE_RATE);
  const restoredAt = Math.round((Date.now() - t0) / 1000);
  console.log(`  t+${restoredAt}s  restored, back to ${BASE_RATE}/s`);

  const recovered = await waitFor(async () => {
    await observe(series, t0, 1);
    return allClosed();
  }, RECOVERY_TIMEOUT_S);
  const recoveredAt = Math.round((Date.now() - t0) / 1000);

  const final = await publisher.stop();
  const drained = await waitFor(async () => {
    await observe(series, t0, 1);
    const w = await tryQueueInfo(WORK);
    return w?.ready === 0;
  }, DRAIN_TIMEOUT_S);
  // Whatever a scenario put in the dead-letter queue has to come back out through the redrive.
  const deadDrained = await waitFor(async () => {
    await observe(series, t0, 1);
    const d = await tryQueueInfo(DEAD);
    const w = await tryQueueInfo(WORK);
    return d?.ready === deadBefore && w?.ready === 0;
  }, DRAIN_TIMEOUT_S * 2);
  await sleep(3000);

  const upstream = await audit(run);
  const extra = await (fault.check?.(ctx) ?? Promise.resolve({ pass: true }));
  const permit = await permitTokens().catch(() => ({ ready: null, held: null }));
  // `increase`, not a before/after difference: a killed replica's counters restart at zero.
  const window = `${Math.ceil((Date.now() - started.getTime()) / 1000)}s`;
  const permitLost = Math.round(await promSum(`sum(increase(egress_consumer_probe_permit_lost_total[${window}]))`));
  const redrives = Math.round(await promSum(`sum(increase(egress_consumer_redrives_total[${window}]))`));
  const calls = Object.fromEntries(
    await Promise.all(
      ["ok", "failed", "open", "client_error"].map(async (o) => [
        o,
        Math.round(await promSum(`sum(increase(egress_consumer_calls_total{outcome="${o}"}[${window}]))`)),
      ]),
    ),
  );
  const processed = decodeBits(upstream.bits);
  const confirmed = decodeBits(final.bits);
  const dead = (await queueInfo(DEAD)).ready - deadBefore;
  const unprocessed = [];
  for (let n = 0; n < final.sent; n++) if (isSet(confirmed, n) && !isSet(processed, n)) unprocessed.push(n);
  // Whatever the third party never answered 200 for is either parked in the
  // dead-letter queue or gone; only the second is a loss.
  const lost = unprocessed.slice(0, Math.max(0, unprocessed.length - dead));

  const replicas = await Promise.all(
    (await consumerContainers()).map((c) => transitionsOf(c, started.toISOString())),
  );
  const violations = replicas.flatMap((r) => r.violations.map((v) => `${r.container}: ${v}`));
  const holds = replicas.flatMap((r) => r.holds);
  const maxHold = Math.max(0, ...holds.map((h) => h.seconds));
  const maxAttempt = Math.max(0, ...holds.map((h) => h.attempt));
  const openings = replicas.reduce((sum, r) => sum + r.openings, 0);

  // "Open is no consumer": while the fleet's gauges are all in, the broker's own
  // consumer count should be the replicas that are not open (half-open consumes).
  const comparable =
    DESIGN === "held"
      ? series.filter((s) => s.consumers !== null && s.closed !== null && s.closed + s.open + s.halfOpen >= 5)
      : [];
  const matching = comparable.filter((s) => s.consumers === s.closed + s.halfOpen);
  const peakOpen = Math.max(0, ...series.map((s) => s.open ?? 0));
  const peakTokens = Math.max(0, ...series.map((s) => s.tokens ?? 0));
  const peakBacklog = Math.max(0, ...series.map((s) => s.work ?? 0));

  const result = {
    scenario: name,
    run,
    startedAt: started.toISOString(),
    correctness: {
      sent: final.sent,
      confirmed: final.confirmed,
      processed: upstream.processed,
      duplicates: upstream.duplicates,
      unprocessed: unprocessed.length,
      lost: lost.length,
      deadLettered: dead,
      drained,
      deadDrained,
    },
    design: DESIGN,
    calls,
    permit: { tokensAfter: permit, racesLost: DESIGN === "held" ? permitLost : null },
    redrives,
    extra,
    breaker: {
      recoveredAllClosed: recovered,
      secondsFromRestoreToAllClosed: recoveredAt - restoredAt,
      openings,
      peakReplicasOpen: peakOpen,
      peakTokensInChain: peakTokens,
      peakBacklog,
      longestHoldSeconds: maxHold,
      highestAttempt: maxAttempt,
      illegalTransitions: violations,
      consumerCountMatchedNotOpen: `${matching.length}/${comparable.length}`,
    },
    series,
  };

  const onePermit = permit.ready === 1 && permit.held === 0;
  // A host that slept mid-run stretches every timing and can trip a timeout: the run says nothing either way.
  const suspendedMs = Math.round(skewMs() - skewAtStart);
  const verdict =
    lost.length === 0 && dead === 0 && drained && deadDrained && recovered && violations.length === 0 && onePermit && extra.pass;
  console.log(
    `  sent ${final.sent}, confirmed ${final.confirmed}, processed ${upstream.processed}, duplicates ${upstream.duplicates}, ` +
      `unprocessed ${unprocessed.length}, lost ${lost.length}, dead-lettered ${dead}, drained=${drained}`,
  );
  console.log(
    `  breakers: ${openings} openings, peak ${peakOpen} open at once, peak ${peakTokens} tokens in the chain, ` +
      `longest hold ${maxHold}s (attempt ${maxAttempt}), all closed ${recoveredAt - restoredAt}s after restore`,
  );
  console.log(
    `  transitions legal: ${violations.length === 0}; broker consumer count = replicas not open in ${matching.length}/${comparable.length} samples`,
  );
  console.log(
    `  calls: ${calls.failed} reached the third party and failed, ${calls.open} refused locally, ${calls.ok} ok`,
  );
  console.log(
    `  permit: ${permit.ready} ready + ${permit.held} held after (want 1 + 0)` +
      (DESIGN === "held" ? `, ${permitLost} races lost` : "") +
      `; ${redrives} redriven` +
      (extra.summary ? `; ${extra.summary}` : ""),
  );
  const voided = suspendedMs > 2000;
  console.log(`  ${voided ? `VOID (the host was suspended for ${Math.round(suspendedMs / 1000)}s)` : verdict ? "PASS" : "FAIL"}`);
  await exec("docker", ["start", `${PROJECT}-rmq-producer-1`]).catch(() => {});
  return { ...result, suspendedMs, void: voided, pass: verdict };
};

// ---- the faults --------------------------------------------------------------

const outage = (body) => ({
  inject: () => setFailure(body),
  restore: () => setFailure({}),
});

/** Kill a replica whose breaker is open — a token is in flight for a process that will not be there to take it. */
const killOpenReplica = {
  ...outage({ rate: 1 }),
  during: async () => {
    const found = await waitFor(async () => Boolean(await openContainer()), 30);
    const victim = found ? await openContainer() : undefined;
    console.log(`  killing ${victim ?? "(no replica was open)"} while its breaker is open`);
    await (victim ? exec("docker", ["kill", victim]) : Promise.resolve());
    await sleep(3000);
    await (victim ? exec("docker", ["start", victim]) : Promise.resolve());
  },
};

/**
 * Restart the broker while several breakers are open. For the held breaker the tokens are messages in quorum
 * queues and must come back; for cockatiel the open state is in the processes and must survive losing the broker.
 */
const killBrokerWhileOpen = {
  ...outage({ rate: 1 }),
  during: async () => {
    await waitFor(async () => ((await breakerStates()) ?? []).filter((s) => s !== 0).length >= 3, 30);
    console.log(`  restarting the broker with ${((await breakerStates()) ?? []).filter((s) => s !== 0).length} breakers open`);
    await exec("docker", ["restart", `${PROJECT}-rabbitmq-1`]);
    await waitFor(async () => (await tryQueueInfo(WORK)) !== undefined, 90);
  },
};

/** Ready tokens right now, over AMQP: the management API's figures lag by seconds. */
const permitReady = async () => (await tryQueueInfo(permitQueue()))?.ready;

/**
 * Restart replicas while a probe holds the permit, three times: a hanging third party keeps each probe open for
 * its timeout. A victim that was the holder must not take the token with it; a victim that wasn't seeds a token
 * on startup while the holder still has it, which the return must collapse. Either way one token is left.
 */
const restartDuringProbe = {
  ...outage({ rate: 1, mode: "hang" }),
  during: async () => {
    for (let i = 0; i < 3; i++) {
      const held = await waitFor(async () => (await permitReady()) === 0, 20);
      const all = await consumerContainers();
      const victim = held ? all[Math.floor(Math.random() * all.length)] : undefined;
      console.log(`  ${held ? `the permit is held; restarting ${victim}` : "never saw the permit held"}`);
      await (victim ? exec("docker", ["kill", victim]).then(() => exec("docker", ["start", victim])) : Promise.resolve());
      await sleep(4000);
    }
  },
};

/**
 * Kill the replica holding the probe permit, mid-probe: a hanging third party keeps the call open until its
 * timeout. The token must come back when the holder's channel dies, and the replica's restart seeds another
 * while someone else may hold it — so the duplicate must collapse too.
 */
const killPermitHolder = {
  ...outage({ rate: 1, mode: "hang" }),
  during: async () => {
    const found = await waitFor(async () => (await permitTokens()).held === 1, 30);
    const victim = found ? await containerIn("half-open") : undefined;
    console.log(`  killing ${victim ?? "(no replica held the permit)"} while it holds the probe permit`);
    await (victim ? exec("docker", ["kill", victim]) : Promise.resolve());
    await sleep(3000);
    await (victim ? exec("docker", ["start", victim]) : Promise.resolve());
  },
};

/**
 * Dead-lettered work comes back, and the election survives losing the redriver: messages of a second run go
 * straight into the dead-letter queue, and the replica RabbitMQ made active is killed once it has moved some.
 * No outage: the healthy fleet is what the redrive waits for.
 */
const REDRIVE_BATCH = 600;
const redriveFailover = {
  inject: async () => {},
  restore: async () => {},
  during: async (ctx) => {
    const conn = await amqp.connect(BROKER);
    const ch = await conn.createConfirmChannel();
    const run = `${ctx.run}d`;
    for (let n = 0; n < REDRIVE_BATCH; n++) {
      ch.sendToQueue(DEAD, Buffer.from(JSON.stringify({ apiId: API, n })), {
        persistent: true,
        messageId: `${run}:${n}`,
        contentType: "application/json",
        type: "egress.work",
      });
    }
    await ch.waitForConfirms();
    await conn.close();
    const deadAtStart = (await queueInfo(DEAD)).ready;
    const moving = await waitFor(async () => (await queueInfo(DEAD)).ready < deadAtStart, 45);
    const victim = moving ? await activeRedriver() : undefined;
    console.log(`  ${REDRIVE_BATCH} messages dead-lettered; killing the elected redriver ${victim ?? "(none moved any)"}`);
    await (victim ? exec("docker", ["kill", victim]) : Promise.resolve());
    await sleep(3000);
    await (victim ? exec("docker", ["start", victim]) : Promise.resolve());
    ctx.redriveRun = run;
  },
  check: async (ctx) => {
    const a = await audit(ctx.redriveRun);
    const bits = decodeBits(a.bits);
    const missing = Array.from({ length: REDRIVE_BATCH }, (_, n) => n).filter((n) => !isSet(bits, n));
    return {
      pass: missing.length === 0,
      summary: `redriven batch: ${REDRIVE_BATCH - missing.length}/${REDRIVE_BATCH} processed, ${a.duplicates} duplicates`,
      processed: REDRIVE_BATCH - missing.length,
      duplicates: a.duplicates,
    };
  },
};

/**
 * A third party that is full, not broken: 20 at once at 100ms (200/s) under the 1,000/s spike, answering 429 to
 * the rest. It is backpressure, so no breaker may open and nothing may be dead-lettered, and the backlog must still
 * drain once the ceiling lifts.
 */
const overload = {
  inject: () => setFailure({ capacity: 20, delayMs: 100 }),
  restore: () => setFailure({}),
  check: async (ctx) => {
    const window = `${Math.ceil((Date.now() - ctx.t0) / 1000)}s`;
    const trips = Math.round(await promSum(`sum(increase(egress_consumer_breaker_trips_total[${window}]))`));
    const throttled = Math.round(await promSum(`sum(increase(egress_consumer_calls_total{outcome="throttled"}[${window}]))`));
    return { pass: trips === 0, summary: `${throttled} answered 429, ${trips} breaker trips (want 0)`, trips, throttled };
  },
};

const SCENARIOS = {
  outage: () => scenario("outage", outage({ rate: 1 })),
  "outage-hang": () => scenario("outage-hang", outage({ rate: 1, mode: "hang" })),
  "kill-open-replica": () => scenario("kill-open-replica", killOpenReplica),
  "kill-broker-while-open": () => scenario("kill-broker-while-open", killBrokerWhileOpen),
  "restart-during-probe": () => scenario("restart-during-probe", restartDuringProbe),
  // Held only: it finds the holder by its logged half-open phase, which cockatiel doesn't log.
  "kill-permit-holder": () => scenario("kill-permit-holder", killPermitHolder),
  "redrive-failover": () => scenario("redrive-failover", redriveFailover),
  overload: () => scenario("overload", overload),
  // Informational: no correctness claim is made for it. See the article.
  partial: () => scenario("partial", outage({ rate: 0.6 })),
};

/**
 * A long hold across a broker restart, without waiting a day: a message delayed
 * 100s (binary 1100100 — it waits in levels 6, 5 and 2) is sent, the broker is
 * restarted 10s in, and it must arrive once, near 100s after it was sent.
 */
const delaySurvivesRestart = async () => {
  const SECONDS = 100;
  const queue = "chaos.delay.probe";
  console.log(`\n== delay-survives-broker-restart (${SECONDS}s, bits ${delayBits(SECONDS).join("")}, enters level ${entryLevel(SECONDS)}) ==`);
  const conn = await amqp.connect(BROKER);
  const ch = await conn.createConfirmChannel();
  await ch.deleteQueue(queue);
  await ch.assertQueue(queue, { durable: true });
  await ch.bindQueue(queue, DELIVERY_EXCHANGE, bindingKey(queue));
  const sentAt = Date.now();
  await new Promise((resolve, reject) =>
    ch.publish(levelName(entryLevel(SECONDS)), routingKey(SECONDS, queue), Buffer.from("probe"), { persistent: true }, (e) =>
      e ? reject(e) : resolve(),
    ),
  );
  await conn.close();
  await sleep(10_000);
  console.log(`  t+${Math.round((Date.now() - sentAt) / 1000)}s  restarting the broker, token in level ${entryLevel(SECONDS)}`);
  await exec("docker", ["restart", `${PROJECT}-rabbitmq-1`]);

  const arrivals = [];
  const deadline = sentAt + (SECONDS + 30) * 1000;
  while (Date.now() < deadline && arrivals.length === 0) {
    await sleep(1000);
    const c = await amqp.connect(BROKER).catch(() => undefined);
    const ch2 = await c?.createChannel().catch(() => undefined);
    const got = await ch2?.get(queue, { noAck: true }).catch(() => false);
    if (got) arrivals.push(Date.now() - sentAt);
    await c?.close().catch(() => {});
  }
  await sleep(3000);
  const c = await amqp.connect(BROKER);
  const ch3 = await c.createChannel();
  const extra = (await ch3.checkQueue(queue)).messageCount;
  await ch3.deleteQueue(queue);
  await c.close();
  const at = arrivals[0];
  const pass = arrivals.length === 1 && extra === 0 && at >= SECONDS * 1000 - 1500 && at <= SECONDS * 1000 + 4000;
  console.log(`  arrived after ${at === undefined ? "never" : `${(at / 1000).toFixed(1)}s`}, ${extra} extra copies`);
  console.log(`  ${pass ? "PASS" : "FAIL"}`);
  return { scenario: "delay-survives-broker-restart", requestedSeconds: SECONDS, arrivedMs: at ?? null, extraCopies: extra, pass };
};
SCENARIOS["delay-survives-broker-restart"] = delaySurvivesRestart;

/** Scenarios that only exercise the held breaker's own machinery. */
const HELD_ONLY = new Set(["kill-permit-holder", "delay-survives-broker-restart"]);

const main = async () => {
  if (flag("list", false)) return void console.log(Object.keys(SCENARIOS).join("\n"));
  DESIGN = await detectDesign();
  console.log(`design: ${DESIGN}`);
  const chosen = String(flag("scenarios", Object.keys(SCENARIOS).join(",")))
    .split(",")
    .filter((name) => DESIGN === "held" || !HELD_ONLY.has(name));
  const results = [];
  try {
    for (const name of chosen) {
      results.push(await SCENARIOS[name]());
      // Between scenarios: everything healthy, so one cannot leave residue for the next.
      await setFailure({});
      await waitFor(async () => (await consumerContainers()).length >= 5, 60);
      await Promise.all((await consumerContainers()).map((c) => exec("docker", ["start", c]).catch(() => {})));
      await waitFor(allClosed, 120);
    }
  } finally {
    await setFailure({});
    await exec("docker", ["start", `${PROJECT}-rmq-producer-1`]).catch(() => {});
    mkdirSync(dirname(OUT), { recursive: true });
    writeFileSync(OUT, JSON.stringify({ at: new Date().toISOString(), results }, null, 2));
    console.log(`\nwrote ${OUT}`);
  }
  const graded = results.filter((r) => r.scenario !== "partial" && !r.void);
  console.log(`\n${graded.filter((r) => r.pass).length}/${graded.length} graded scenarios passed`);
  process.exitCode = graded.every((r) => r.pass) ? 0 : 1;
};

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
