import { Config, Duration, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";
import { NodeRuntime, NodeServices } from "@effect/platform-node";

/**
 * Drives the "Demo script" from the README end to end over HTTP, narrating
 * every published transition as it happens — so a live demo is one command in
 * a second terminal, and the console/Grafana is what the audience watches.
 *
 *   pnpm run demo                          # sim fleet: payments-provider, localhost:8088
 *   pnpm run demo -- shipping-rates        # a different API
 *   pnpm run demo:envoy                    # same script, against `docker compose up`
 *
 * It reads /api/events — the same feed subscriber.ts consumes over SSE — so
 * what it prints is the published contract, not an assumption about timing.
 * Nothing here is simulated or mocked: this is either the sim fleet's own
 * outlier detection, or (FAILURE_MODE=envoy) three real Envoy replicas, and
 * the aggregator's own quorum/dwell logic, all running on the wall clock.
 *
 * The two modes differ only in how failure gets injected, because the sim
 * fleet is driven by the console's /api/failure route while real Envoy has no
 * such hook — it only reacts to the upstream flaky-upstream.mjs actually
 * returns, so FAILURE_MODE=envoy posts straight to its /__fail port instead.
 * Everything downstream of that (waiting on /api/events, the delivery-contract
 * check) is identical either way.
 *
 * The last step covers the other half of the system. When PROMETHEUS points
 * at the monitoring stack, the driver also asserts what the RabbitMQ daemon
 * fleet did — stopped pulling on OPEN, let the queue build, ramped back, and
 * kept the same per-API sequence contract on the AMQP transport. Unset, or
 * pointed at an API no fleet is running for, that step is skipped rather
 * than failed.
 *
 * Flags or the environment, whichever suits — `--failure-mode=envoy` and
 * `FAILURE_MODE=envoy` are the same instruction, so the `demo:*` scripts still
 * read as they did. `--help` is the list; this comment used to be it.
 */

type Settings = {
  readonly api: string;
  readonly aggregator: string;
  readonly failureMode: "sim" | "envoy";
  readonly flakyUpstream: string;
  readonly prometheus: string;
};

/**
 * The whole script, as a function of what it was asked to do.
 *
 * Everything below used to be module scope reading `process.env` directly,
 * which is why `ORIGIN` and `peakWork` were mutable state belonging to no one.
 * They are locals now; the identifiers are unchanged, so the body reads exactly
 * as it did.
 */
const run = (settings: Settings) => {
  /**
   * One or more comma-separated aggregator instances. Only the leader publishes,
   * so only the leader's /api/events has anything on it — pointing this at a
   * standby means every wait below times out on an empty feed. With two real
   * instances competing for one lease, which of them that is at any moment is
   * not knowable in advance, so the list is resolved against the live
   * `leader.isLeader` flag at startup rather than guessed in configuration.
   */
  const CANDIDATES = settings.aggregator
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  let ORIGIN = CANDIDATES[0] ?? "http://127.0.0.1:8088";
  const API = settings.api;
  const FAILURE_MODE = settings.failureMode;
  const FLAKY_UPSTREAM = settings.flakyUpstream;
  const PROMETHEUS = settings.prometheus;

  // Real Envoy's outlier detection needs actual requests flowing (see
  // traffic-generator.mjs, part of `docker compose up`) before failure injected
  // here shows up as anything — the sim fleet has no such lag, so give the
  // envoy path more room on every wait below.
  const TIMEOUT_SCALE = FAILURE_MODE === "envoy" ? 2 : 1;

  /**
   * flaky-upstream.mjs's ports, several per API — one per endpoint in the
   * matching Envoy cluster. Failing *all* of an API's ports is what this script
   * does, because the six steps are about a whole third party degrading;
   * failing a subset by hand is what produces partial ejection and therefore
   * DEGRADED, which is the interesting thing to try afterwards.
   */
  const UPSTREAM_PORTS: Readonly<Record<string, ReadonlyArray<number>>> = {
    "payments-provider": [8080, 8081, 8082, 8083, 8084, 8085],
    "shipping-rates": [8090, 8091, 8092, 8093],
    "tax-calc": [8094, 8095, 8096],
  };

  type CircuitEventData = {
    readonly apiId: string;
    readonly sequence: number;
    readonly previousState: string | null;
    readonly state: string;
    readonly reason: string;
  };

  const getEvents = Effect.tryPromise({
    try: () =>
      fetch(`${ORIGIN}/api/events`).then(
        (r) => r.json() as Promise<{ events: ReadonlyArray<{ type: string; data: CircuitEventData }> }>,
      ),
    catch: (cause) => new Error(`GET /api/events failed: ${String(cause)}`),
  });

  const getSubscriber = Effect.tryPromise({
    try: () =>
      fetch(`${ORIGIN}/api/subscriber`).then(
        (r) => r.json() as Promise<{ received: number; snapshots: number; duplicates: number; gaps: ReadonlyArray<string> }>,
      ),
    catch: (cause) => new Error(`GET /api/subscriber failed: ${String(cause)}`),
  });

  const setFailureRate = (apiId: string, rate: number) => {
    if (FAILURE_MODE === "sim") {
      return Effect.tryPromise({
        try: () =>
          fetch(`${ORIGIN}/api/failure`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ apiId, rate }),
          }),
        catch: (cause) => new Error(`POST /api/failure failed: ${String(cause)}`),
      });
    }
    const ports = UPSTREAM_PORTS[apiId];
    if (ports === undefined) {
      return Effect.fail(
        new Error(
          `no flaky-upstream ports known for "${apiId}" — expected one of ${Object.keys(UPSTREAM_PORTS).join(", ")}`,
        ),
      );
    }
    return Effect.forEach(
      ports,
      (port) => {
        const url = `${FLAKY_UPSTREAM}:${port}/__fail`;
        return Effect.tryPromise({
          try: () =>
            fetch(url, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ rate }),
            }),
          catch: (cause) => new Error(`POST ${url} failed: ${String(cause)}`),
        });
      },
      { discard: true },
    );
  };

  /**
   * One instant-vector query, summed. `null` means the series does not exist —
   * which is how the fleet step tells "no daemons are running for this API"
   * apart from "the daemons are running and the value is zero".
   */
  const promQuery = (query: string) =>
    Effect.tryPromise({
      try: () =>
        fetch(`${PROMETHEUS}/api/v1/query?query=${encodeURIComponent(query)}`).then(
          (r) =>
            r.json() as Promise<{ data?: { result?: ReadonlyArray<{ value: [number, string] }> } }>,
        ),
      catch: (cause) => new Error(`prometheus query ${query} failed: ${String(cause)}`),
    }).pipe(
      Effect.map((body) => {
        const rows = body.data?.result ?? [];
        return rows.length === 0
          ? null
          : rows.reduce((sum, row) => sum + Number(row.value[1]), 0);
      }),
    );

  type Fleet = {
    readonly target: number | null;
    readonly active: number | null;
    readonly size: number | null;
    readonly work: number | null;
    readonly dead: number | null;
    readonly broken: number | null;
  };

  const fleetSnapshot: Effect.Effect<Fleet, Error> = Effect.all({
    target: promQuery(`max(egress_daemon_target_active{apiId="${API}"})`),
    active: promQuery(`sum(egress_daemon_self_active{apiId="${API}"})`),
    size: promQuery(`max(egress_daemon_fleet_size{apiId="${API}"})`),
    work: promQuery(`rabbitmq_detailed_queue_messages{queue="${API}.work"}`),
    dead: promQuery(`rabbitmq_detailed_queue_messages{queue="${API}.work.dead"}`),
    broken: promQuery(
      `sum(egress_daemon_control_gaps_total{apiId="${API}"}) + ` +
        `sum(egress_daemon_control_duplicates_total{apiId="${API}"})`,
    ),
  });

  /**
   * The deepest the work queue was seen to get. Sampled by a background fiber
   * rather than read once at the end of the outage: the queue is at its minimum
   * the instant the fleet stops (that is when the drain finished and the build
   * has not started) and at its maximum somewhere in the middle, so a single
   * reading taken at either edge understates it by an order of magnitude.
   */
  let peakWork = 0;

  const sampleFleet = Effect.forever(
    fleetSnapshot.pipe(
      Effect.map((f) => {
        peakWork = Math.max(peakWork, f.work ?? 0);
      }),
      Effect.catchCause(() => Effect.void),
      Effect.andThen(Effect.sleep(Duration.seconds(1))),
    ),
  );

  const describeFleet = (f: Fleet) =>
    `target=${f.target}/${f.size} pulling=${f.active} work=${f.work} dead-lettered=${f.dead}`;

  /**
   * Polls Prometheus until the fleet satisfies `predicate`. Scrape interval is
   * 2s and the daemons publish once a second, so anything asserted here is
   * necessarily a few seconds behind the event that caused it — which is why
   * this waits for a condition rather than reading once after a sleep.
   */
  const awaitFleet = (predicate: (f: Fleet) => boolean, what: string, timeoutMs: number) =>
    Effect.gen(function* () {
      const deadline = Date.now() + timeoutMs;
      let last: Fleet | null = null;
      while (true) {
        // A transient scrape failure is not a demo failure. `sampleFleet`
        // already ignores them; without the same treatment here a single blip
        // ends the run with a stack trace instead of a verdict.
        const now = yield* fleetSnapshot.pipe(
          Effect.map((f): Fleet | null => f),
          Effect.catchCause(() => Effect.succeed(null)),
        );
        if (now !== null) {
          last = now;
          if (predicate(now)) return now;
        }
        if (Date.now() > deadline) {
          return yield* Effect.die(
            `timed out after ${timeoutMs}ms waiting for ${what} — ` +
              `last saw ${last === null ? "no readable fleet metrics at all" : describeFleet(last)}`,
          );
        }
        yield* Effect.sleep(Duration.millis(500));
      }
    });

  /** Picks whichever candidate currently holds the publishing lease. */
  const resolveLeader = Effect.gen(function* () {
    if (CANDIDATES.length === 1) return;
    for (const candidate of CANDIDATES) {
      const leading = yield* Effect.tryPromise({
        try: () =>
          fetch(`${candidate}/api/state`).then(
            (r) => r.json() as Promise<{ leader?: { isLeader: boolean; instanceId: string } }>,
          ),
        catch: (cause) => new Error(String(cause)),
      }).pipe(
        Effect.map((body) => body.leader?.isLeader === true),
        Effect.catchCause(() => Effect.succeed(false)),
      );
      if (leading) {
        ORIGIN = candidate;
        return;
      }
    }
    console.log(
      `  (none of ${CANDIDATES.join(", ")} reports itself leader — using ${ORIGIN} and hoping)`,
    );
  });

  const header = (msg: string) => Effect.sync(() => console.log(`\n\x1b[1m== ${msg} ==\x1b[0m`));

  const narrate = (d: CircuitEventData) =>
    Effect.sync(() =>
      console.log(
        `  seq=${String(d.sequence).padEnd(3)} ${(d.previousState ?? "").padEnd(10)} -> ${d.state.padEnd(10)} ${d.reason}`,
      ),
    );

  /**
   * Polls /api/events (newest first) until a state_changed for `apiId` with
   * sequence > `after` appears, narrates it, and returns its sequence. A probe
   * cycle can pass through more than one transition before landing on the one
   * the caller cares about, so this reports every one it sees on the way.
   */
  const awaitTransition = (apiId: string, after: number, baseTimeoutMs: number) =>
    Effect.gen(function* () {
      const timeoutMs = baseTimeoutMs * TIMEOUT_SCALE;
      const deadline = Date.now() + timeoutMs;
      while (true) {
        if (Date.now() > deadline) {
          return yield* Effect.die(
            `timed out after ${timeoutMs}ms waiting for ${apiId} to publish past seq=${after}`,
          );
        }
        const { events } = yield* getEvents;
        const next = events
          .filter(
            (e): e is { type: "egress.circuit.state_changed"; data: CircuitEventData } =>
              e.type === "egress.circuit.state_changed" &&
              e.data.apiId === apiId &&
              e.data.sequence > after,
          )
          .sort((a, b) => a.data.sequence - b.data.sequence);
        if (next.length > 0) {
          for (const e of next) yield* narrate(e.data);
          return next[next.length - 1]!.data.sequence;
        }
        yield* Effect.sleep(Duration.millis(300));
      }
    });

  const program = Effect.gen(function* () {
    yield* resolveLeader;
    const ports = UPSTREAM_PORTS[API];
    const injectVia =
      FAILURE_MODE === "envoy"
        ? `${FLAKY_UPSTREAM}:{${ports?.join(",") ?? "?"}}/__fail`
        : `${ORIGIN}/api/failure`;
    yield* Effect.log(`driving ${API} against ${ORIGIN} (failure via ${injectVia})`);

    // Fails fast with a one-line fix instead of the six-step choreography dying
    // on step one with a fetch stack trace — the aggregator not being up yet is
    // the overwhelmingly likely reason this doesn't connect.
    yield* getEvents.pipe(
      Effect.catchCause(() =>
        Effect.die(
          `cannot reach the aggregator at ${ORIGIN} — start it first, in another terminal: pnpm start.\n` +
            `  If the stack IS up and you are running this from a devcontainer, published ports on\n` +
            `  localhost may not be reachable from here (see the README's note on the compose\n` +
            `  network): use service names instead —\n` +
            `    AGGREGATOR=http://aggregator:8088 FLAKY_UPSTREAM=http://flaky-upstream \\\n` +
            `    PROMETHEUS=http://prometheus:9090 FAILURE_MODE=envoy pnpm run demo`,
        ),
      ),
    );

    // The fleet step is opt-in twice over: PROMETHEUS has to be set, and there
    // has to actually be a daemon fleet running for *this* API. A missing
    // series is the signal for the second — distinct from a series reading 0.
    const fleetPresent =
      PROMETHEUS !== "" &&
      (yield* fleetSnapshot.pipe(
        Effect.map((f) => f.target !== null),
        Effect.catchCause(() => Effect.succeed(false)),
      ));
    if (PROMETHEUS !== "" && !fleetPresent) {
      console.log(`  (no daemon fleet metrics for ${API} at ${PROMETHEUS} — fleet step will be skipped)`);
    }
    // Interrupted with the program, so there is nothing to tear down by hand.
    if (fleetPresent) yield* Effect.forkChild(sampleFleet);

    yield* header("Steady state");
    yield* setFailureRate(API, 0);
    const s0 = yield* getEvents;
    const start =
      s0.events.find((e) => e.type === "egress.circuit.state_changed" && e.data.apiId === API)
        ?.data.sequence ?? 0;
    console.log(`  waiting a moment so the console reads as calm before the incident starts...`);
    yield* Effect.sleep(Duration.seconds(2));

    yield* header(`Drag ${API} to 45%`);
    yield* setFailureRate(API, 0.45);
    const s1 = yield* awaitTransition(API, start, 15_000);

    yield* header(`Drag ${API} to 100%`);
    yield* setFailureRate(API, 1.0);
    const s2 = yield* awaitTransition(API, s1, 15_000);

    if (fleetPresent) {
      yield* header("The daemon fleet reacts — no coordination, same events");
      const stopped = yield* awaitFleet(
        (f) => f.active === 0,
        "every daemon to stop pulling work",
        45_000,
      );
      console.log(`  ${describeFleet(stopped)}`);
      console.log(`  nothing is calling the dead upstream; the backlog is the point.`);
    }

    yield* header("Watch it probe — upstream is still dead, so this reopens with doubled backoff");
    const s3 = yield* awaitTransition(API, s2, 15_000); // HALF_OPEN
    const s4 = yield* awaitTransition(API, s3, 20_000); // OPEN again, PROBE_FAILED

    yield* header("Hit Restore");
    yield* setFailureRate(API, 0);
    const s5 = yield* awaitTransition(API, s4, 30_000); // HALF_OPEN, after the doubled backoff
    yield* awaitTransition(API, s5, 15_000); // CLOSED, after probeSuccesses healthy checks

    yield* header("Delivery contract, read from outside the process");
    const sub = yield* getSubscriber;
    console.log(
      `  received=${sub.received} snapshots=${sub.snapshots} duplicates=${sub.duplicates} gaps=${sub.gaps.length}`,
    );
    if (sub.duplicates > 0 || sub.gaps.length > 0) {
      console.log(`  gaps: ${sub.gaps.join(", ") || "(none)"}`);
      return yield* Effect.die("delivery contract broken — see gaps/duplicates above");
    }
    console.log(`  gapless and non-repeating through the full incident.`);

    if (fleetPresent) {
      yield* header("Fleet: ramp back, drain, and the same contract on AMQP");
      const peak = peakWork;
      const back = yield* awaitFleet(
        (f) => f.active !== null && f.active === f.size && (f.work ?? 0) <= Math.max(50, peak * 0.25),
        `the fleet to ramp back to full strength and drain the ${peak}-message backlog`,
        120_000,
      );
      console.log(`  ${describeFleet(back)} (deepest backlog seen: ${peak})`);
      if ((back.broken ?? 0) > 0) {
        return yield* Effect.die(
          "the sequence contract broke on circuit.control — gaps or duplicates seen by the daemons",
        );
      }
      console.log(
        `  the same per-API sequence guarantee held on the AMQP transport too, ` +
          `checked by ${back.size} consumers the publisher does not control.`,
      );
    }
  });

  return program;
};

const demo = Command.make(
  "demo",
  {
    api: Argument.string("api").pipe(
      Argument.withDefault("payments-provider"),
      Argument.withDescription("Which API to drive the script against"),
    ),
    aggregator: Flag.string("aggregator").pipe(
      Flag.withFallbackConfig(Config.nonEmptyString("AGGREGATOR")),
      Flag.withDefault("http://127.0.0.1:8088"),
      Flag.withDescription("Comma-separated instances; the leader is resolved among them"),
    ),
    failureMode: Flag.choice("failure-mode", ["sim", "envoy"] as const).pipe(
      Flag.withFallbackConfig(Config.literals(["sim", "envoy"], "FAILURE_MODE")),
      Flag.withDefault("sim" as const),
      Flag.withDescription("Inject failure into the simulator or into a real upstream"),
    ),
    flakyUpstream: Flag.string("flaky-upstream").pipe(
      Flag.withFallbackConfig(Config.nonEmptyString("FLAKY_UPSTREAM")),
      Flag.withDefault("http://127.0.0.1"),
      Flag.withDescription("Base URL of the upstream to fail (--failure-mode=envoy)"),
    ),
    prometheus: Flag.string("prometheus").pipe(
      Flag.withFallbackConfig(Config.nonEmptyString("PROMETHEUS")),
      Flag.withDefault(""),
      Flag.withDescription("Prometheus base URL; enables the fleet step"),
    ),
  },
  (settings) => run(settings),
);

Command.run(demo, { version: "0.1.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
