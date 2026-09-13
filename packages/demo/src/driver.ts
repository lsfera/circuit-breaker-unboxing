import { Config, Console, Duration, Effect, Option as O, Schedule, Schema } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { VERSION } from "@egress/config/Settings.ts";
import { CircuitEvent, SEQUENCED_EVENT, State } from "@egress/domain/Model.ts";

/** Derived from the published schema rather than restated — see getEvents. */
type CircuitEventData = CircuitEvent["data"];

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
 * Flags or the environment, whichever suits: `--failure-mode=envoy` and
 * `FAILURE_MODE=envoy` are the same instruction. `--help` is the full list.
 */

const decodeEvent = Schema.decodeUnknownOption(CircuitEvent);

/** What `/api/subscriber` answers with — the delivery-contract reading. */
const SubscriberReport = Schema.Struct({
  received: Schema.Natural,
  snapshots: Schema.Natural,
  duplicates: Schema.Natural,
  gaps: Schema.Array(Schema.String),
});

const decodeSubscriber = Schema.decodeUnknownEffect(SubscriberReport);

type Settings = {
  readonly api: string;
  readonly aggregator: string;
  readonly failureMode: "sim" | "envoy";
  readonly flakyUpstream: string;
  readonly prometheus: string;
};

/** The whole script, as a function of what it was asked to do. */
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

  /**
   * Decoded against the published schema, not cast to a shape declared here.
   * This script's claim is that what it prints *is* the contract; reading it
   * through a hand-written copy of the payload would make a contract change
   * show up as a timeout or a printed `undefined` rather than as a failure
   * naming the field. Anything that does not decode is dropped and counted, so
   * a partial answer is visible instead of silently short.
   */
  const getEvents = Effect.tryPromise({
    try: () => fetch(`${ORIGIN}/api/events`).then((r) => r.json() as Promise<unknown>),
    catch: (cause) => new Error(`GET /api/events failed: ${String(cause)}`),
  }).pipe(
    Effect.map((body) => {
      const raw = (body as { events?: ReadonlyArray<unknown> }).events ?? [];
      const events = raw.flatMap((e) => O.toArray(decodeEvent(e)));
      return { events, undecodable: raw.length - events.length };
    }),
    Effect.tap(({ undecodable }) =>
      undecodable > 0
        ? Effect.logWarning(
            `${undecodable} event(s) on /api/events did not match the published schema`,
          )
        : Effect.void,
    ),
  );

  /**
   * Decoded, not cast, for the same reason `/subscriber/webhook` is: these four
   * numbers are the verdict this script prints, and the check below is
   * `duplicates > 0`. A field that went missing would read as `undefined`, and
   * `undefined > 0` is false — so a drifted contract would pass the contract
   * check silently. It fails naming the field instead.
   */
  const getSubscriber = Effect.tryPromise({
    try: () => fetch(`${ORIGIN}/api/subscriber`).then((r) => r.json() as Promise<unknown>),
    catch: (cause) => new Error(`GET /api/subscriber failed: ${String(cause)}`),
  }).pipe(Effect.flatMap(decodeSubscriber));

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
    target: promQuery(`max(egress_daemon_target_fraction{apiId="${API}"})`),
    active: promQuery(`sum(egress_daemon_self_active{apiId="${API}"})`),
    // Counted rather than configured: the fleet is one scaled service and no
    // daemon knows how many there are, so how many are reporting *is* the size.
    size: promQuery(`count(egress_daemon_self_active{apiId="${API}"})`),
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

  const sampleFleet = Effect.repeat(
    fleetSnapshot.pipe(
      Effect.map((f) => {
        peakWork = Math.max(peakWork, f.work ?? 0);
      }),
      // A scrape blip is not a demo failure; the next sample covers it.
      Effect.catch(() => Effect.void),
    ),
    Schedule.spaced(Duration.seconds(1)),
  );

  const describeFleet = (f: Fleet) =>
    // Intended and actual, because the fraction only approximately lands — a
    // gap between the two is the cost of selecting by hash rather than index,
    // and it is on the line rather than hidden. See ADR 013.
    `target=${f.target === null ? "?" : Math.round(f.target * 100)}%` +
    `${f.target !== null && f.size !== null ? ` (~${Math.round(f.target * f.size)} of ${f.size})` : ""}` +
    ` pulling=${f.active} work=${f.work} dead-lettered=${f.dead}`;

  /**
   * Polls `probe` every `everyMs` until it yields something, or dies saying
   * what it was waiting for and what it last saw. Both waits below are this
   * shape: a condition the system reaches on its own clock, not a sleep long
   * enough to hope it has.
   */
  const awaitOn = <A, E>(
    probe: Effect.Effect<O.Option<A>, E>,
    everyMs: number,
    timeoutMs: number,
    describe: () => string,
  ): Effect.Effect<A> =>
    probe.pipe(
      // "Not yet" is a failure so that one `retry` covers it and a transient
      // fetch error alike — a blip against a live stack is not a verdict.
      Effect.flatMap(
        O.match({
          onNone: () => Effect.fail("pending" as const),
          onSome: (found: A) => Effect.succeed(found),
        }),
      ),
      Effect.retry(Schedule.spaced(Duration.millis(everyMs))),
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () => Effect.die(`timed out after ${timeoutMs}ms ${describe()}`),
      }),
      Effect.orDie,
    );

  /**
   * Polls Prometheus until the fleet satisfies `predicate`. Scrape interval is
   * 2s and the daemons publish once a second, so anything asserted here is
   * necessarily a few seconds behind the event that caused it — which is why
   * this waits for a condition rather than reading once after a sleep.
   */
  const awaitFleet = (predicate: (f: Fleet) => boolean, what: string, timeoutMs: number) => {
    let last: Fleet | null = null;
    return awaitOn(
      // A transient scrape failure is not a demo failure. `sampleFleet`
      // already ignores them; without the same treatment here a single blip
      // ends the run with a stack trace instead of a verdict.
      fleetSnapshot.pipe(
        Effect.map((f) => {
          last = f;
          return predicate(f) ? O.some(f) : O.none();
        }),
        Effect.catch(() => Effect.succeed(O.none<Fleet>())),
      ),
      500,
      timeoutMs,
      () =>
        `waiting for ${what} — ` +
        `last saw ${last === null ? "no readable fleet metrics at all" : describeFleet(last)}`,
    );
  };

  /** Picks whichever candidate currently holds the publishing lease. */
  const claimsLeadership = (candidate: string) =>
    Effect.tryPromise({
      try: () =>
        fetch(`${candidate}/api/state`).then(
          (r) => r.json() as Promise<{ leader?: { isLeader: boolean; instanceId: string } }>,
        ),
      catch: (cause) => new Error(String(cause)),
    }).pipe(
      Effect.map((body) => body.leader?.isLeader === true),
      // An instance that cannot be reached is not the leader as far as this
      // script is concerned; the next candidate gets asked.
      Effect.catch(() => Effect.succeed(false)),
    );

  const resolveLeader = Effect.when(
    Effect.findFirst(CANDIDATES, claimsLeadership).pipe(
      Effect.flatMap(
        O.match({
          onSome: (candidate) =>
            Effect.sync(() => {
              ORIGIN = candidate;
            }),
          onNone: () =>
            Console.log(
              `  (none of ${CANDIDATES.join(", ")} reports itself leader — using ${ORIGIN} and hoping)`,
            ),
        }),
      ),
    ),
    Effect.succeed(CANDIDATES.length > 1),
  ).pipe(Effect.asVoid);

  const header = (msg: string) => Console.log(`\n\x1b[1m== ${msg} ==\x1b[0m`);

  const narrate = (d: CircuitEventData) =>
    Console.log(
      `  seq=${String(d.sequence).padEnd(3)} ${(d.previousState ?? "").padEnd(10)} -> ${d.state.padEnd(10)} ${d.reason}`,
    );

  /**
   * Polls /api/events (newest first) until a state_changed for `apiId` with
   * sequence > `after` that reaches `expected` appears, narrates every
   * transition up to and including it, and returns its sequence.
   *
   * `expected` is a parameter and not a trailing comment because it is the
   * assertion. This used to return the newest transition it could see, so a
   * poll that caught two — HALF_OPEN and the OPEN right behind it land well
   * inside one 300ms window — consumed both, every later step shifted by one,
   * and the run finished a transition early. The last call in the script is
   * the only check that the circuit recovered, and in that state it returned
   * on a transition to HALF_OPEN and reported success with the circuit open.
   *
   * Transitions past the expected one are left un-narrated on purpose: the
   * next step filters on `sequence > after` and reports them itself.
   */
  const awaitTransition = (
    apiId: string,
    after: number,
    baseTimeoutMs: number,
    expected: State,
  ) =>
    awaitOn(
      getEvents.pipe(
        Effect.flatMap(({ events }) => {
          const next = events
            .filter(
              (e): e is CircuitEvent & { type: typeof SEQUENCED_EVENT } =>
                e.type === SEQUENCED_EVENT &&
                e.data.apiId === apiId &&
                e.data.sequence > after,
            )
            .sort((a, b) => a.data.sequence - b.data.sequence);
          // `findIndex` returning -1 makes this `slice(0, 0)`, so "not there
          // yet" and "nothing new" are the same empty answer.
          const upto = next.slice(0, next.findIndex((e) => e.data.state === expected) + 1);
          const landed = upto[upto.length - 1];
          return landed === undefined
            ? Effect.succeed(O.none<number>())
            : Effect.as(
                Effect.forEach(upto, (e) => narrate(e.data)),
                O.some(landed.data.sequence),
              );
        }),
      ),
      300,
      baseTimeoutMs * TIMEOUT_SCALE,
      () => `waiting for ${apiId} to reach ${expected} past seq=${after}`,
    );

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
      Effect.catch(() =>
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
        Effect.catch(() => Effect.succeed(false)),
      ));
    if (PROMETHEUS !== "" && !fleetPresent) {
      yield* Console.log(`  (no daemon fleet metrics for ${API} at ${PROMETHEUS} — fleet step will be skipped)`);
    }
    // Interrupted with the program, so there is nothing to tear down by hand.
    if (fleetPresent) yield* Effect.forkChild(sampleFleet);

    yield* header("Steady state");
    yield* setFailureRate(API, 0);
    const s0 = yield* getEvents;
    const start =
      s0.events.find((e) => e.type === SEQUENCED_EVENT && e.data.apiId === API)
        ?.data.sequence ?? 0;
    yield* Console.log(`  waiting a moment so the console reads as calm before the incident starts...`);
    yield* Effect.sleep(Duration.seconds(2));

    yield* header(`Drag ${API} to 45%`);
    yield* setFailureRate(API, 0.45);
    const s1 = yield* awaitTransition(API, start, 15_000, State.DEGRADED);

    yield* header(`Drag ${API} to 100%`);
    yield* setFailureRate(API, 1.0);
    const s2 = yield* awaitTransition(API, s1, 15_000, State.OPEN);

    if (fleetPresent) {
      yield* header("The daemon fleet reacts — no coordination, same events");
      const stopped = yield* awaitFleet(
        (f) => f.active === 0,
        "every daemon to stop pulling work",
        45_000,
      );
      yield* Console.log(`  ${describeFleet(stopped)}`);
      yield* Console.log(`  nothing is calling the dead upstream; the backlog is the point.`);
    }

    yield* header("Watch it probe — upstream is still dead, so this reopens with doubled backoff");
    const s3 = yield* awaitTransition(API, s2, 15_000, State.HALF_OPEN);
    const s4 = yield* awaitTransition(API, s3, 20_000, State.OPEN); // PROBE_FAILED

    yield* header("Hit Restore");
    yield* setFailureRate(API, 0);
    const s5 = yield* awaitTransition(API, s4, 30_000, State.HALF_OPEN); // after the doubled backoff
    yield* awaitTransition(API, s5, 15_000, State.CLOSED); // after probeSuccesses healthy checks

    yield* header("Delivery contract, read from outside the process");
    const sub = yield* getSubscriber;
    yield* Console.log(
      `  received=${sub.received} snapshots=${sub.snapshots} duplicates=${sub.duplicates} gaps=${sub.gaps.length}`,
    );
    if (sub.duplicates > 0 || sub.gaps.length > 0) {
      yield* Console.log(`  gaps: ${sub.gaps.join(", ") || "(none)"}`);
      return yield* Effect.die("delivery contract broken — see gaps/duplicates above");
    }
    yield* Console.log(`  gapless and non-repeating through the full incident.`);

    if (fleetPresent) {
      yield* header("Fleet: ramp back, drain, and the same contract on AMQP");
      const peak = peakWork;
      const back = yield* awaitFleet(
        (f) => f.active !== null && f.active === f.size && (f.work ?? 0) <= Math.max(50, peak * 0.25),
        `the fleet to ramp back to full strength and drain the ${peak}-message backlog`,
        120_000,
      );
      yield* Console.log(`  ${describeFleet(back)} (deepest backlog seen: ${peak})`);
      if ((back.broken ?? 0) > 0) {
        return yield* Effect.die(
          "the sequence contract broke on circuit.control — gaps or duplicates seen by the daemons",
        );
      }
      yield* Console.log(
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
    api: Argument.String("api").pipe(
      Argument.withDefault("payments-provider"),
      Argument.withDescription("Which API to drive the script against"),
    ),
    aggregator: Flag.String("aggregator").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("AGGREGATOR")),
      Flag.withDefault("http://127.0.0.1:8088"),
      Flag.withDescription("Comma-separated instances; the leader is resolved among them"),
    ),
    failureMode: Flag.Literals("failure-mode", ["sim", "envoy"] as const).pipe(
      Flag.withFallbackConfig(Config.Literals(["sim", "envoy"], "FAILURE_MODE")),
      Flag.withDefault("sim" as const),
      Flag.withDescription("Inject failure into the simulator or into a real upstream"),
    ),
    flakyUpstream: Flag.String("flaky-upstream").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("FLAKY_UPSTREAM")),
      Flag.withDefault("http://127.0.0.1"),
      Flag.withDescription("Base URL of the upstream to fail (--failure-mode=envoy)"),
    ),
    prometheus: Flag.String("prometheus").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("PROMETHEUS")),
      Flag.withDefault(""),
      Flag.withDescription("Prometheus base URL; enables the fleet step"),
    ),
  },
  (settings) => run(settings),
);

Command.run(demo, { version: VERSION }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
