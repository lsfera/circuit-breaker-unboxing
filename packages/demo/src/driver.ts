import { Config, Console, Duration, Effect, Option as O, Schedule, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { VERSION } from "@egress/config/Settings.ts";
import { CircuitEvent, SEQUENCED_EVENT, State } from "@egress/domain/Model.ts";

/** Derived from the published schema rather than restated — see getEvents. */
type CircuitEventData = CircuitEvent["data"];

/**
 * Drives the demo incident over HTTP and narrates every published transition.
 *
 *   pnpm run demo                  # sim fleet, localhost:8088
 *   pnpm run demo -- shipping-rates
 *   pnpm run demo:envoy            # against `docker compose up`
 *
 * It reads /api/events, so what it prints is the published contract. The modes
 * differ only in how failure is injected: the sim's /api/failure, or
 * flaky-upstream's /__fail. With PROMETHEUS set it also asserts what the daemon
 * fleet did; unset, that step is skipped.
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
  /** Only the leader's /api/events has anything, so the list is resolved against the live leader. */
  const CANDIDATES = settings.aggregator
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  let ORIGIN = CANDIDATES[0] ?? "http://127.0.0.1:8088";
  const API = settings.api;
  const FAILURE_MODE = settings.failureMode;
  const FLAKY_UPSTREAM = settings.flakyUpstream;
  const PROMETHEUS = settings.prometheus;

  // Real Envoy needs traffic flowing before a failure shows.
  const TIMEOUT_SCALE = FAILURE_MODE === "envoy" ? 2 : 1;

  /** One port per endpoint. Failing all is an outage; failing some is how to reach DEGRADED. */
  const UPSTREAM_PORTS: Readonly<Record<string, ReadonlyArray<number>>> = {
    "payments-provider": [8080, 8081, 8082, 8083, 8084, 8085],
    "shipping-rates": [8090, 8091, 8092, 8093],
    "tax-calc": [8094, 8095, 8096],
  };

  /** Decoded, not cast: a contract change must fail naming the field. Undecodable events are counted. */
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

  /** Decoded: a missing `duplicates` would be `undefined > 0`, false, and pass silently. */
  const getSubscriber = Effect.tryPromise({
    try: () => fetch(`${ORIGIN}/api/subscriber`).then((r) => r.json() as Promise<unknown>),
    catch: (cause) => new Error(`GET /api/subscriber failed: ${String(cause)}`),
  }).pipe(Effect.flatMap(decodeSubscriber));

  /** A refusal is a failure too: a 400 here means the incident this demo narrates never started. */
  const post = (url: string, body: unknown) =>
    Effect.tryPromise({
      try: () =>
        fetch(url, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        }),
      catch: (cause) => new Error(`POST ${url} failed: ${String(cause)}`),
    }).pipe(
      Effect.filterOrFail(
        (res) => res.ok,
        (res) => new Error(`POST ${url} answered ${res.status}`),
      ),
      Effect.asVoid,
    );

  const setFailureRate = (apiId: string, rate: number) => {
    if (FAILURE_MODE === "sim") return post(`${ORIGIN}/api/failure`, { apiId, rate });
    const ports = UPSTREAM_PORTS[apiId];
    if (ports === undefined) {
      return Effect.fail(
        new Error(
          `no flaky-upstream ports known for "${apiId}" — expected one of ${Object.keys(UPSTREAM_PORTS).join(", ")}`,
        ),
      );
    }
    return Effect.forEach(ports, (port) => post(`${FLAKY_UPSTREAM}:${port}/__fail`, { rate }), { discard: true });
  };

  /** `null`: no series, i.e. no daemons for this API, as opposed to zero. */
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
    // The broker's count, not a daemon's; includes the prober during HALF_OPEN.
    active: promQuery(`sum(rabbitmq_detailed_queue_consumers{queue="${API}.work"})`),
    // No daemon knows the fleet size; the number reporting is the size.
    size: promQuery(`count(egress_daemon_target_fraction{apiId="${API}"})`),
    work: promQuery(`rabbitmq_detailed_queue_messages{queue="${API}.work"}`),
    dead: promQuery(`rabbitmq_detailed_queue_messages{queue="${API}.work.dead"}`),
    broken: promQuery(
      `sum(egress_daemon_control_gaps_total{apiId="${API}"}) + ` +
        `sum(egress_daemon_control_duplicates_total{apiId="${API}"})`,
    ),
  });

  /** Sampled in the background: the queue peaks mid-outage, not at either edge. */
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

  /** A condition, not a sleep: scrapes lag the event by a few seconds. */
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
   * Waits for the transition to `expected` after `after`. `expected` is the
   * assertion: returning the newest transition once consumed HALF_OPEN and the
   * OPEN behind it together, and reported recovery with the circuit open.
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
