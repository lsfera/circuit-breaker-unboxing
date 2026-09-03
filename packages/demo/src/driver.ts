import { Duration, Effect } from "effect";
import { NodeRuntime } from "@effect/platform-node";

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
 *   FAILURE_MODE=sim|envoy   (default sim)
 *   AGGREGATOR=http://host:port          (default http://127.0.0.1:8088)
 *   FLAKY_UPSTREAM=http://host           (default http://127.0.0.1; envoy mode only)
 */

const ORIGIN = process.env["AGGREGATOR"] ?? "http://127.0.0.1:8088";
const API = process.argv[2] ?? "payments-provider";
const FAILURE_MODE = process.env["FAILURE_MODE"] === "envoy" ? "envoy" : "sim";
const FLAKY_UPSTREAM = process.env["FLAKY_UPSTREAM"] ?? "http://127.0.0.1";

// Real Envoy's outlier detection needs actual requests flowing (see
// traffic-generator.mjs, part of `docker compose up`) before failure injected
// here shows up as anything — the sim fleet has no such lag, so give the
// envoy path more room on every wait below.
const TIMEOUT_SCALE = FAILURE_MODE === "envoy" ? 2 : 1;

/** flaky-upstream.mjs's three independently controllable ports, one per API. */
const UPSTREAM_PORT: Readonly<Record<string, number>> = {
  "payments-provider": 8080,
  "shipping-rates": 8081,
  "tax-calc": 8082,
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
  const port = UPSTREAM_PORT[apiId];
  if (port === undefined) {
    return Effect.fail(
      new Error(
        `no flaky-upstream port known for "${apiId}" — expected one of ${Object.keys(UPSTREAM_PORT).join(", ")}`,
      ),
    );
  }
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
};

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
  const injectVia =
    FAILURE_MODE === "envoy"
      ? `${FLAKY_UPSTREAM}:${UPSTREAM_PORT[API] ?? "?"}/__fail`
      : `${ORIGIN}/api/failure`;
  yield* Effect.log(`driving ${API} against ${ORIGIN} (failure via ${injectVia})`);

  // Fails fast with a one-line fix instead of the six-step choreography dying
  // on step one with a fetch stack trace — the aggregator not being up yet is
  // the overwhelmingly likely reason this doesn't connect.
  yield* getEvents.pipe(
    Effect.catchCause(() =>
      Effect.die(
        `cannot reach the aggregator at ${ORIGIN} — start it first, in another terminal: pnpm start`,
      ),
    ),
  );

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
});

NodeRuntime.runMain(program);
