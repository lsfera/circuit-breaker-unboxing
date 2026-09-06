import { Effect, Metric, Ref, Schedule, Stream } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Aggregator } from "./Aggregator.ts";
import { HaSettings } from "./Coordination.ts";
import { EventBus, EventSink } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

const HERE = dirname(fileURLToPath(import.meta.url));

/**
 * How many gap descriptions to keep. Same reasoning as the sinks' dead-letter
 * buffers: this is a diagnostic list in a long-running process, and the exact
 * count is a Prometheus counter (`egress_subscriber_gaps_total`).
 */
export const GAP_BUFFER = 100;

/**
 * Delivery integrity as seen from OUTSIDE the process. The webhook sink posts
 * here over real HTTP, so this measures the contract rather than an in-process
 * function call: per-API sequences must be gapless and non-repeating.
 *
 * Snapshots deliberately republish the current sequence, so only
 * state_changed events carry that guarantee.
 */
export type Integrity = {
  readonly received: number;
  readonly snapshots: number;
  readonly duplicates: number;
  readonly gaps: ReadonlyArray<string>;
  readonly bySequence: ReadonlyMap<string, number>;
  readonly recent: ReadonlyArray<CircuitEvent>;
};

export const emptyIntegrity: Integrity = {
  received: 0,
  snapshots: 0,
  duplicates: 0,
  gaps: [],
  bySequence: new Map(),
  recent: [],
};

export const record = (self: Integrity, event: CircuitEvent): Integrity => {
  const recent = [event, ...self.recent].slice(0, 100);
  const base = { ...self, received: self.received + 1, recent };
  if (event.type !== "egress.circuit.state_changed") {
    return { ...base, snapshots: self.snapshots + 1 };
  }
  const { apiId, sequence } = event.data;
  const seen = self.bySequence.get(apiId);
  const bySequence = new Map(self.bySequence);
  if (seen === undefined || sequence > seen) bySequence.set(apiId, sequence);
  if (seen === undefined) return { ...base, bySequence };
  // `<=`, not `===`. A sequence that goes *backwards* is the same violation
  // as one that repeats — a number was reused — and it is the shape a
  // leadership bug actually produces: an instance that resumes from stale
  // in-memory state republishes numbers a later leader already used. Testing
  // only for equality left that case falling through this function
  // uncounted, which made the one check that is supposed to prove the
  // contract blind to the most likely way of breaking it.
  if (sequence <= seen) return { ...base, bySequence, duplicates: self.duplicates + 1 };
  if (sequence > seen + 1) {
    return {
      ...base,
      bySequence,
      gaps: [...self.gaps, `${apiId}: jumped ${seen} -> ${sequence}`].slice(-GAP_BUFFER),
    };
  }
  return { ...base, bySequence };
};

const sse = (event: string, data: unknown) =>
  `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

export const HttpLive = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    const bus = yield* EventBus;
    const fleet = yield* FleetSource;
    const sink = yield* EventSink;
    const ha = yield* HaSettings;
    const integrity = yield* Ref.make(emptyIntegrity);

    // Register every per-API counter at zero before anything has happened to
    // it. An `effect` counter has no series until its first update, so
    // without this the dashboard's delivery-contract tiles read "No data"
    // rather than 0 — and a tile whose whole job is to sit at zero through
    // an incident is worse than useless when zero looks like broken.
    yield* fleet.specs.pipe(
      Effect.flatMap((specs) =>
        Effect.forEach(
          specs,
          ({ apiId }) =>
            Effect.all(
              [
                Metric.update(Metric.withAttributes(Telemetry.subscriberReceived, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.subscriberGaps, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.subscriberDuplicates, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.webhookDelivered, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.webhookFailed, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.webhookDeadLettered, { apiId }), 0),
                // The outbox metrics belong in this list more than any of the
                // others: depth is zero in every healthy minute this system
                // will ever have, so without a series at zero the panel that
                // is supposed to show "nothing is stuck" shows "no data",
                // which is what a broken exporter looks like.
                Metric.update(Metric.withAttributes(Telemetry.outboxDepth, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.outboxReplayed, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.outboxDropped, { apiId }), 0),
              ],
              { discard: true },
            ),
          { discard: true },
        ),
      ),
    );

    const stateFrame = Effect.gen(function* () {
      const apis = yield* agg.snapshots;
      const specs = yield* fleet.specs;
      const dead = yield* sink.deadLetters;
      const i = yield* Ref.get(integrity);
      // A standby polls nothing, so it has no APIs to show. Without saying
      // so, its console is an empty page that looks exactly like a broken
      // one — which is a bad thing to be looking at during a failover demo,
      // when the empty page is the correct behaviour.
      const isLeader = yield* agg.isLeader;
      return {
        apis,
        specs,
        leader: { isLeader, instanceId: ha.instanceId },
        deadLetters: dead.length,
        subscriber: {
          count: i.received,
          snapshots: i.snapshots,
          gaps: i.gaps.length,
          duplicates: i.duplicates,
        },
      };
    });

    yield* router.add(
      "GET",
      "/",
      Effect.promise(() => readFile(join(HERE, "..", "public", "index.html"), "utf8")).pipe(
        Effect.map(HttpServerResponse.html),
      ),
    );

    // Two independent streams merged into one SSE body: periodic state frames
    // for the console, and the event tape as it is published. In the pre-Effect
    // version this was a Set of response objects plus a setInterval that had to
    // be torn down by hand; here the stream ends with the request scope.
    yield* router.add(
      "GET",
      "/api/stream",
      Effect.sync(() => {
        const states = Stream.fromEffectSchedule(
          stateFrame,
          Schedule.spaced("400 millis"),
        ).pipe(Stream.map((frame) => sse("state", frame)));
        const events = bus.subscribe.pipe(
          Stream.map((e) => sse("cloudevent", e)),
        );
        return HttpServerResponse.stream(
          Stream.merge(states, events).pipe(Stream.encodeText),
          {
            contentType: "text/event-stream",
            headers: { "cache-control": "no-cache", connection: "keep-alive" },
          },
        );
      }),
    );

    yield* router.add("GET", "/api/state", stateFrame.pipe(Effect.map((f) => HttpServerResponse.jsonUnsafe(f))));

    /**
     * Liveness and readiness, and the distinction between them is the whole
     * point of having two routes.
     *
     * **Liveness** is "is the control loop still running". The failure this
     * catches is the one that actually happened here: a defect out of the
     * tick killed `Effect.repeat`, the loop was simply gone, and the process
     * kept serving HTTP 200 with every gauge frozen at its last value — which
     * is indistinguishable from a system where nothing is happening. Anything
     * that restarts unhealthy containers should watch this.
     *
     * **Readiness** is "can this instance serve requests", and it is
     * emphatically **not** leadership. A standby serves the same read-only
     * API, keeps its own metrics, and is one lease away from leading. Marking
     * it unready would take it out of rotation for doing its job, and during
     * a rolling deploy it would take out the pair: the leader is stopping and
     * the standby is "not ready", so nothing is left. What readiness waits
     * for is one completed pass, so `/api/state` answers with the fleet
     * rather than with an empty registry.
     */
    const health = Effect.gen(function* () {
      const [now, last, leader] = yield* Effect.all([
        Effect.clockWith((c) => c.currentTimeMillis),
        agg.lastTickAt,
        agg.isLeader,
      ]);
      // Three ticks, or five seconds, whichever is longer: one missed tick is
      // a slow poll, three is a loop that has stopped. A short tickMs must not
      // turn a GC pause into a restart.
      const staleAfterMs = Math.max(agg.tickMs * 3, 5000);
      const started = last > 0;
      return {
        started,
        isLeader: leader,
        lastTickAgoMs: started ? now - last : null,
        staleAfterMs,
        live: !started || now - last <= staleAfterMs,
        instanceId: ha.instanceId,
      };
    });

    yield* router.add(
      "GET",
      "/livez",
      health.pipe(
        Effect.map((h) =>
          HttpServerResponse.jsonUnsafe(h, { status: h.live ? 200 : 503 }),
        ),
        HttpMiddleware.withLoggerDisabled,
      ),
    );
    yield* router.add(
      "GET",
      "/readyz",
      health.pipe(
        Effect.map((h) =>
          HttpServerResponse.jsonUnsafe(h, { status: h.started && h.live ? 200 : 503 }),
        ),
        HttpMiddleware.withLoggerDisabled,
      ),
    );

    // Same in-process Metric registry the Aggregator and EventSink update —
    // nothing here is scraped or pushed separately. Point Prometheus (or the
    // docker-compose monitoring stack) at this path in either sim or real-Envoy
    // mode; it works identically since it reads the registry, not the fleet.
    // Logging disabled for this route alone: Prometheus scrapes every 2s, so
    // an access log line per scrape buries every log that says something.
    yield* router.add(
      "GET",
      "/metrics",
      PrometheusMetrics.format().pipe(
        Effect.map((body) =>
          HttpServerResponse.text(body, {
            contentType: "text/plain; version=0.0.4; charset=utf-8",
          }),
        ),
        HttpMiddleware.withLoggerDisabled,
      ),
    );

    yield* router.add(
      "GET",
      "/api/events",
      bus.recent.pipe(
        Effect.map((events) => HttpServerResponse.jsonUnsafe({ events: [...events].reverse() })),
      ),
    );

    yield* router.add(
      "GET",
      "/api/subscriber",
      Ref.get(integrity).pipe(
        Effect.map((i) =>
          HttpServerResponse.jsonUnsafe({
            received: i.received,
            snapshots: i.snapshots,
            duplicates: i.duplicates,
            gaps: i.gaps,
            recent: i.recent.slice(0, 50),
          }),
        ),
      ),
    );

    yield* router.add("POST", "/api/failure", (request) =>
      Effect.gen(function* () {
        const body = (yield* request.json) as { apiId?: string; rate?: number };
        if (!body.apiId || typeof body.rate !== "number") {
          return HttpServerResponse.jsonUnsafe(
            { error: "expected { apiId, rate }" },
            { status: 400 },
          );
        }
        const ok = yield* fleet.setFailureRate(body.apiId, body.rate);
        const specs = yield* fleet.specs;
        return HttpServerResponse.jsonUnsafe({ ok, specs }, { status: ok ? 200 : 404 });
      }).pipe(
        Effect.catchCause(() =>
          Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: "bad request" }, { status: 400 }),
          ),
        ),
      ),
    );

    // The demo's downstream consumer. In production this is your broker.
    yield* router.add("POST", "/subscriber/webhook", (request) =>
      Effect.gen(function* () {
        const event = (yield* request.json) as CircuitEvent;
        const apiId = event.data.apiId;
        // Diffed rather than derived from `event` alone: gap/duplicate is a
        // property of this event against what the subscriber already holds.
        const [before, after] = yield* Ref.modify(integrity, (i) => {
          const next = record(i, event);
          return [[i, next] as const, next];
        });
        yield* Metric.update(Metric.withAttributes(Telemetry.subscriberReceived, { apiId }), 1);
        if (after.gaps.length > before.gaps.length) {
          yield* Metric.update(Metric.withAttributes(Telemetry.subscriberGaps, { apiId }), 1);
        }
        if (after.duplicates > before.duplicates) {
          yield* Metric.update(Metric.withAttributes(Telemetry.subscriberDuplicates, { apiId }), 1);
        }
        return HttpServerResponse.jsonUnsafe({ accepted: true }, { status: 202 });
      }).pipe(
        Effect.catchCause(() =>
          Effect.succeed(HttpServerResponse.jsonUnsafe({ accepted: false }, { status: 400 })),
        ),
      ),
    );
  }),
);
