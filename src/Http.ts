import { Effect, Metric, Ref, Schedule, Stream } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { PrometheusMetrics } from "effect/unstable/observability";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Aggregator } from "./Aggregator.ts";
import { EventBus, EventSink } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { CircuitEvent } from "./domain/Model.ts";

const HERE = dirname(fileURLToPath(import.meta.url));

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
  if (sequence === seen) return { ...base, bySequence, duplicates: self.duplicates + 1 };
  if (sequence > seen + 1) {
    return {
      ...base,
      bySequence,
      gaps: [...self.gaps, `${apiId}: jumped ${seen} -> ${sequence}`],
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
    const integrity = yield* Ref.make(emptyIntegrity);

    const stateFrame = Effect.gen(function* () {
      const apis = yield* agg.snapshots;
      const specs = yield* fleet.specs;
      const dead = yield* sink.deadLetters;
      const i = yield* Ref.get(integrity);
      return {
        apis,
        specs,
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

    // Same in-process Metric registry the Aggregator and EventSink update —
    // nothing here is scraped or pushed separately. Point Prometheus (or the
    // docker-compose monitoring stack) at this path in either sim or real-Envoy
    // mode; it works identically since it reads the registry, not the fleet.
    yield* router.add(
      "GET",
      "/metrics",
      PrometheusMetrics.format().pipe(
        Effect.map((body) =>
          HttpServerResponse.text(body, {
            contentType: "text/plain; version=0.0.4; charset=utf-8",
          }),
        ),
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
