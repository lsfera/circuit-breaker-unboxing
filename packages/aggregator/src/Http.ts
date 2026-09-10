import { Clock, Effect, Metric, Option as O, Ref, Schedule, Schema, Stream } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Sse } from "effect/unstable/encoding";
import { CircuitEvent, classifySequence, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import { metricsResponse } from "@egress/tracing/Metrics.ts";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Aggregator } from "./Aggregator.ts";
import { HaSettings } from "./Coordination.ts";
import { EventBus, EventSink } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";

const HERE = dirname(fileURLToPath(import.meta.url));

/**
 * How many gap descriptions to keep. Same reasoning as the sinks' dead-letter
 * buffers: this is a diagnostic list in a long-running process, and the exact
 * count is a Prometheus counter (`egress_subscriber_gaps_total`).
 */
const GAP_BUFFER = 100;

/**
 * Delivery integrity as seen from OUTSIDE the process. The webhook sink posts
 * here over real HTTP, so this measures the contract rather than an in-process
 * function call: per-API sequences must be gapless and non-repeating.
 *
 * Snapshots deliberately republish the current sequence, so only
 * state_changed events carry that guarantee.
 */
type Integrity = {
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
  if (event.type !== SEQUENCED_EVENT) {
    return { ...base, snapshots: self.snapshots + 1 };
  }
  const { apiId, sequence } = event.data;
  const last = self.bySequence.get(apiId);
  const highest = O.fromUndefinedOr(last);
  const advanced = () => new Map(self.bySequence).set(apiId, sequence);
  // The rule lives in @egress/domain, shared with the daemon fleet's own
  // observer. What is local here is the shape: one highest sequence per API,
  // because this process watches all of them at once.
  switch (classifySequence(highest, sequence)) {
    case "duplicate":
      return { ...base, bySequence: self.bySequence, duplicates: self.duplicates + 1 };
    case "gap":
      return {
        ...base,
        bySequence: advanced(),
        gaps: [
          ...self.gaps,
          `${apiId}: jumped ${last} -> ${sequence}`,
        ].slice(-GAP_BUFFER),
      };
    case "first":
    case "next":
      return { ...base, bySequence: advanced() };
  }
};

/**
 * One `Sse.Event`, encoded by `Sse.encode` below rather than by a template
 * string here. The bytes are identical — checked — and the point is that
 * @egress/subscriber decodes with the same module, so the wire format has one
 * definition instead of an encoder and a parser that happen to agree.
 */
/**
 * `/api/failure`'s body. `rate` is a probability, and it is bounded here for
 * the reason every other bound in this repo exists: `setFailureRate(47)` is
 * `Math.random() < 47`, which is "always", and `-1` is "never" — two silent
 * settings that look like a typo and behave like a decision.
 */
const FailureRequest = Schema.Struct({
  apiId: Schema.NonEmptyString,
  rate: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
});

const sse = (event: string, data: unknown): Sse.Event => ({
  _tag: "Event",
  event,
  id: undefined,
  data: JSON.stringify(data),
});

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
    // for the console, and the event tape as it is published. Both end with the
    // request scope, so a client that disconnects needs no tear-down of ours.
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
          Stream.merge(states, events).pipe(
            Stream.pipeThroughChannel(Sse.encode()),
            Stream.encodeText,
          ),
          {
            contentType: "text/event-stream",
            headers: { "cache-control": "no-cache", connection: "keep-alive" },
          },
        );
      }),
    );

    yield* router.add("GET", "/api/state", stateFrame.pipe(Effect.map((f) => HttpServerResponse.jsonUnsafe(f))));

    /**
     * Liveness is "is the control loop still running" — a dead loop leaves a
     * process serving 200s with every gauge frozen, which reads as a quiet system.
     *
     * Readiness is "can this instance serve requests", and emphatically not
     * leadership: a standby serves the same read-only API and is one lease away
     * from leading, so marking it unready would empty the rotation during a
     * rolling deploy. It waits for one completed pass, no more.
     */
    const health = Effect.gen(function* () {
      const [now, last, leader] = yield* Effect.all([
        Clock.currentTimeMillis,
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
      metricsResponse,
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

    yield* router.add("POST", "/api/failure", () =>
      Effect.gen(function* () {
        const body = yield* HttpServerRequest.schemaBodyJson(FailureRequest);
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
    yield* router.add("POST", "/subscriber/webhook", () =>
      Effect.gen(function* () {
        // Decoded, not cast. This endpoint is the delivery-contract check, and
        // it was trusting its input: an event whose `sequence` was absent or
        // not a number sailed past `as CircuitEvent` into `record`, where
        // `undefined <= n` and `undefined > n + 1` are both false — so it
        // counted as an ordinary next event and wrote `undefined` into the
        // per-API high-water mark. The one endpoint whose readings are quoted
        // as proof of the contract was the one not applying it.
        const event = yield* HttpServerRequest.schemaBodyJson(CircuitEvent);
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
