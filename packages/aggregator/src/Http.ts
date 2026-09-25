import { Clock, Effect, Match, Metric, Option as O, Ref, Schema, Stream } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { NodeStream } from "@effect/platform-node";
import { CircuitEvent, classifySequence, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import { metricsResponse } from "@egress/tracing/Metrics.ts";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as Zlib from "node:zlib";
import { Aggregator } from "./Aggregator.ts";
import * as ConsoleFrames from "./ConsoleFrames.ts";
import { HaSettings } from "./Coordination.ts";
import { EventBus, EventSink } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";

const HERE = dirname(fileURLToPath(import.meta.url));

/** A diagnostic list; the exact count is `egress_subscriber_gaps_total`. */
const GAP_BUFFER = 100;

/** The delivery contract checked from outside the process: the webhook sink posts here over real HTTP. */
type Integrity = {
  readonly received: number;
  readonly snapshots: number;
  readonly duplicates: number;
  /** Every gap seen; `gaps` keeps only the last `GAP_BUFFER` descriptions. */
  readonly gapCount: number;
  readonly gaps: ReadonlyArray<string>;
  readonly bySequence: ReadonlyMap<string, number>;
  readonly recent: ReadonlyArray<CircuitEvent>;
};

export const emptyIntegrity: Integrity = {
  received: 0,
  snapshots: 0,
  duplicates: 0,
  gapCount: 0,
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
  return Match.value(classifySequence(highest, sequence)).pipe(
    Match.when("duplicate", () => ({
      ...base,
      bySequence: self.bySequence,
      duplicates: self.duplicates + 1,
    })),
    Match.when("gap", () => ({
      ...base,
      bySequence: advanced(),
      gapCount: self.gapCount + 1,
      gaps: [...self.gaps, `${apiId}: jumped ${last} -> ${sequence}`].slice(-GAP_BUFFER),
    })),
    Match.when(Match.is("first", "next"), () => ({ ...base, bySequence: advanced() })),
    Match.exhaustive,
  );
};

/** `rate` is bounded: 47 would mean "always" and -1 "never", both silently. */
const FailureRequest = Schema.Struct({
  apiId: Schema.NonEmptyString,
  rate: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
});

export const HttpLive = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    const bus = yield* EventBus;
    const fleet = yield* FleetSource;
    const sink = yield* EventSink;
    const ha = yield* HaSettings;
    const integrity = yield* Ref.make(emptyIntegrity);

    // Zeroed at startup: a counter has no series until its first update, and a
    // tile meant to sit at zero must not read "No data".
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
                Metric.update(Metric.withAttributes(Telemetry.outboxDepth, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.outboxReplayed, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.outboxDropped, { apiId }), 0),
                Metric.update(Metric.withAttributes(Telemetry.fencingConflicts, { apiId }), 0),
              ],
              { discard: true },
            ),
          { discard: true },
        ),
      ),
    );

    // The same for counters with no apiId; alerts name both directly.
    yield* Effect.all(
      [
        Metric.update(Telemetry.coordinationErrors, 0),
        Metric.update(Telemetry.consoleStreams, 0),
        Metric.update(Telemetry.consoleFramesBuilt, 0),
        Metric.update(Telemetry.consoleAttentionStreams, 0),
        Metric.update(Telemetry.consoleAttentionBuilt, 0),
        ...(["no-node-id", "went-quiet", "unreachable", "incomplete-stats"] as const).map(
          (reason) => Metric.update(Metric.withAttributes(Telemetry.replicasLost, { reason }), 0),
        ),
      ],
      { discard: true },
    );

    const stateFrame = Effect.gen(function* () {
      const apis = yield* agg.snapshots;
      const specs = yield* fleet.specs;
      const dead = yield* sink.deadLetters;
      const i = yield* Ref.get(integrity);
      // A standby polls nothing; say so rather than show an empty page.
      const isLeader = yield* agg.isLeader;
      return {
        apis,
        specs,
        leader: { isLeader, instanceId: ha.instanceId },
        deadLetters: dead.length,
        subscriber: {
          count: i.received,
          snapshots: i.snapshots,
          gaps: i.gapCount,
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

    const consoleFrames = yield* ConsoleFrames.make(stateFrame, "400 millis");
    // The attention view (ADR 015): its own schedule and broadcast, so it does
    // not pay for the full-frame build.
    const attention = yield* ConsoleFrames.makeAttention(agg.snapshots, "400 millis");

    const sseHeaders = {
      contentType: "text/event-stream",
      headers: { "cache-control": "no-cache", connection: "keep-alive" },
    };

    /** The event tape as it is published, one SSE event each. A fresh
     *  subscription to the bus per run, so per connection. */
    const tape = bus.subscribe.pipe(Stream.map((e) => ConsoleFrames.encodeEvent("cloudevent", e)));

    /** So the attention view's stream — quiet whenever nothing in the view
     *  changed — does not read as an idle connection to a load balancer. */
    const keepAlive = Stream.map(Stream.tick("15 seconds"), () => ConsoleFrames.KEEP_ALIVE);

    /** Anything but a non-negative integer means "no resume point", answered with a snapshot. */
    const lastEventIdOf = (request: HttpServerRequest.HttpServerRequest): O.Option<number> =>
      O.filter(
        O.map(O.fromUndefinedOr(request.headers["last-event-id"]), Number),
        (n) => Number.isInteger(n) && n >= 0,
      );

    /**
     * gzip flushed per event, by hand: `HttpMiddleware.compression` skips
     * `Cache-Control: no-transform`, which this route sets so proxies do not buffer it.
     */
    const gzip = (bytes: Stream.Stream<Uint8Array>) =>
      bytes.pipe(NodeStream.pipeThroughSimple(() => Zlib.createGzip({ flush: Zlib.constants.Z_SYNC_FLUSH })));

    const acceptsGzip = (request: HttpServerRequest.HttpServerRequest): boolean =>
      (request.headers["accept-encoding"] ?? "").includes("gzip");

    // Default: the full frame plus the tape. `?view=attention`: snapshot or
    // resumed patches, gzipped when accepted. Both end with the request scope.
    yield* router.add("GET", "/api/stream", () =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const view = new URL(request.url, "http://localhost").searchParams.get("view");

        if (view !== "attention") {
          return HttpServerResponse.stream(Stream.merge(consoleFrames.frames, Stream.encodeText(tape)), sseHeaders);
        }

        const body = Stream.merge(attention.connect(lastEventIdOf(request)), Stream.encodeText(Stream.merge(tape, keepAlive)));

        return acceptsGzip(request)
          ? HttpServerResponse.stream(gzip(body), {
              ...sseHeaders,
              headers: {
                ...sseHeaders.headers,
                "content-encoding": "gzip",
                "cache-control": "no-cache, no-transform",
                "x-accel-buffering": "no",
              },
            })
          : HttpServerResponse.stream(body, sseHeaders);
      }),
    );

    // The tape alone, for machines. The keep-alive stops a load balancer
    // closing a connection that is idle through a quiet hour.
    yield* router.add(
      "GET",
      "/api/events/stream",
      Effect.sync(() =>
        HttpServerResponse.stream(
          Stream.encodeText(Stream.merge(tape, Stream.map(Stream.tick("15 seconds"), () => ConsoleFrames.KEEP_ALIVE))),
          sseHeaders,
        ),
      ),
    );

    yield* router.add("GET", "/api/state", stateFrame.pipe(Effect.map((f) => HttpServerResponse.jsonUnsafe(f))));

    /**
     * Liveness: the control loop is still ticking. Readiness: one pass completed —
     * not leadership, or a rolling deploy would empty the rotation of standbys.
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

    // Not access-logged: a line per 2s scrape buries everything else.
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
        Effect.catch(() =>
          Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: "bad request" }, { status: 400 }),
          ),
        ),
      ),
    );

    // The demo's downstream consumer. In production this is your broker.
    yield* router.add("POST", "/subscriber/webhook", () =>
      Effect.gen(function* () {
        // Decoded, not cast: an undefined sequence compared false both ways and
        // passed as "next".
        const event = yield* HttpServerRequest.schemaBodyJson(CircuitEvent);
        const apiId = event.data.apiId;

        // Served APIs only: each unknown apiId costs two Prometheus series for
        // good (2,000 ids took /metrics from 37 series to 4,037).
        const served = yield* fleet.specs.pipe(
          Effect.map((specs) => specs.some((spec) => spec.apiId === apiId)),
        );
        const rejectUnknown = HttpServerResponse.jsonUnsafe(
          { accepted: false, reason: `unknown apiId: ${apiId}` },
          { status: 404 },
        );

        const accept = Effect.gen(function* () {
          // Diffed rather than derived from `event` alone: gap/duplicate is a
          // property of this event against what the subscriber already holds.
          const [before, after] = yield* Ref.modify(integrity, (i) => {
            const next = record(i, event);
            return [[i, next] as const, next];
          });
          yield* Metric.update(Metric.withAttributes(Telemetry.subscriberReceived, { apiId }), 1);
          yield* after.gapCount > before.gapCount
            ? Metric.update(Metric.withAttributes(Telemetry.subscriberGaps, { apiId }), 1)
            : Effect.void;
          yield* after.duplicates > before.duplicates
            ? Metric.update(Metric.withAttributes(Telemetry.subscriberDuplicates, { apiId }), 1)
            : Effect.void;
          return HttpServerResponse.jsonUnsafe({ accepted: true }, { status: 202 });
        });

        return yield* served ? accept : Effect.succeed(rejectUnknown);
      }).pipe(
        Effect.catch(() =>
          Effect.succeed(HttpServerResponse.jsonUnsafe({ accepted: false }, { status: 400 })),
        ),
      ),
    );
  }),
);
