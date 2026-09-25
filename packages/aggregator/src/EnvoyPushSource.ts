import { Array as Arr, Clock, Data, Effect, Layer, Metric, Option as O, Result } from "effect";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { FleetSource, makeIncompleteReporter, parseStats } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSpec } from "./FleetSource.ts";

/**
 * Envoy pushes its stats here; same `parseStats` and `ReplicaReport` as polling.
 *
 * - `@grpc/proto-loader` reads the partial schemas in `proto/` at runtime.
 * - Only a stream's first message carries the node id; it is remembered per call.
 * - One sink per aggregator: a sink cluster with two endpoints load-balances the
 *   stream, and each aggregator computes a quorum from a partial fleet.
 */

/** The gRPC sink could not listen: without it no replica can report, so startup fails. */
export class MetricsSinkUnavailable extends Data.TaggedError("MetricsSinkUnavailable")<{
  readonly port: number;
  readonly cause: string;
}> {}

const HERE = dirname(fileURLToPath(import.meta.url));
const PROTO_DIR = join(HERE, "..", "proto");

type MetricFamily = {
  readonly name?: string;
  readonly metric?: ReadonlyArray<{
    readonly counter?: { readonly value?: number };
    readonly gauge?: { readonly value?: number };
  }>;
};

type StreamMetricsMessage = {
  readonly identifier?: { readonly node?: { readonly id?: string } };
  readonly envoy_metrics?: ReadonlyArray<MetricFamily>;
};

/** With `emit_tags_as_labels` on, names shorten and the shared parser matches nothing. */
const flatten = (families: ReadonlyArray<MetricFamily>) => ({
  // A family with no name, and an entry that is neither a counter nor a gauge,
  // are both absences rather than zeroes — `filterMap` drops them where the
  // loops used to `continue` past them.
  stats: Arr.flatMap(families, (family) =>
    O.match(O.fromUndefinedOr(family.name), {
      onNone: () => [],
      onSome: (name) =>
        Arr.filterMap(family.metric ?? [], (entry) => {
          const value = entry.counter?.value ?? entry.gauge?.value;
          return value === undefined
            ? Result.fail(name)
            : Result.succeed({ name, value: Number(value) });
        }),
    }),
  ),
});

type Snapshot = {
  readonly stats: ReadonlyArray<{ name: string; value: number }>;
  readonly receivedAt: number;
};

/**
 * @param port     where Envoy pushes; one sink per aggregator instance.
 * @param specs    the APIs this aggregator reconciles.
 * @param staleMs  how long a replica's last push stays usable. Stats counted long
 *                 after they stopped arriving are a frozen gauge, so they expire.
 */
export const EnvoyPushFleetLayer = (
  port: number,
  specs: ReadonlyArray<ApiSpec>,
  staleMs = 15_000,
) =>
  Layer.effect(
    FleetSource,
    Effect.gen(function* () {
      const known = new Set(specs.map((s) => s.apiId));
      // A Map: written from a gRPC callback with no fiber; last write wins.
      const latest = new Map<string, Snapshot>();

      /** Pushes discarded for want of a node id; `poll` reads and zeroes it in one step. */
      let anonymous = 0;

      const noteIncomplete = makeIncompleteReporter();

      const packageDefinition = protoLoader.loadSync(
        "envoy/service/metrics/v3/metrics_service.proto",
        {
          includeDirs: [PROTO_DIR],
          keepCase: true,
          longs: Number,
          defaults: true,
        },
      );
      const loaded = grpc.loadPackageDefinition(packageDefinition) as unknown as {
        envoy: { service: { metrics: { v3: { MetricsService: { service: grpc.ServiceDefinition } } } } };
      };

      const server = yield* Effect.acquireRelease(
        Effect.sync(() => new grpc.Server()),
        (s) => Effect.sync(() => s.forceShutdown()),
      );

      server.addService(loaded.envoy.service.metrics.v3.MetricsService.service, {
        StreamMetrics: (
          call: grpc.ServerReadableStream<StreamMetricsMessage, unknown>,
          callback: (error: grpc.ServiceError | null, value?: object) => void,
        ) => {
          // Remembered for the life of the stream: Envoy sends it once.
          let replicaId = O.none<string>();
          call.on("data", (message: StreamMetricsMessage) => {
            const announced = O.filter(O.fromUndefinedOr(message.identifier?.node?.id), (id) => id !== "");
            replicaId = O.orElse(announced, () => replicaId);
            O.match(replicaId, {
              // An Envoy started without `--service-node` pushes stats nothing can
              // attribute; filing them under the last writer would be a second vote
              // from one replica. Dropped, but counted — see `poll`.
              onNone: () => void anonymous++,
              // `Date.now()`: no fiber here, and this layer only ever runs on wall time.
              onSome: (id) =>
                void latest.set(id, { stats: flatten(message.envoy_metrics ?? []).stats, receivedAt: Date.now() }),
            });
          });
          call.on("end", () => callback(null, {}));
          // A replica going away closes its stream. That is not an error here:
          // its snapshot expires on its own, which is the same thing the
          // polling layer does when a replica stops answering.
          call.on("error", () => callback(null, {}));
        },
      });

      // A port already taken is a configuration error to report, not a defect.
      yield* Effect.tryPromise({
        try: () =>
          new Promise<void>((resolve, reject) => {
            server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err) =>
              err ? reject(err) : resolve(),
            );
          }),
        catch: (cause) => new MetricsSinkUnavailable({ port, cause: String(cause) }),
      });

      yield* Effect.logInfo(`envoy metrics sink listening on 0.0.0.0:${port}`);

      const poll = Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        const unidentified = anonymous;
        anonymous = 0;
        yield* Effect.when(
          Effect.andThen(
            Effect.logWarning(
              `envoy metrics: discarded ${unidentified} push(es) carrying no node id — ` +
                `a replica started without --service-node reports stats nobody can ` +
                `attribute, and is therefore absent from this API's quorum`,
            ),
            Metric.update(Metric.withAttributes(Telemetry.replicasLost, { reason: "no-node-id" }), 1),
          ),
          Effect.succeed(unidentified > 0),
        );

        const [quiet, live] = Arr.separate(
          Arr.map([...latest], ([replicaId, snapshot]) =>
            now - snapshot.receivedAt > staleMs
              ? Result.fail(replicaId)
              : Result.succeed([replicaId, snapshot] as const),
          ),
        );

        // Removed rather than skipped, so the departure is announced once rather
        // than re-discovered every tick.
        yield* Effect.forEach(
          quiet,
          (replicaId) =>
            Effect.all(
              [
                Effect.sync(() => latest.delete(replicaId)),
                Effect.logWarning(
                  `envoy metrics: ${replicaId} stopped pushing ${staleMs}ms ago and no ` +
                    `longer counts toward any quorum`,
                ),
                Metric.update(
                  Metric.withAttributes(Telemetry.replicasLost, { reason: "went-quiet" }),
                  1,
                ),
              ],
              { discard: true },
            ),
          { discard: true },
        );

        // `observedAt` is the tick's clock, not the push's: downstream ages reports
        // against the loop's own time.
        return Arr.flatten(
          yield* Effect.forEach(live, ([replicaId, snapshot]) => {
            const parsed = parseStats(replicaId, snapshot, now, (c) => known.has(c));
            return Effect.as(noteIncomplete(replicaId, parsed.incomplete), parsed.reports);
          }),
        );
      });

      return {
        poll,
        specs: Effect.succeed(specs),
        setFailureRate: () => Effect.succeed(false),
      };
    }),
  );
