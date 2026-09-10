import { Clock, Effect, Layer, Metric } from "effect";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { FleetSource, makeIncompleteReporter, parseStats } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSpec } from "./FleetSource.ts";
import type { ReplicaReport } from "@egress/domain/Model.ts";

/**
 * Envoy pushing its stats here, instead of this process polling admin ports.
 * Same `FleetSource`, same `parseStats`, same `ReplicaReport`; only the
 * transport differs.
 *
 * Three things to know before changing it:
 *
 * - No build step. `@grpc/proto-loader` reads the partial schemas in `proto/`
 *   at runtime, and protobuf addresses fields by number, so Envoy's messages
 *   decode without vendoring its api tree.
 * - Only the *first* message on a stream carries the node identifier, so it is
 *   remembered per call. Reading it per message loses every replica's identity.
 * - Push does not fan out. A sink names one gRPC cluster, and pointing that at
 *   two aggregators load-balances the stream, giving each a partial fleet to
 *   compute a quorum from. Hence one sink per aggregator in the Envoy config.
 *
 * A replica leaving is counted and logged rather than silent — see
 * docs/decisions/009-what-the-quorum-is-a-quorum-of.md.
 */

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

/**
 * With `emit_tags_as_labels` off, each family name is the full dotted stat name,
 * exactly as the admin endpoint reports it — which is what lets `parseStats` be
 * shared. Turning that option on shortens the names and the shared regex then
 * matches nothing at all.
 */
const flatten = (families: ReadonlyArray<MetricFamily>) => {
  const stats: Array<{ name: string; value: number }> = [];
  for (const family of families) {
    const name = family.name;
    if (name === undefined) continue;
    for (const entry of family.metric ?? []) {
      const value = entry.counter?.value ?? entry.gauge?.value;
      if (value === undefined) continue;
      stats.push({ name, value: Number(value) });
    }
  }
  return { stats };
};

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
      /**
       * A plain Map, not a Ref: writes happen in a gRPC socket callback with no
       * fiber to run an Effect in, reads happen in `poll` on the loop's fiber, and
       * the shared state is last-write-wins.
       */
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
          let replicaId: string | null = null;
          call.on("data", (message: StreamMetricsMessage) => {
            const id = message.identifier?.node?.id;
            if (id !== undefined && id !== "") replicaId = id;
            if (replicaId === null) {
              // An Envoy started without `--service-node` pushes stats nothing can
              // attribute; filing them under the last writer would be a second vote
              // from one replica. Dropped, but counted — see `poll`.
              anonymous++;
              return;
            }
            // `Date.now()` rather than the Effect clock, because this is not
            // running on a fiber. `poll` compares it against the loop's clock,
            // which is the same wall clock in every configuration this layer
            // is used in — it receives from a real Envoy or it receives
            // nothing.
            latest.set(replicaId, {
              stats: flatten(message.envoy_metrics ?? []).stats,
              receivedAt: Date.now(),
            });
          });
          call.on("end", () => callback(null, {}));
          // A replica going away closes its stream. That is not an error here:
          // its snapshot expires on its own, which is the same thing the
          // polling layer does when a replica stops answering.
          call.on("error", () => callback(null, {}));
        },
      });

      yield* Effect.promise(
        () =>
          new Promise<void>((resolve, reject) => {
            server.bindAsync(
              `0.0.0.0:${port}`,
              grpc.ServerCredentials.createInsecure(),
              (err) => (err ? reject(err) : resolve()),
            );
          }),
      );

      yield* Effect.logInfo(`envoy metrics sink listening on 0.0.0.0:${port}`);

      const poll = Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        const unidentified = anonymous;
        anonymous = 0;
        if (unidentified > 0) {
          yield* Effect.logWarning(
            `envoy metrics: discarded ${unidentified} push(es) carrying no node id — ` +
              `a replica started without --service-node reports stats nobody can ` +
              `attribute, and is therefore absent from this API's quorum`,
          );
          yield* Metric.update(
            Metric.withAttributes(Telemetry.replicasLost, { reason: "no-node-id" }),
            1,
          );
        }

        const reports: ReplicaReport[] = [];
        for (const [replicaId, snapshot] of latest) {
          if (now - snapshot.receivedAt > staleMs) {
            // Removed rather than skipped, so the departure is announced once rather
            // than re-discovered every tick.
            latest.delete(replicaId);
            yield* Effect.logWarning(
              `envoy metrics: ${replicaId} stopped pushing ${staleMs}ms ago and no ` +
                `longer counts toward any quorum`,
            );
            yield* Metric.update(
              Metric.withAttributes(Telemetry.replicasLost, { reason: "went-quiet" }),
              1,
            );
            continue;
          }
          // `observedAt` is the tick's clock, not the push's: downstream ages reports
          // against the loop's own time.
          const parsed = parseStats(replicaId, snapshot, now, (c) => known.has(c));
          yield* noteIncomplete(replicaId, parsed.incomplete);
          reports.push(...parsed.reports);
        }
        return reports;
      });

      return {
        poll,
        specs: Effect.succeed(specs),
        setFailureRate: () => Effect.succeed(false),
      };
    }),
  );
