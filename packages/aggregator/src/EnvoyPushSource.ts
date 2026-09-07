import { Effect, Layer, Metric } from "effect";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { FleetSource, parseStats } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSpec } from "./FleetSource.ts";
import type { ReplicaReport } from "@egress/domain/Model.ts";

/**
 * The other half of the ingestion question: Envoy pushing its stats here,
 * instead of this process polling every replica's admin port.
 *
 * Same `FleetSource` interface, same `parseStats`, same `ReplicaReport` — only
 * the transport differs, which is the claim the interface was written to make
 * good on. Nothing downstream knows which layer it is talking to.
 *
 * Four things about this are worth knowing before reading the code.
 *
 * **There is no build step, and there did not have to be.** The reason
 * polling was chosen originally was that a gRPC server implies generated
 * stubs. It does not: `@grpc/proto-loader` reads `.proto` files at runtime,
 * and protobuf addresses fields by number, so the deliberately partial schemas
 * in `proto/` decode Envoy's real messages without vendoring Envoy's api tree.
 *
 * **Only the first message on a stream carries the node identifier.** Envoy
 * opens one stream per sink and pushes one message per flush interval;
 * `identifier` is set once, at the start. Reading it per message works
 * perfectly in a test with one message and loses every replica's identity in
 * production, so it is remembered per call.
 *
 * **Push does not fan out on its own.** A stats sink names one gRPC cluster.
 * Point that cluster at two aggregators and Envoy load-balances the stream, so
 * each instance sees *some* replicas — a quorum computed from a partial fleet,
 * which is worse than no data because it looks like data. The Envoy config
 * therefore declares one sink per aggregator, and each pushes the whole set.
 *
 * **A replica can leave without anyone noticing, and this is where that was
 * possible.** The paragraph above names the hazard exactly, and the fix it
 * describes only closes one cause of it. Two others lived here: a stream
 * pushing with no node id was dropped message by message in silence, and a
 * replica that stopped pushing expired out of the fleet with nothing said and
 * its entry left behind. Both quietly shrink the denominator every quorum in
 * @egress/domain is a fraction of. They are counted and logged now — see
 * docs/decisions/009-what-the-quorum-is-a-quorum-of.md for why the arithmetic
 * itself is deliberately not changed.
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
 * Envoy sends one `MetricFamily` per stat, each with a single unlabelled
 * entry, because `emit_tags_as_labels` is left off: the family name is the
 * full dotted stat name (`cluster.payments-provider.membership_healthy`),
 * exactly as the admin endpoint reports it.
 *
 * That is what lets `parseStats` be shared rather than reimplemented, and it
 * is the reason the sink must NOT be configured with `emit_tags_as_labels:
 * true` — that form extracts the tags into labels and shortens the name, and
 * the shared regex would match nothing at all.
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
 * @param port     where Envoy pushes. One sink per aggregator instance, so
 *                 this is a normal listening port, not a shared one.
 * @param specs    the APIs this aggregator reconciles, same as the polling layer.
 * @param staleMs  how long a replica's last push stays usable. A replica that
 *                 has stopped pushing is a replica that is gone, and the
 *                 quorum rule already tolerates a missing one — but stats that
 *                 keep being counted long after they stopped arriving are the
 *                 push equivalent of a frozen gauge, so they expire.
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
       * A plain Map, not a Ref, and the reason is the boundary it lives on:
       * writes happen inside a gRPC socket callback, which is ordinary
       * JavaScript with no fiber to run an Effect in. Bridging one back into
       * the runtime for a single map assignment would buy nothing but a
       * runtime handle to keep alive. Reads happen in `poll`, on the loop's
       * own fiber, and the only shared state is last-write-wins.
       */
      const latest = new Map<string, Snapshot>();

      /**
       * Pushes discarded for want of a node id, since the last poll.
       *
       * Accumulated as a plain number for the same reason `latest` is a plain
       * Map: the write happens in a socket callback with no fiber to run an
       * Effect in. `poll` reads and zeroes it in one synchronous step and
       * publishes the difference — the shape @egress/rmq-consumer's Tally.ts
       * exists to make safe.
       */
      let anonymous = 0;

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
              // An Envoy started without `--service-node` pushes stats it
              // cannot be credited with. Dropping the message is right — a
              // report with no replica identity would be counted as a second
              // vote from whoever wrote it last — but dropping it *quietly*
              // is how a replica goes missing from a quorum with nothing
              // said. Recorded here and reported by `poll`, on the fiber.
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
        const now = yield* Effect.clockWith((c) => c.currentTimeMillis);

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
            // Removed rather than skipped, so the departure is announced once
            // instead of being re-discovered every tick — and so a replica
            // that comes back registers as an arrival rather than as an entry
            // that was quietly there all along.
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
          // `observedAt` is the tick's clock, not the push's, for the same
          // reason the polling layer uses it: downstream ages a report against
          // the loop's own time, and mixing the two would make a report look
          // fresher or staler than the loop can justify.
          reports.push(...parseStats(replicaId, snapshot, now, (c) => known.has(c)));
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
