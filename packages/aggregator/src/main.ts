import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { createServer } from "node:http";
import { Aggregator, AggregatorLayer } from "./Aggregator.ts";
import { EventBusLayer, NoopSinkLayer, WebhookSinkLayer } from "./Events.ts";
import { EnvoyFleetLayer, SimFleetLayer } from "./FleetSource.ts";
import { HttpLive } from "./Http.ts";
import { Config, defaultConfig } from "@egress/domain/Model.ts";
import type { ApiSpec } from "./FleetSource.ts";

const args = new Map<string, string>();
for (const arg of process.argv.slice(2)) {
  const [k, v = "true"] = arg.replace(/^--/, "").split("=");
  if (k) args.set(k, v);
}

const PORT = Number(args.get("port") ?? 8088);
const MODE = args.get("source") ?? "sim";
const REPLICAS = Number(args.get("replicas") ?? 5);

const APIS: ReadonlyArray<ApiSpec> = [
  { apiId: "payments-provider", endpoints: 6, rps: 900, failureRate: 0 },
  { apiId: "shipping-rates", endpoints: 4, rps: 300, failureRate: 0 },
  { apiId: "tax-calc", endpoints: 3, rps: 120, failureRate: 0 },
];

const FleetLayer =
  MODE === "sim"
    ? SimFleetLayer(APIS, REPLICAS)
    : EnvoyFleetLayer(
        (args.get("envoy") ?? "http://127.0.0.1:9901")
          .split(",")
          .map((adminUrl, i) => ({
            replicaId: `envoy-${String(i).padStart(2, "0")}`,
            adminUrl: adminUrl.trim(),
          })),
        APIS,
      );

const SinkLayer = args.has("no-webhook")
  ? NoopSinkLayer
  : WebhookSinkLayer(`http://127.0.0.1:${PORT}/subscriber/webhook`);

/**
 * The dependency graph, declared once. Layer.provideMerge keeps FleetSource,
 * EventBus and EventSink in the output context because the HTTP routes read
 * them directly.
 */
const AppLayer = HttpLive.pipe(
  Layer.provideMerge(AggregatorLayer),
  Layer.provideMerge(
    Layer.mergeAll(FleetLayer, EventBusLayer, SinkLayer),
  ),
  Layer.provide(Layer.succeed(Config, defaultConfig)),
);

/**
 * The aggregator loop runs as a scoped fiber for the lifetime of the server.
 * Interruption is structural: when the server layer shuts down, the scope
 * closes and the loop stops. No interval handle to remember.
 */
const AggregatorDaemon = Layer.effectDiscard(
  Effect.gen(function* () {
    const agg = yield* Aggregator;
    yield* Effect.forkScoped(agg.run);
    yield* Effect.log(
      `egress circuit breaker console  source=${MODE}` +
        (MODE === "sim" ? ` replicas=${REPLICAS}` : "") +
        `  http://localhost:${PORT}`,
    );
  }),
);

const MainLayer = HttpRouter.serve(
  Layer.provideMerge(AggregatorDaemon, AppLayer),
).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port: PORT })));

NodeRuntime.runMain(Layer.launch(MainLayer));
