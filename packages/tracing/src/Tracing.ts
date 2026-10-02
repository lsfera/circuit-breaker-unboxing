// The submodule, not the package root: the root re-exports WebSdk, which imports a browser-only package and
// crashes a Node image at import time.
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { BatchSpanProcessor, ParentBasedSampler, TraceIdRatioBasedSampler } from "@opentelemetry/sdk-trace-base";
import { Config, Effect, Layer, Option as O, Schema } from "effect";

/**
 * Tracing for every process here. Opt-in on `OTEL_EXPORTER_OTLP_ENDPOINT`: without it no tracer is installed,
 * Effect's no-op one stays, and `Effect.withSpan` costs nothing. Sampling is left to a collector at the tail, so
 * this exports everything; `OTEL_TRACES_SAMPLER_ARG` is for deployments with no collector.
 */

/** Empty counts as unset: docker-compose interpolates an unset host variable to the empty string, so a present-but-empty endpoint would install an exporter pointed at nowhere. */
const endpoint = Config.String("OTEL_EXPORTER_OTLP_ENDPOINT").pipe(
  Config.map((raw) => raw.trim()),
  Config.option,
  Config.map(O.filter((raw) => raw !== ""))
);

/**
 * Head sampling ratio, default 1 (export everything, let the tail decide). Anything dropped here can never be
 * reconsidered, so lower it only where there is no collector.
 */
const ratio = Config.schema(
  Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  "OTEL_TRACES_SAMPLER_ARG"
).pipe(Config.withDefault(1));

/**
 * Install tracing for one service, or nothing at all. `serviceName` separates processes in a trace view; the
 * five daemons share one, because "which daemon" is a span attribute, not a different service.
 */
export const TracingLive = (serviceName: string) =>
  Layer.unwrap(
    Effect.map(Effect.all([endpoint, ratio]), ([url, sampleRatio]) =>
      O.match(url, {
        // `layerEmpty` provides the resource and installs no tracer, so Effect's no-op one stays. Both branches have the
        // same type on purpose: a caller should not have to know which one it got.
        onNone: () => NodeSdk.layerEmpty,
        onSome: (url) =>
          NodeSdk.layer(() => ({
            resource: { serviceName },
            // Batched rather than simple: a span per message at this rate would
            // put an HTTP round trip on the path this is supposed to be measuring.
            spanProcessor: new BatchSpanProcessor(
              new OTLPTraceExporter({ url: `${url}/v1/traces` })
            ),
            tracerConfig: {
              sampler: new ParentBasedSampler({
                root: new TraceIdRatioBasedSampler(sampleRatio)
              })
            }
          }))
      }))
  );
