// The submodule: the package root pulls a browser-only dependency and crashes at import.
import { Config, Effect, Layer, Option as O, Schema } from "effect";
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import {
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
} from "@opentelemetry/sdk-trace-base";

/**
 * Opt-in on `OTEL_EXPORTER_OTLP_ENDPOINT`; otherwise Effect's no-op tracer stays.
 * Everything is exported and sampled at the tail by the collector (ADR 003).
 */

/** Empty is unset: compose interpolates an unset variable to "". */
const endpoint = Config.String("OTEL_EXPORTER_OTLP_ENDPOINT").pipe(
  Config.map((raw) => raw.trim()),
  Config.option,
  Config.map(O.filter((raw) => raw !== "")),
);

/** Head sampling, 1 by default; lower only where no collector samples the tail. */
const ratio = Config.schema(
  Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  "OTEL_TRACES_SAMPLER_ARG",
).pipe(Config.withDefault(1));

/** Daemons share one service name; which daemon is a span attribute. */
export const TracingLive = (serviceName: string) =>
  Layer.unwrap(
    Effect.map(Effect.all([endpoint, ratio]), ([url, sampleRatio]) =>
      O.match(url, {
        onNone: () => NodeSdk.layerEmpty,
        onSome: (url) =>
          NodeSdk.layer(() => ({
            resource: { serviceName },
            // Batched rather than simple: a span per message at this rate would
            // put an HTTP round trip on the path this is supposed to be measuring.
            spanProcessor: new BatchSpanProcessor(
              new OTLPTraceExporter({ url: `${url}/v1/traces` }),
            ),
            tracerConfig: {
              sampler: new ParentBasedSampler({
                root: new TraceIdRatioBasedSampler(sampleRatio),
              }),
            },
          })),
      }),
    ),
  );
