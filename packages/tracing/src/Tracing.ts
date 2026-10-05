import { Config, Effect, Layer, Option as O, Schema, Tracer } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpExporter, OtlpSerialization, OtlpTracer } from "effect/observability";

/**
 * Tracing for every process here. It is off unless the process was started with `--telemetry` or
 * `EXPOSE_TELEMETRY=true`, and an OTLP endpoint is configured. Without both, no tracer is installed, Effect's
 * no-op one stays, and `Effect.withSpan` costs nothing. Sampling is left to a collector at the tail, so this
 * exports everything; `OTEL_TRACES_SAMPLER_ARG` is for deployments with no collector.
 *
 * Effect's own OTLP exporter over `fetch`, not the OpenTelemetry JS SDK: nothing here names a runtime.
 */

/** A variable that is unset or empty. Empty counts as unset: docker-compose interpolates an unset host variable to the empty string, so a present-but-empty endpoint would install an exporter pointed at nowhere. */
const nonEmpty = (name: string) =>
  Config.String(name).pipe(
    Config.map((raw) => raw.trim()),
    Config.option,
    Config.map(O.filter((raw) => raw !== ""))
  );

/**
 * Where spans go, as OpenTelemetry defines it: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` as given, else
 * `OTEL_EXPORTER_OTLP_ENDPOINT` with the traces path added. Each is read on its own, so an empty or missing first
 * one falls through to the second rather than ending the search.
 */
export const tracingEndpoint = Config.all([
  nonEmpty("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
  nonEmpty("OTEL_EXPORTER_OTLP_ENDPOINT")
]).pipe(
  Config.map(([traces, base]) => O.orElse(traces, () => O.map(base, (url) => `${url.replace(/\/+$/, "")}/v1/traces`)))
);

const enabled = Config.Boolean("EXPOSE_TELEMETRY").pipe(
  Config.orElse(() => Config.Boolean("TELEMETRY")),
  Config.withDefault(false)
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
 * A root span is kept with probability `ratio`; a child follows its parent, local or carried in from a
 * `traceparent`, which Effect already does by marking the child of an unsampled span unsampled. The decision is
 * the root's alone, so a trace is exported whole or not at all.
 */
const sampledAt = (ratio: number) => (tracer: Tracer.Tracer): Tracer.Tracer =>
  Tracer.make({
    span: (options) =>
      tracer.span({ ...options, sampled: options.sampled && (O.isSome(options.parent) || Math.random() < ratio) }),
    context: tracer.context
  });

/**
 * Install tracing for one service, or nothing at all. `serviceName` separates processes in a trace view; the
 * five daemons share one, because "which daemon" is a span attribute, not a different service.
 */
export const TracingLive = (serviceName: string) =>
  Layer.unwrap(
    Effect.map(Effect.all([enabled, tracingEndpoint, ratio]), ([exposeTelemetry, url, sampleRatio]) =>
      exposeTelemetry
        ? O.match(url, {
          // No tracer is installed, so Effect's no-op one stays.
          onNone: () => Layer.empty,
          onSome: (url) =>
            // Batched (every 5s, or 1000 spans): a span per message at this rate would put an HTTP round trip on
            // the path this is supposed to be measuring.
            Layer.effect(
              Tracer.Tracer,
              Effect.map(
                OtlpTracer.make({ url, resource: { serviceName } }),
                sampledAt(sampleRatio)
              )
            ).pipe(Layer.provide([OtlpExporter.layerFlusher, OtlpSerialization.layerJson, FetchHttpClient.layer]))
        })
        : Layer.empty)
  );
