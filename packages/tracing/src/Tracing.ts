// The submodule, not the package root. The root re-exports WebSdk, which
// imports @opentelemetry/sdk-trace-web — a browser package that has no place
// in a Node image, and whose absence is a crash at import time rather than a
// missing feature.
import { Option as O } from "effect";
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import {
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
} from "@opentelemetry/sdk-trace-base";

/**
 * Tracing for every process here — see docs/decisions/003-tracing.md.
 *
 * Opt-in, on `OTEL_EXPORTER_OTLP_ENDPOINT`. Without it no tracer is installed,
 * Effect's no-op one stays, and `Effect.withSpan` costs nothing; a stack running
 * without a collector must not degrade.
 *
 * Sampling is decided at the *tail*, in infra/otel-collector.yaml: this exports
 * everything, so the traces that turned out to be slow or failed can be the ones
 * kept. A head sampler would answer "why did this message take nine seconds" by
 * luck. `OTEL_TRACES_SAMPLER_ARG` stays for deployments with no collector, where
 * dropping at the source is the only option.
 */

/**
 * Empty counts as unset. docker-compose interpolates an unset host variable to
 * the empty string rather than omitting it, so a `=== undefined` check here
 * would enable an exporter pointed at nowhere.
 */
const endpoint = (): O.Option<string> => {
  const raw = process.env["OTEL_EXPORTER_OTLP_ENDPOINT"]?.trim();
  return raw === undefined || raw === "" ? O.none() : O.some(raw);
};

/**
 * Head sampling ratio, defaulting to 1 — export everything and let the tail
 * decide. Lower it only where there is no collector to decide at the tail,
 * because anything dropped here can never be reconsidered.
 */
const ratio = () => {
  const raw = Number(process.env["OTEL_TRACES_SAMPLER_ARG"] ?? "1");
  return Number.isFinite(raw) && raw >= 0 && raw <= 1 ? raw : 1;
};

/**
 * Install tracing for one service, or nothing at all.
 *
 * `serviceName` is what separates the processes in a trace view — the five
 * daemons deliberately share one, because "which daemon" is an attribute of a
 * span rather than a different service, and a fleet that renames itself per
 * replica is unreadable at ten.
 */
export const TracingLive = (serviceName: string) =>
  O.match(endpoint(), {
    // `layerEmpty` provides the resource and installs no tracer, so Effect's
    // default no-op one stays and `Effect.withSpan` costs nothing. Both
    // branches have the same type on purpose — a caller should not have to
    // know which one it got.
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
          sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(ratio()) }),
        },
      })),
  });
