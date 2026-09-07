// The submodule, not the package root. The root re-exports WebSdk, which
// imports @opentelemetry/sdk-trace-web — a browser package that has no place
// in a Node image, and whose absence is a crash at import time rather than a
// missing feature.
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import {
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
} from "@opentelemetry/sdk-trace-base";

/**
 * Tracing for every process in this repo, in one place.
 *
 * [docs/decisions/003-tracing.md](../../../docs/decisions/003-tracing.md)
 * deferred this deliberately, and named what doing it properly would take: an
 * exporter, a collector to send to, a sampling decision, and span naming that
 * survives the AMQP boundary. This is that, and the fourth item is the one
 * with any substance — see `@egress/rmq`'s `traceparent` handling.
 *
 * ## Opt-in
 *
 * Tracing is opt-in, and the opt is `OTEL_EXPORTER_OTLP_ENDPOINT`. Without it
 * the layer installs no tracer, Effect's default no-op one stays, and
 * `Effect.withSpan` costs nothing because it does not make a span. That
 * matters more here than it usually would: every failure this repo has
 * actually had was a state-over-time failure that metrics and a heartbeat
 * surfaced, so tracing earns its place on one specific path rather than
 * everywhere — and a stack that has to run without a collector must not
 * degrade when there isn't one.
 *
 * ## Where the sampling decision belongs
 *
 * The fleet moves 200 messages a second and every one crosses four processes,
 * so something has to be dropped. The question is what, and *when* it is
 * decided.
 *
 * Deciding at the head — here, at the producer, on a ratio — is cheap and
 * blind. It keeps a random slice and throws the rest away before anything is
 * known about it, which is precisely wrong for the question
 * docs/decisions/003-tracing.md said tracing would exist to answer: "why did
 * *this* message take nine seconds". A head sampler answers that by luck, one
 * time in twenty.
 *
 * Deciding at the tail — in the collector, once the trace is complete — can
 * keep the traces that turned out to matter: the ones that errored, the ones
 * that were slow, a small sample of the ordinary. It costs more, because every
 * span has to be exported before it can be judged, and the collector has to
 * hold a trace until it is whole.
 *
 * So this exports everything by default and lets
 * [infra/otel-collector.yaml](../../../infra/otel-collector.yaml) decide, which
 * is the arrangement that answers the question. `OTEL_TRACES_SAMPLER_ARG` is
 * still here and still `ParentBased`, for a deployment that has no collector
 * and has to drop spans at the source instead — the cheap, blind option, kept
 * because it is sometimes the only one available.
 */

/**
 * Empty counts as unset. docker-compose interpolates an unset host variable to
 * the empty string rather than omitting it, so a `=== undefined` check here
 * would enable an exporter pointed at nowhere.
 */
const endpoint = () => {
  const raw = process.env["OTEL_EXPORTER_OTLP_ENDPOINT"];
  return raw === undefined || raw.trim() === "" ? undefined : raw.trim();
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
export const TracingLive = (serviceName: string) => {
  const url = endpoint();
  // `layerEmpty` provides the resource and installs no tracer, so Effect's
  // default no-op one stays and `Effect.withSpan` costs nothing. Both branches
  // have the same type on purpose — a caller should not have to know which one
  // it got.
  if (url === undefined) return NodeSdk.layerEmpty;
  return NodeSdk.layer(() => ({
    resource: { serviceName },
    // Batched rather than simple: a span per message at this rate would put an
    // HTTP round trip on the path this is supposed to be measuring.
    spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter({ url: `${url}/v1/traces` })),
    tracerConfig: {
      sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(ratio()) }),
    },
  }));
};
