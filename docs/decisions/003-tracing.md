# 003 — Distributed tracing, tail-sampled

**Status**: deferred 2026-09-06, implemented 2026-09-07.

## Decision

Trace the one path that crosses processes — producer publish → broker → daemon
call → Envoy — and leave state-over-time questions to metrics. Tracing is off
unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; the collector and Jaeger sit behind
compose's `tracing` profile.

## Why

Every failure found here was a loop that stopped or two numbers that stopped
agreeing, which metrics caught. What metrics cannot answer is "why did *this*
message take nine seconds". A head sampler keeping 5% answers that by luck, so
every process exports every span and `infra/otel-collector.yaml` decides once a
trace is complete: errors, anything over a second, anything requeued or
dead-lettered, and a 2% baseline.

## Gotchas

- `traceparent` rides as a message header, built with `Tracer.externalSpan`, so
  `@egress/rmq` has no OpenTelemetry dependency.
- The producer publishes a batch per tick under one `work.publish` span
  (`messaging.batch.message_count`), so one trace holds a tick's messages and
  each daemon's `work.call` beneath it.
- A plain callback running `Effect.runPromise` gets a fresh runtime with the
  no-op tracer: spans were made and went nowhere. Use `runPromiseWith` on a
  captured context.
- Import `@effect/opentelemetry/NodeSdk`, not the package root, which pulls a
  browser-only dependency.
- An empty endpoint means unset: compose interpolates `${VAR-}` to `""`.

## Open

A broken exporter is invisible: nothing alerts on the collector.
