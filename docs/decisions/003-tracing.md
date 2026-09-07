# 003 — Distributed tracing: available, deliberately not wired

**Status**: **implemented, 2026-09-07.** The reasoning below is why it was
deferred, and it still describes what tracing is and is not for here; what
changed is that the one trace-shaped path now has traces. See
[what was built](#what-was-built) at the end.
**Date**: 2026-09-06.
**Context**: Phase 9 of `.claude/plans/production-readiness.md`, which made
tracing conditional on the package existing for this repo's pinned Effect.

## The gate

It exists. `@effect/opentelemetry` publishes `4.0.0-rc.112`, the exact version
`pnpm-workspace.yaml` pins for `effect` and `@effect/platform-node`. So the
reason not to do it cannot be "we cannot".

## The decision

Not wired, for now, and this record exists so that stays a decision rather than
an omission.

## Why

Every failure this repo has actually had was a *state over time* failure, and
metrics plus a heartbeat log are what surfaced them:

- a control loop that died while the process kept serving 200s — found by a
  counter that stopped moving, and now by `/livez`;
- a daemon that went deaf to the control plane while looking perfectly healthy
  — found by two gauges that normally track each other;
- a lease that was never handed back — found by reading the code, then
  measured with a stopwatch;
- a coordination call that hung instead of failing — found by a tick counter
  that stopped and a liveness endpoint that said so.

None of those is trace-shaped. A trace answers "where did this request spend
its time", and the questions here have been "is this loop still running" and
"do these two numbers still agree".

## Where it would earn its place

One path in this system genuinely is trace-shaped: a message arriving on
`<apiId>.work`, the daemon's third-party call through the egress listener, the
resulting Envoy stats, and the circuit event that comes back. That crosses four
processes and two protocols, and per-message latency across it is not derivable
from any counter here.

Doing it properly means: `@effect/opentelemetry` with an OTLP exporter in the
aggregator and the daemons, a collector or Jaeger in `docker-compose.yml`, a
sampling decision (a fleet at 200 msg/s cannot trace everything), and span
naming that survives the AMQP boundary — the daemon would have to carry a trace
context as an application property, next to the ones the redrive already
stamps. That is a phase, not a flag.

## What would move it

- A latency question this system cannot answer: "why did *this* message take
  nine seconds" rather than "what is the p99".
- A second consumer of the event stream, at which point the interesting path
  stops being one this repo can see end to end from its own metrics.

---

## What was built

Everything this record said it would take, and the fourth item was the one with
substance.

**An exporter and a collector.** `@egress/tracing` installs the Node SDK when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set and nothing at all when it is not — so the
default stack traces nothing and pays nothing. `docker compose up` starts no
tracing services; they sit behind a `tracing` profile:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
  docker compose --profile tracing up -d
# Jaeger at http://localhost:16686
```

**Sampling, at the tail rather than the head.** This is the part worth arguing
about. A head sampler keeping 5% keeps a *random* 5%, and the question this
record said tracing exists to answer — "why did this message take nine
seconds" — is answered by luck one time in twenty. So every process exports
every span and [infra/otel-collector.yaml](../../infra/otel-collector.yaml)
decides once each trace is complete: errors in full, anything over a second in
full, anything the daemon requeued or dead-lettered in full, and a 2% baseline
of the ordinary so there is something to compare against. Head sampling is
still available (`OTEL_TRACES_SAMPLER_ARG`) for a deployment with no collector,
where dropping at the source is the only option.

**Span naming that survives the AMQP boundary.** A `traceparent` rides as an
ordinary message header, injected from whatever span the publisher is in and
exposed to the consumer as `delivery.parent`. It lives in `@egress/rmq`, not in
`@egress/tracing`, so the dependency edge stays honest: publishing a message
does not require an OpenTelemetry stack, and the AMQP client has no OTel
dependency at all — Effect's own `Tracer.externalSpan` is enough.

Verified on the running stack: ten of ten sampled traces contained both
`work.publish` (rmq-producer) and `work.call` (rmq-daemon), joined across the
broker. Through an incident, 54 `work.call` spans were kept out of tens of
thousands of messages — 14 of them carrying `egress.settlement=requeue`, which
is the tail policy doing exactly what it exists for.

### What it cost to get right

Two things, both worth writing down.

`Effect.runPromise` builds a fresh runtime with default services, and the
daemon's work handler is a plain callback that has to start one. The span it
made was therefore created against the default no-op tracer: the `traceparent`
was on the wire, the daemon made a span, and nothing reached the collector.
`Effect.runPromiseWith` and a captured context fixed it. A span that costs
something and goes nowhere is the worst of both, and nothing in the types says
so.

The Dockerfile lists workspace manifests one per line, so a new package
installs none of its dependencies and fails at runtime with
`ERR_MODULE_NOT_FOUND` rather than at build time. Also: importing
`@effect/opentelemetry` at the package root pulls `WebSdk`, which needs a
browser-only package; importing `@effect/opentelemetry/NodeSdk` does not.

### Still open

Tracing is visible only when someone opens Jaeger. There is no alert on it and
no metric derived from it, which is consistent with what this record says
tracing is for here — the state-over-time questions stay with metrics — but it
does mean a broken exporter is invisible. The collector's own telemetry is the
place to catch that, and it is not wired to Prometheus.
