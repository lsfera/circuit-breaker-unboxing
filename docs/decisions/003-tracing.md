# 003 — Distributed tracing: available, deliberately not wired

**Status**: deferred, with the gate checked.
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
