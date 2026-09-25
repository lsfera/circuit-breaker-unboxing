# 017 — Detecting a lost control plane inside the lease

**Status**: decided 2026-09-17; simplified 2026-09-18 to a tuned protocol
heartbeat.

## Problem

On `net-control-partition+outage`, a leader whose broker link was cut kept
`egress_aggregator_is_leader=1` for several seconds after the standby took
over. No split-brain write happened — sequences stayed monotonic, nothing
delivered twice — but the gauge said two leaders.

## What was tried

1. `CONSECUTIVE_FAILURE_THRESHOLD` 3 → 2: its own comment claimed ≈6.3 s was
   inside the 5 s lease. Narrowed the window to 3 s.
2. A message-level heartbeat on the shared confirm channel produced **5
   duplicate control events** under a 9,000/s spike: a real confirm queued
   behind heartbeats timed out, was retried, and the original also landed. On
   its own channel it was clean, but the window stayed at ~4 s.
3. The cause was elsewhere: a delivery already in its retry chain never re-read
   the failure streak. Checking it on every attempt closed the window to 0 s.
4. Then the same 0 s with **AMQP's own heartbeat at 1 s** (it had been the
   default 60 s), and none of the bespoke heartbeat.

## Decision

- `heartbeat: 1` on every connection.
- `AmqpControlPlaneSink.publish` fails fast on `rmq.isConnected` on every
  attempt, and on the confirm-failure streak only on a **retry** — checked on a
  first attempt, a stale streak would block the delivery that could prove
  recovery.

## Open

A broker that answers heartbeats but stops confirming (a resource alarm) is
caught only by the confirm-failure streak; that fault was not tested.
