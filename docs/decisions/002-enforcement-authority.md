# 002 — The aggregator's `OPEN` is observational, not authoritative

**Status**: decided 2026-09-05.

## Decision

The aggregator publishes its verdict and never pushes configuration to Envoy.

## Why

Envoy already enforces locally, immediately, and whether or not anything else
here is running. Making the aggregator authoritative would put it in-band for
every request: its slowness or absence would become traffic failures instead of
reporting gaps. The stream is for what no replica can do alone — tell a
*consumer* of the API that the API as a whole is out, so it can stop work at the
source.

## Consequences

- No xDS push path, no consistency protocol between the verdict and running
  proxy config.
- Losing the aggregator costs visibility and the fleet's compensating actions,
  not enforcement. High availability therefore targets continuity of the event
  stream (lease, fencing, checkpoints), not failover speed.
- Subscribers own their reaction; the repo owes them a contract they can check.

## What would flip it

- A subscriber that cannot be trusted to react in time.
- A requirement that egress stop fleet-wide within a bounded time. Local
  ejection cannot promise it: a replica not calling the API has nothing to eject.
- A contractual limit on calls during an incident — an obligation advice cannot
  discharge.
