# 009 — What the quorum is a quorum of

**Status**: decided 2026-09-07. The arithmetic is unchanged; its inputs are no
longer silent.

## The property

`Breaker.step` computes `votes.DOWN / live.length` over replicas that reported
recently, with no floor. A replica leaving does not shrink the verdict, it
changes what it means: at one replica left, that replica is unanimous.

## Decision

Keep the arithmetic, and announce every departure on
`egress_fleet_replica_lost_total{reason}`, watched by `FleetShrunk`:

- `no-node-id` — an Envoy pushing without `--service-node`; its stats cannot be
  attributed, and were discarded per message in silence;
- `went-quiet` — a push stream that stopped;
- `unreachable` — a replica the poller could not reach.

## Why not count missing replicas in the denominator

If two of three replicas die and the survivor sees the upstream down, today's
rule opens (1/1); the "fixed" rule holds closed (1/3), hiding a real outage on
the traffic that is still flowing. No rule is right both ways without knowing
why the fleet shrank, and the push source cannot tell a dead replica from one
never deployed.

## What would change it

An expected fleet size the aggregator can trust (service discovery, an operator
setting). Then "fewer than N reporting" can be published as not authoritative.
