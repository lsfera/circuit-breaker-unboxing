# 009 — What the quorum is a quorum of

**Status**: decided — the arithmetic is unchanged on purpose; the inputs to it
are no longer silent.
**Date**: 2026-09-07.
**Context**: a review pass over the ingestion path, prompted by reading
`EnvoyPushSource.ts`. Its module doc already names this hazard exactly — and
closes only one of the three ways into it.

## The property

Every state this system publishes comes from one line in `Breaker.step`:

```ts
const downFrac = votes.DOWN / live.length;
```

`live.length` is the replicas that reported within `replicaTimeoutMs`. There is
no floor. So a replica leaving the fleet does not make the verdict *smaller* —
it changes what the verdict **means**, at the same number. Three replicas
reporting means a 0.6 quorum needs two to agree. One replica reporting means
that replica is unanimous by itself, and the fleet-wide guarantee this repo
exists to provide has become a single instance's opinion, published with the
same confidence and the same event contract.

`EnvoyPushSource.ts` said this in its own words, about the fan-out case:

> Point that cluster at two aggregators and Envoy load-balances the stream, so
> each instance sees *some* replicas — a quorum computed from a partial fleet,
> which is worse than no data because it looks like data.

That is right, and the config fix it describes (one stats sink per aggregator)
closes exactly one cause. The hazard is the shrinking denominator, not the
load balancer.

## The three ways a replica left in silence

All three found by reading, all three now counted on
`egress_fleet_replica_lost_total{reason}` and logged once per departure:

- **`no-node-id`** — an Envoy pushing without `--service-node` sends stats that
  cannot be attributed to a replica. Discarding them is correct: a report with
  no identity would be filed under whoever wrote that slot last, which is a
  second vote from one replica. But it was discarded *per message*, silently,
  at the flush interval — a replica that is up, connected, and pushing four
  times a second was simply not in the fleet, and nothing anywhere said so.
- **`went-quiet`** — a stream that stops. The snapshot expired after `staleMs`
  and was skipped on every subsequent poll, its entry left in the map forever.
  Correct behaviour, no announcement.
- **`unreachable`** — the polling path's `Effect.catch(() => [])`. The comment
  above it is right that one unreachable replica must not fail the whole poll.
  It is the "and say nothing" part that was wrong.

## What is deliberately not changed

**The arithmetic.** The obvious fix — count replicas that have gone quiet in
the denominator until they are properly retired, so a partial fleet cannot
reach quorum — is not obviously right, and would trade one failure for another.

If two of three Envoy replicas die and the survivor sees the upstream as DOWN,
the current rule opens the circuit (1/1 = 100%). The "fixed" rule would not
(1/3 = 33%), and it would be wrong to hold closed: the traffic those two dead
replicas carried is gone with them, and the survivor is reporting on the
traffic that is actually flowing. Suppressing a legitimate `OPEN` during a
partial fleet failure is a worse outcome than publishing one from a small
quorum, because the failure it hides is the one this system exists to catch.

There is no rule that is right in both cases without knowing *why* the fleet
shrank, and the push path deliberately does not know: "a replica this process
has never been told about still reports, because it is the one doing the
talking" is the property that makes it better than polling. It cannot
distinguish a replica that died from a replica that was never deployed.

So the decision is to leave the judgement where it is and make the input to it
loud: **the denominator may move, and when it does, somebody is told.**
`egress_circuit_reporting_replicas` has existed for a while and nothing watched
it; `FleetShrunk` watches the departures now, with
[a runbook](../runbooks/FleetShrunk.md) whose first job is to say what the
gauge means before the next transition is trusted.

## What would change this

An expected fleet size the aggregator can trust — from service discovery, from
an operator-set `--expect-replicas`, or from the deployment itself. With that,
a floor becomes expressible without guessing: "fewer than N reporting means the
verdict is not authoritative, and here is a state that says so." Without it,
any floor this process invents is a number it made up about a fleet it cannot
see.
