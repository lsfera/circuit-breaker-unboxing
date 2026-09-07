# FleetShrunk

`increase(egress_fleet_replica_lost_total[10m]) > 0` for 1m.

## What it means

A replica stopped contributing to the fleet's verdict. Not that it is
*failing* — that it is no longer being counted at all.

That distinction is the whole reason this alert exists. Every state this system
publishes comes from a fraction:

```
downFrac = votes.DOWN / live.length
```

`live.length` is however many replicas reported within `replicaTimeoutMs`. A
replica leaving does not make the number smaller — it makes the number *mean*
less, at the same value. With three replicas reporting, a 0.6 quorum needs two
of them to agree. With one, that one replica is unanimous by itself, and the
fleet-wide guarantee this repo is about has quietly become one instance's
opinion.

The `reason` label says how it went:

| `reason` | What happened |
| --- | --- |
| `unreachable` | The polling path could not reach a replica's admin port, or the poll timed out. |
| `went-quiet` | The push path heard nothing from a replica for `staleMs`, so its last stats expired. |
| `no-node-id` | An Envoy is pushing stats with no node identifier — almost always a replica started without `--service-node`. Its stats cannot be attributed, so it is not in the fleet at all. |

## Check first

```bash
curl -s 'prometheus:9090/api/v1/query?query=egress_circuit_reporting_replicas'
docker compose logs aggregator --tail 30 | grep -E 'stopped (answering|pushing)|node id'
docker compose ps envoy-00 envoy-01 envoy-02
```

The gauge is the denominator itself, per API. Compare it against how many
replicas you expect to be running — the aggregator cannot do that comparison
for you on the push path, which is the point of the third row above: a replica
it has never been told about is indistinguishable from one that does not exist.

## Causes

- **A replica genuinely went away** — a crash, a rolling deploy, a scale-down.
  Expected during a deploy; the alert clears ten minutes after the last
  departure.
- **A replica is up but unattributable.** `no-node-id` means Envoy is pushing
  and the aggregator is discarding, which looks from the outside exactly like a
  replica that is not there. Check `--service-node` on that replica.
- **The stats sink is pointed at one aggregator instead of one per instance.**
  Envoy load-balances a gRPC cluster, so a single sink naming a cluster with
  two aggregators in it gives each of them *some* replicas — both compute a
  quorum from a partial fleet and neither knows it. See
  `infra/envoy/envoy.yaml`: there is one `stats_sinks` entry per aggregator,
  deliberately.
- **The aggregator lost network reachability to the admin ports** (polling
  path only). The circuit itself may be perfectly healthy.

## Resolution

Restore the replica, or confirm the shrink was intended. Nothing needs
replaying: reports are current-state, not a stream, so a returning replica is
counted again on its next push or poll and logs `is answering again`.

While the fleet is short, treat transitions with suspicion — particularly an
`OPEN` published while `egress_circuit_reporting_replicas` is 1. That is a
correct application of the rule to a fleet that is not there.

## What this alert is not

It is not an error budget, and it does not fire on a replica *voting* DOWN —
that is the circuit breaker working. It fires on a replica ceasing to vote at
all, which is the case the arithmetic cannot see.
