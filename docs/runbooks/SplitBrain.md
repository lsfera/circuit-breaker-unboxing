# SplitBrain

`sum(egress_aggregator_is_leader) > 1` for 30s.

## What it means

Two instances believe they hold the publishing lease. This should be
impossible: the lease is held with a TTL and fenced with an epoch-qualified
token, and a checkpoint write from a stale token is rejected.

Treat it as a correctness incident, not a capacity one. Two publishers means
two instances issuing sequence numbers for the same API, which is the exact
guarantee this system exists to provide.

## Check first

```bash
for a in aggregator aggregator-2; do
  curl -s $a:8088/metrics | grep -E "^egress_aggregator_(is_leader|fencing_conflicts_total)"
done
docker compose exec redis redis-cli get egress:aggregator:leader:holder
docker compose exec redis redis-cli get egress:aggregator:leader:epoch
```

`egress_aggregator_fencing_conflicts_total` moving means fencing is doing its
job: the stale writer is being rejected, and the damage is bounded to what it
published before the rejection.

## Causes

- Clock or TTL misconfiguration: a lease TTL shorter than the time a tick takes
  lets a slow leader lose the lease it thinks it holds.
- A coordinator that lost its state and reissued tokens from the beginning.
  The epoch in the token exists precisely for this; if this alert fires
  *without* fencing conflicts, check that tokens still carry an epoch.
- Two deployments pointed at different Redis instances but the same
  subscribers.

## Resolution

Stop one instance immediately — the one *not* holding the Redis lease key.
Then check `/api/subscriber` for gaps and duplicates before deciding whether
subscribers need to be told.
