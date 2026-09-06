# NoLeaderElected

`absent(egress_aggregator_is_leader) == 1 or sum(egress_aggregator_is_leader) == 0`
for 2m.

## What it means

Instances are running, but none can confirm it holds the lease, so nothing is
publishing. Standing down is the *correct* response to an unreachable
coordinator — an instance that cannot confirm leadership must not act as leader
— so this alert is about the duration, not the behaviour.

The `absent()` half is not decoration. The gauge is written after an acquire
attempt returns, so an instance that has never reached the coordinator once
published no series at all, and `sum(...) == 0` cannot fire on a metric that
does not exist. Found by partitioning one instance from Redis and watching the
gauge vanish rather than drop.

## Check first

```bash
curl -s aggregator:8088/metrics | grep -E "coordination_errors_total|ticks_total|is_leader"
docker compose logs aggregator --tail 30 | grep -i coordination
docker compose exec redis redis-cli ping
```

Errors climbing at roughly the tick rate means the loop is alive and standing
down, which is the healthy failure. Errors *not* climbing while ticks are also
flat is [ControlLoopStalled](ControlLoopStalled.md) wearing a different label.

## Causes

- Redis down, unreachable, or out of memory.
- A one-sided partition: one instance blind, the other fine. The blind one
  stands down, the other should be leading — if neither is, both are blind.
- Every instance genuinely gone (the `absent()` case).

## Resolution

Restore the coordinator. Recovery needs no intervention: the next tick that
reaches Redis acquires the lease and resumes from the checkpoint. Nothing is
lost while this is firing except the events that were not published, and those
sequences were never issued, so subscribers see no gap.
