# ControlLoopStalled

`sum(rate(egress_aggregator_ticks_total[2m])) == 0` for 1m.

## What it means

No aggregator instance is iterating its control loop. Nothing is being polled,
nothing is being published, and every gauge is frozen at whatever it last held
— which is indistinguishable from a quiet system unless you look at this
counter. That is the entire reason the counter exists.

## Check first

```bash
curl -s aggregator:8088/livez    # 503 with lastTickAgoMs is this alert, confirmed
docker compose logs aggregator --tail 50 | grep -iE "fatal|defect|coordination"
```

`/livez` answers the same question in one request and says how stale the loop
is.

## Causes seen here, in order of likelihood

1. **The coordinator is unreachable and the call did not come back.** A
   one-sided partition once left the tick blocked on a Redis command that
   neither succeeded nor failed: two ticks in twenty-five seconds. Coordination
   calls are bounded at `COORDINATION_TIMEOUT_MS` (1s) now and the client is
   configured to fail rather than queue, so this should present as
   `NoLeaderElected` with errors climbing instead. If it presents as a stall,
   the bound has been removed or a new client is behind `RedisLike`.
2. **A defect killed the loop.** `Effect.repeat` terminates on a defect. The
   process logs `control loop died, restarting the process` and exits so the
   restart policy takes over; if it is up and stalled instead, that path was
   changed.
3. **The process is up but wedged** — event loop blocked by something
   synchronous. Rare here; check CPU.

## Resolution

Restarting the instance is safe and immediate: a new leader resumes from the
checkpoint, not from zero. `docker compose restart aggregator`. Then find out
which of the three it was, because 1 and 2 are bugs.
