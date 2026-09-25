# 005 — The client recovers its own connection

**Status**: decided 2026-09-07; amended through 2026-09-24.

## Decision

amqplib's `recovery` only reopens the socket. `@egress/rmq` records every
declare, bind and consumer, and rebuilds them in `setup`, which runs after each
connect and before the connection is handed out: topology first (in the order it
was declared), then the publish channel, then consumers.

## Invariants

- **Consumer handles are mutated in place**, because callers compare them by
  identity; a cancelled or closed consumer leaves the live set, so recovery
  never resurrects one deliberately retired.
- **A single channel can die with the connection healthy**, and nothing fires.
  A live consumer whose channel closes is rebuilt, **without a retry budget**:
  a budget reset by deliveries never resets on the idle election queues, and
  six channel deaths had evicted a daemon from its election for good.
- **Recovery is bounded** (60 attempts, 200 ms to 5 s) with a 5 s socket
  timeout, since a one-sided partition otherwise waits out `tcp_syn_retries`
  (~135 s per attempt). A deterministic rejection (406 and similar) gives up on
  the first attempt.
- **Giving up is a failure, not `process.exit`.** The service exposes
  `lost: Effect<never, RmqError>`; `launchWithRmq` races it against the
  process's main loop, because a defect in a fiber forked into a layer's scope
  does not end `Layer.launch` (measured).
- **Listen for `error` before `setup`.** amqplib 2.0.1 binds its own listener
  only after `setup` resolves, so a missed heartbeat during topology replay
  crashed the process. `test/integration/ReplayClose.test.ts` reproduces it.
- Log through the captured runtime, not a bare `Effect.run*`, which uses the
  default logger.

## Evidence

A full broker restart under the running stack: every container stayed up, call
counters continued rather than resetting, a transient queue the restart
destroyed was recreated, and a following incident ran clean.

## Costs

The client holds a record of every declare for the life of the connection —
bounded because declares happen at startup. Recovery is visible only in logs.
