# 011 — The concurrency ceiling belongs to the broker

**Status**: decided 2026-09-07. Extended by the adaptive limiter (below).

## Decision

No in-process gate. The work consumer asks for `prefetch: MAX_IN_FLIGHT`, so
every delivery a daemon holds is a call in progress and everything else stays
in the queue.

## Why

The gate (32) sat under a prefetch of 100. Draining a backlog:

| | peak unacked | working | waiting in a JS array |
|---|---|---|---|
| prefetch 100, gate 32 | 500 | 160 | 340 |
| prefetch 32, no gate | 160 | 160 | 0 |

Throughput was identical (3,640 messages in 0.27 s). The 340 made queue depth
lie and were held hostage by a daemon that might be paused. Backpressure is
settlement timing: a delivery settles when its handler resolves, so a slow
daemon stops acking and the broker stops pushing.

## The adaptive limit

A daemon also keeps an AIMD limit below the prefetch (`Limiter.ts`): ×0.7 on a
`429`, +1/limit per success, never above `MAX_IN_FLIGHT`. It uses
`Semaphore.resize`, which does not wait for permits already out. Against a
third party capped at 2 calls per endpoint, it drew 20 `429`s a second where a
fixed limit drew 341.
