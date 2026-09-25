# 015 — The console at a thousand APIs

**Status**: built 2026-09-13 to 2026-09-18. The bandwidth targets below have
not been re-measured against the finished build.

## Problem

At 1,000 APIs × 10 replicas (`scripts/measure-console-stream.mjs`):

- `/api/stream` sent the whole 1.1 MB state every 400 ms — 2.77 MB/s per
  browser whether anything changed or not;
- each connection built its own frame on the control loop's event loop: at 100
  consoles, ticks fell from 3.69/s to 2.88/s and RSS rose to 1.2 GiB;
- the contract checker downloaded the console to read the event tape.

## Decision, in the order built

1. **The tape gets its own route**, `/api/events/stream`, with a 15 s
   keep-alive so an idle connection is not closed by a load balancer.
2. **One frame per interval for every connection**, through `PubSub.sliding(1)`
   — not `SubscriptionRef`, whose `changes` is unbounded and would queue frames
   for a slow browser. No replay: a stored frame could be an hour old.
   Measured: 100 consoles now cost the control loop nothing (3.69 ticks/s) and
   add 60 MiB, not 770.
3. **A view, not the fleet** (`?view=attention`): counts by state and the 50
   worst APIs, truncation stated.
4. **Snapshot, then patches**, each with an SSE `id`; `Last-Event-ID` resumes
   from the last 64 revisions or falls back to a snapshot. An id from the future
   (a restarted process) also falls back.
5. **gzip, applied by hand**, since `HttpMiddleware.compression` skips
   `Cache-Control: no-transform`, which the stream sets so proxies do not
   buffer it.

## Targets, per browser in a storm

2.80 MB/s → under 10 KB/s; connect 1.14 MB → under 50 KB. Projected from the
recorded stream, not measured on the build.

## Not done

Search, a per-API route, deltas on the default full-frame channel, and any
measurement of browser rendering cost.
