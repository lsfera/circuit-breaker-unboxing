# 010 — A proxy that fails on its own behalf

**Status**: decided 2026-09-07; amended 2026-09-13 (shed with `429`).

## Finding

With a fully healthy upstream (47,807 requests, all 2xx), every Envoy replica
had still returned 5xx downstream, each count equal to its
`adaptive_concurrency…rq_blocked`. Every error the proxy served was its own.

A daemon saw a shed request as a plain `503`, counted a failed attempt, and
eventually dead-lettered it, while the circuit correctly stayed `CLOSED`
because no upstream request was made. Under the chaos harness with +300 ms
upstream latency this was **90,753** local 503s and **22,226** dead letters
with the circuit closed throughout.

## Decision

- **Shed with `429`** (`concurrency_limit_exceeded_status: TooManyRequests`).
  A daemon releases a `429` after 100–400 ms of jitter, uncounted, and backs off
  its own limit; any other failure spends an attempt.
- **Log what the proxy generates**: an access log filtered to status ≥ 400,
  carrying `%RESPONSE_CODE_DETAILS%` and `%RESPONSE_FLAGS%`. At ~570 rps a full
  log is noise; these lines are rare.
- `EgressSheddingLocally` alerts on the counter.
- The filter and its parameters stay: shedding under contention is its job.

## Unexplained

The trigger for the original 95 sheds did not reproduce (181,429 requests at
concurrency 64 across a min-RTT recalculation produced none), and
`min_rtt_msecs` read 0 on every replica during the 2026-09-13 incident. Both
are open.
