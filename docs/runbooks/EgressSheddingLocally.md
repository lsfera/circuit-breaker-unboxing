# EgressSheddingLocally

`increase(envoy_http_adaptive_concurrency_gradient_controller_rq_blocked[5m]) > 0`
for 1m.

## What it means

The egress proxy is returning `503` to its own callers without asking the
upstream. The adaptive concurrency filter has decided current concurrency
exceeds what the measured round-trip time supports, and is shedding the excess.

This is the filter working as designed. The alert exists because of what the
shedding looks like from anywhere else in this system: identical to the third
party failing.

A daemon receives a plain `503`, counts
`egress_daemon_calls_total{outcome="failed"}`, requeues the message, and the
broker dead-letters it once the delivery budget is spent. The circuit stays
`CLOSED`, correctly, because outlier detection never saw a failed upstream
request — none was made. **Dead-lettered work plus a healthy circuit is the
signature of this alert, not of a contradiction.**

## Check first

```bash
docker compose logs envoy-00 --tail 50 | grep reached_concurrency_limit
curl -s envoy-00:9901/stats | grep adaptive_concurrency
curl -s envoy-00:9901/stats | grep -E 'cluster\.[^.]+\.upstream_rq_(2xx|5xx|timeout)'
```

The access log is the fast answer, and the reason it exists: `details` carries
`%RESPONSE_CODE_DETAILS%`, which is where Envoy says `reached_concurrency_limit`.
It is filtered to `status >= 400`, so anything it prints is worth reading.

`concurrency_limit` against `rq_blocked` says how hard the filter is squeezing.
The upstream counters say whether the third party is involved at all — during
this alert they should look fine, and if they do not, you have two problems.

## Causes

- **A genuine latency rise upstream.** The gradient controller lowers the limit
  when sampled RTT climbs against the measured minimum, so a slow third party
  produces shedding *before* it produces errors. This is the filter doing its
  job early.
- **A min-RTT recalculation window.** The filter periodically drops the limit to
  `min_concurrency` (4 here) to measure uncontended RTT. Bursty callers exceed 4
  easily. Note that this was *not* reproducible on demand — 181,429 requests at
  concurrency 64 across a full recalculation interval shed nothing — so treat it
  as a candidate, not a conclusion.
- **A real burst.** Five daemons at `MAX_IN_FLIGHT` each, plus the traffic
  generator, arriving together after a circuit closes.

## Resolution

Usually none: shedding is the intended response to contention, and the work is
requeued rather than lost. Act on it when the dead-letter queue is growing —
that is work being discarded because of *our* limiter rather than the upstream,
and the levers are `min_concurrency`, `concurrency_update_interval`, and the
daemon fleet's own `MAX_IN_FLIGHT`.

Do not respond by widening the breaker's thresholds. The breaker is not
involved, and it is not wrong.
