# 010 — A proxy that fails on its own behalf

**Status**: decided — the listener records its own errors now; the shedding
itself is left alone, and the reason is below.
**Date**: 2026-09-07.
**Context**: a review pass over `infra/envoy/envoy.yaml`.

## What was measured

On a fleet with a completely healthy upstream — `cluster.payments-provider`
showing 47,807 requests, **all 2xx**, zero timeouts, zero pending/connection/
retry overflow, 6 of 6 hosts healthy — all three Envoy replicas had returned
5xx downstream anyway:

| replica | `downstream_rq_5xx` | `adaptive_concurrency…rq_blocked` |
| --- | --- | --- |
| envoy-00 | 19 | 19 |
| envoy-01 | 6 | 6 |
| envoy-02 | 70 | 70 |

Equal on every replica. Every error this proxy has served was manufactured by
the proxy, and none of it came from the third party it exists to protect.

## Why that matters more here than in most systems

This repo's entire product is *one coherent signal about a third party*. A
request shed by the adaptive concurrency filter arrives at a daemon as a plain
`503`, and the daemon does what it must: counts
`egress_daemon_calls_total{outcome="failed"}`, requeues, and lets the broker
dead-letter the message once its delivery budget is spent. Meanwhile the
circuit stays `CLOSED` — correctly, because outlier detection never saw a
failed upstream request, because no upstream request was made.

So the two halves of this system disagree about the same event: work is dying
and the breaker says the API is healthy. Both are right. Nothing reconciles
them, and nothing could, because the daemon has no way to tell the two apart —
Envoy reports `reached_concurrency_limit` in `%RESPONSE_CODE_DETAILS%`, which
is an access-log field and never a response header. From the client's side a
shed request and a third-party failure are byte-for-byte the same.

## What was found underneath it

The listener had **no access log at all**. Outlier ejections are logged
(`cluster_manager.outlier_detection.event_log_path`), upstream behaviour is in
the stats, but a response the proxy generated itself left no trace of why. The
95 errors above were discoverable only because two counters happened to match.

That is the gap that got fixed: `access_log` on the HTTP connection manager,
filtered to `status_code >= 400`, carrying `%RESPONSE_CODE_DETAILS%` and
`%RESPONSE_FLAGS%`. At ~570 rps a full access log is noise nobody reads; the
responses worth keeping are rare enough to be free. Verified against a running
replica: an unrouted request produced exactly one line —
`{"status":404,"details":"route_not_found","flags":"NR",…}` — and sixty seconds
of live traffic produced nothing else.

## What is deliberately not changed, and what stayed unexplained

**The filter stays, and its parameters stay.** Shedding under contention is
what an adaptive concurrency limiter is *for*, and `min_concurrency: 4` during
a min-RTT recalculation is the documented mechanism, not a misconfiguration.

**The trigger was not identified, and this is recorded rather than guessed
at.** The obvious hypothesis — that the every-30s min-RTT recalculation window
sheds whatever exceeds `min_concurrency` — did not reproduce: 181,429 requests
at concurrency 64 across a full recalculation interval, against a replica
restarted for the purpose, produced **zero** blocks and zero 5xx. Whatever
produced those 95 was not steady-state load and not merely the recalculation
timer. The honest position is that the misattribution path is proven and its
trigger is not, so the fix is the one that holds either way: make the proxy say
what it did.

## Consequences

An operator seeing dead-lettered work and a `CLOSED` circuit now has somewhere
to look, and the answer is one `grep` on the proxy's own log rather than an
inference from two counters agreeing. `EgressSheddingLocally` watches the
counter directly, because a proxy generating its own failures is exactly this
repo's alerting criterion: something that looks fine from outside.

## Amendment — 2026-09-13

The gap this ADR recorded — a shed request and a third-party failure are
byte-for-byte the same to a daemon, because Envoy only says which in an
access-log field the daemon never sees — is closed. The adaptive concurrency
filter now sheds with `429` (`concurrency_limit_exceeded_status: TooManyRequests`
in `infra/envoy/envoy.yaml`), not the platform default `503`, and a daemon
tells the two apart by status code alone. A `429` is held for 100–400ms of
jitter and released back to the broker uncounted (RabbitMQ 4.3's requeuing
`nack`, which does not spend `x-delivery-limit` — see
[ADR 016](016-the-retry-budget-travels-with-the-message.md)); anything else
still counts as a failed attempt. Shedding no longer dead-letters healthy
work, which was the consequence this ADR described but left standing.

**The measurement that forced it.** Chaos harness `infra/chaos-load.mjs`, low
profile (200 msg/s base, spikes to 3,000/s), fault `net-upstream-latency`
(+300ms ±50ms Envoy → payments endpoints): between 14:06 and 14:08 UTC the
three Envoys served **90,753** local `503 reached_concurrency_limit` on
`/payments` while the upstream returned no errors — the same shape as the
95 errors above, three orders of magnitude larger under sustained load.
`adaptive_concurrency.gradient_controller.min_rtt_msecs` read **0** on all
three replicas throughout. Under the pre-amendment `503` behaviour, **22,226**
messages were dead-lettered with the circuit `CLOSED` the entire time — the
disagreement this ADR named, now with a number on it.

**What is still unchanged.** The filter stays, and its parameters stay — this
ADR's original position holds. `min_rtt_msecs` reading 0 across all three
replicas during the incident is noted, not explained: it was not
investigated as part of this amendment, and remains, like the original
trigger below, a candidate rather than a conclusion.
