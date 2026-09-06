# DeliverySLOFastBurn / DeliverySLOSlowBurn

Multi-window burn rate against a 99.9% delivery objective:
14.4x over 5m *and* 1h (fast, critical), 6x over 30m *and* 6h (slow, warning).

## What it means

Events the sink was asked to deliver are being dead-lettered at a rate that
will exhaust a month's error budget in two days (fast) or five (slow). Both
windows have to be burning, which is what separates a real degradation from a
blip.

This is *not* the same as events being lost. What failed is in the durable
outbox and will be replayed by whichever instance holds the lease, in order,
once the subscriber recovers.

## Check first

```bash
curl -s aggregator:8088/metrics | grep -E "outbox_depth|outbox_replayed_total|webhook_(failed|dead_lettered)_total"
curl -s aggregator:8088/api/subscriber | head -c 200
```

`egress_webhook_outbox_depth` is the number to watch: zero in every healthy
minute, and a value that keeps climbing means the subscriber is still refusing.

## Causes

- The subscriber is down, slow, or returning non-2xx. The sink retries three
  times with exponential backoff and a 2s timeout before the outbox takes it.
- The subscriber is *hanging* rather than refusing, which costs a full timeout
  per attempt and shows up as latency in
  `egress_webhook_delivery_duration_ms` before it shows up here.

## Resolution

Fix the subscriber; nothing needs doing on this side. Watch
`egress_webhook_outbox_replayed_total` move and depth return to zero. If depth
approaches `OUTBOX_MAX_PER_API` (500), the oldest entries start being dropped
and `egress_webhook_outbox_dropped_total` moves — at that point the subscriber
will see a gap it can detect, and it is worth telling them.
