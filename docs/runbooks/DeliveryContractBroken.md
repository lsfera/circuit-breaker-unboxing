# DeliveryContractBroken

Any increase in `egress_subscriber_gaps_total`, `egress_subscriber_duplicates_total`,
`egress_daemon_control_gaps_total` or `egress_daemon_control_duplicates_total`.

## What it means

A per-API sequence was skipped or reused, observed from outside the process
that published it — on the webhook path, the AMQP path, or both. This is the
one guarantee the whole system exists to provide.

## Check first

```bash
curl -s aggregator:8088/api/subscriber | head -c 400   # gaps list names the jump
docker compose logs rmq-daemon --tail 40 | grep -E "gaps=|dup="
```

The gap list says exactly which API jumped and from where to where, which is
usually enough to identify the incident that caused it.

## Causes, and how to tell them apart

- **A leadership handoff that resumed from stale state.** The signature is
  duplicates, not gaps: an instance republishing numbers a later leader already
  used. Losing the lease drops the in-memory registry for this reason, so a
  re-promoted instance takes the rehydrate path.
- **A counter that was read across a restart.** Not a violation. These are
  per-process, in-memory counters: a restarted instance starts at zero, so
  comparing totals across a restart can show a decrease and a "negative" event
  count. Read one instance, before and after — the chaos harness had to learn
  this.
- **At-least-once delivery from the outbox.** A replayed event carries the same
  `idempotency-key`; a subscriber that does not dedupe will count a duplicate
  where the stream had none. Check `egress_webhook_outbox_replayed_total`.

## Resolution

If it is a genuine republish, the sequence is compromised for that API until
the next `state_changed` and subscribers should be told. Snapshots are exempt
from the rule and safe to re-apply, so a subscriber that reconciles on
snapshots recovers on its own within `snapshotMs`.
