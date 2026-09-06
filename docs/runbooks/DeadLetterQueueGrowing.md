# DeadLetterQueueGrowing

`increase(rabbitmq_detailed_queue_messages{queue=~".*work.dead"}[15m]) > 0`
for 15m.

## What it means

Failed work is accumulating on `<apiId>.work.dead` and not being recovered.
Nothing is lost — the queue is durable and quorum — but a dead-letter queue
nobody drains is a slower way of losing the same messages.

## Check first

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages | grep work
docker compose logs rmq-daemon-0 rmq-daemon-1 | grep -i redriv | tail -5
curl -s aggregator:8088/api/state | head -c 200        # is the circuit closed?
```

## Causes

- **The circuit has not closed since the failures.** The redrive runs on the
  transition back to `CLOSED`; if the upstream is still failing, this alert is
  reporting the outage rather than a fault.
- **`REDRIVE_ON_CLOSE` is off.** It is off by default in code and on in
  `docker-compose.yml`, because whether stale work is still worth doing is a
  question about the workload.
- **The messages are not work.** Every queue dead-letters here, including the
  control and election queues, and the redrive replays only what came from the
  work queue — a poison control message is left where a human can find it, on
  purpose. Check `x-first-death-queue` on a sample.
- **They hit the delivery limit.** With `x-delivery-limit`, a message that
  fails its budget arrives with `reason: delivery_limit`. Three attempts per
  outage, and a redrive grants a fresh budget, so a genuinely poison message
  cycles once per recovery rather than looping.

## Resolution

If the circuit is closed and the queue is not draining, check that a daemon
holds the redrive election (`<apiId>.redrive-trigger` has exactly one active
consumer). Draining by hand is publishing the bodies back onto the work queue —
which is all the redrive does.
