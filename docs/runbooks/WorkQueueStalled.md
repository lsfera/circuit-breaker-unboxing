# WorkQueueStalled

`rabbitmq_detailed_queue_messages_ready > 0` and `rabbitmq_detailed_queue_consumers > 0`
and `rate(rabbitmq_detailed_queue_messages_acked_total[1m]) == 0` on a work queue,
for 2m.

## What it means

The broker holds work, the fleet has consumers attached, and nothing is being
settled. Every signal from inside a daemon can look normal: the heartbeat says
`CLOSED … self=ACTIVE`, the process is up, the circuit is closed. The daemons are
not refusing work; they are not being handed any.

This is read entirely from RabbitMQ, on purpose. An application metric cannot see
a consumer the broker has stopped delivering to, because from the application's
side nothing happened.

## What it looked like when it was found

After the host running the stack slept and woke, RabbitMQ closed the daemons'
consumer channels with `delivery acknowledgement on channel 1 timed out` (the
clock had jumped). The client rebuilt the consumers, the management API showed
five consumers `active: true` with prefetch 32 — and `payments-provider.work`
kept 52,621 messages ready and zero unacked for at least eight minutes.
Restarting the daemons drained it in under twenty seconds.

## Check first

```bash
curl -s -u guest:guest 'localhost:15672/api/queues/%2F/payments-provider.work?columns=messages_ready,messages_unacknowledged,consumers'
curl -s -u guest:guest localhost:15672/api/consumers | grep -o '"queue":{"name":"payments-provider.work"[^}]*}' | head
docker compose logs rabbitmq --since 15m | grep -iE 'timed out|channel error|precondition'
docker compose logs rmq-daemon --since 15m | grep -iE 'rebuilt|closed|error'
```

`messages_unacknowledged` at zero with consumers attached is the signature. If
unacked sits at prefetch × consumers instead, the fleet is working at its ceiling
and this alert should not be firing — check the rate window.

## Causes

- **Consumer channels closed by the broker and rebuilt.** A delivery
  acknowledgement timeout (including one caused by a clock jump), a protocol
  error, or a queue redeclared with different arguments. The client re-registers
  the consumer; whether the broker then delivers to it is exactly what this alert
  checks.
- **Every daemon holding its prefetch on calls that never finish.** Unacked
  would be non-zero; see above.

## Resolution

Restart the daemon fleet (`docker compose restart rmq-daemon`): fresh consumers
are delivered to immediately, and unsettled work returns to the queue rather
than being lost. Then find which channel closure started it in the broker log —
the restart recovers, it does not explain.
