# 001 — Which AMQP client the daemon fleet uses

**Status**: superseded by [004](004-downgrade-to-amqp-0-9-1.md). Decided
2026-09-05: keep `rabbitmq-amqp-js-client` (AMQP 1.0).

## Decision

Stay on the AMQP 1.0 client despite three worked-around bugs, because the two
capabilities thought to require `amqplib` turned out not to depend on the client.

## Evidence

A spike against the same broker, both clients:

- **The redelivery budget is a queue property.** A quorum queue with
  `x-delivery-limit: 3` dead-lettered after four deliveries through the 1.0
  client, which reported `deliveryCount 0` every time. The broker counts;
  `amqplib` only makes the count visible (`x-delivery-count`).
- **Live credit reduction does not exist in 0-9-1 either.** `basic.qos` left an
  already-registered consumer's unacked count at 1 across two increases; only a
  re-registered consumer got the new prefetch. Same cancel-and-re-register in
  both protocols.

Not measured: whether cancelling a consumer under load is clean on `amqplib`.
That was the condition for reopening this, and it is what reopened it.
