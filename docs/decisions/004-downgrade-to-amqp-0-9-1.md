# 004 — Down to AMQP 0-9-1, on amqplib

**Status**: decided 2026-09-06; supersedes [001](001-amqp-client.md).

## Decision

`packages/rmq` runs on `amqplib`. `RmqService` kept its interface, so no call
site changed.

## Why

- **The 1.0 client was abandoned** with an open bug routing concurrent links to
  the wrong target (~20,000 misrouted messages in the reporter's production),
  and a hard-coded credit window of 1,000 that made closing a probe consumer
  strand deliveries.
- **The stall did not reproduce on amqplib.** Twelve probe cycles against a
  4,000-message backlog on a shared connection, with a canary consumer live
  throughout. Channels are the isolation the 1.0 client lacked.
- **Nothing load-bearing was an AMQP 1.0 feature.** Quorum queues, delivery
  limits, single-active-consumer and dead-lettering are broker features and
  behave the same over 0-9-1.
- `prefetch` is an argument, so the `HALF_OPEN` probe asks for exactly one.

## Consequences

- The connection-wide semaphore and the two-connection-per-daemon workaround
  went. (A second connection came back later for a different reason — see
  [018](018-control-flow-as-expressions.md).)
- The publish channel is a **confirm channel**: `send` resolves when the broker
  has the message, which is what publish-before-ack in the redrive assumes.
  Confirms pipeline when messages are published back to back (165/s serial,
  190/s pipelined, against 195/s unconfirmed). `sendBatch` does that for a
  batch: every publish issued in order, then all confirms awaited together.
  The producer sends one batch per 100 ms tick.
- A channel closed by a channel-level error is reopened on demand; otherwise one
  bad publish ended publishing for the process, silently.
- amqplib does not reconnect: see [005](005-connection-recovery.md).
