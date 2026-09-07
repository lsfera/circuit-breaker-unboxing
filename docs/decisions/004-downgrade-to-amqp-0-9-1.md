# 004 — Down to AMQP 0-9-1, on amqplib

**Status**: decided — supersedes [001](001-amqp-client.md).
**Date**: 2026-09-06.
**Context**: a review of the AMQP integration, prompted by the upstream state
of the client 001 chose to keep.

## The question

[001](001-amqp-client.md) decided to keep `rabbitmq-amqp-js-client` (AMQP 1.0,
RabbitMQ 4 native) and said what would reopen it:

> - The stranding bug reproducing on a workload we cannot restructure, or
>   dead-lettering proving unreliable in production the way it does after the
>   stranding test.
> - A measured comparison showing `amqplib` cancel-under-load is clean where
>   this client is not, at which point the reliability argument outweighs the
>   rewrite.

The second one is what happened, plus something 001 could not have known.

## What changed

**Upstream stopped.** No commit after 2026-06-25. Issue #96 — concurrent
`createPublisher` calls resolving with crossed links, ~20,000 messages
misrouted in the reporter's production over 4.5 hours — was filed on
2026-07-22 and has no maintainer response. That is the same defect
`packages/rmq/src/Client.ts` serialized every operation on the connection to
avoid, and our own reproduction covered `createConsumer` too, which the issue
does not.

**The credit window was never tunable.** PR #94, also unreviewed, exists to
expose `initialCredits` because the library hard-codes rhea's default of 1000.
That is why closing a probe consumer stranded so badly: the probe wanted one
message and the broker had been told it could send a thousand.

**Three workarounds here were calibrated to one build.** A connection-wide
semaphore, a two-connection topology per daemon, and an `uncaughtException`
filter for a rhea throw with no reachable listener.

## What was measured

The migration was done against the same integration suite, on a real
RabbitMQ 4 container, with the `RmqService` interface unchanged — every call
site compiled untouched.

Two tests failed, and both were the upgrade gates 001 put there on purpose:

```
✖ closing a consumer with deliveries in flight stalls the whole connection
    expected the shared connection to stall within 12 probe cycles;
    if this now survives, the client may have fixed it and the daemon's
    two-connection split can be revisited

✖ the work queue parks a message at the delivery limit, and a redrive
  republish grants a fresh budget
    the client reports deliveryCount 0 throughout: the counting is the
    broker's, which is the whole point
```

The stall does not happen on amqplib — twelve probe cycles against a
4,000-message backlog, on a shared connection, with a canary consumer that
stayed live throughout. And `x-delivery-count` is now readable: the same
budget test reads `[0, 1, 2, 3, 0, 1, 2, 3]`, the limit and then the redrive's
reset, visible rather than inferred from the parkings.

Both tests now assert the new behaviour. All ten pass.

End to end on the compose stack, one full incident:

```
seq=1 OUTLIER_EJECTION      DEGRADED   target=3/5
seq=2 ALL_ENDPOINTS_EJECTED OPEN       target=0/5  self=idle
seq=3 OPEN_TIMEOUT_ELAPSED  HALF_OPEN  target=1/5  daemon-2 elected prober
seq=4 PROBE_FAILED          OPEN       target=0/5
seq=5 OPEN_TIMEOUT_ELAPSED  HALF_OPEN  target=1/5
seq=6 PROBE_SUCCEEDED       CLOSED     target=1/5 -> 5/5
      daemon-2: redrive finished — 577 replayed (drained)
```

`gaps=0 dup=0` throughout, every queue drained to zero afterwards, and zero
errors across all eight containers.

## Why the decision goes this way

**The reliability argument won, as 001 said it would.** Channels are the
isolation the 1.0 client lacked: a consumer cancelled with deliveries
outstanding costs the broker a requeue and costs its neighbours nothing. The
bug that shaped the daemon's whole connection topology is not worked around,
it is absent.

**Flow control became expressible.** `prefetch` is an argument now. The
HALF_OPEN probe asks for exactly one message, which is what the state's
contract always said.

**The supply chain improved in three ways at once.** `amqplib` has zero
dependencies, ships its own types, and is maintained; the old client pulled
`rhea` from a git URL, which needed a `pnpm-workspace.yaml` override to build
at all. That override is gone.

**What it costs** is RabbitMQ 4's native protocol, which bought us nothing we
were using. Every property this repo depends on — quorum queues,
`x-delivery-limit`, `x-single-active-consumer`, dead-lettering with
`x-first-death-*`, durability — is a *broker* feature and works identically
over 0-9-1. That is the finding underneath this decision: nothing load-bearing
here was ever an AMQP 1.0 feature.

## Consequences

- `packages/rmq/src/Client.ts` is rewritten; `RmqService` is unchanged, so
  `AmqpControlPlaneSink`, the daemon fleet and the producer are untouched apart
  from the probe asking for `prefetch: 1`.
- The connection-wide semaphore is gone. There is no link to race.
- Both `transfer after detach` handlers are gone. amqplib surfaces failures as
  `error` events, which the client now listens for on the connection and on
  every channel; everything else stays fatal, as before.
- `deliveryCount` is real. The budget stays the broker's — an in-process
  counter still dies when a message moves to another daemon — but it is now
  observable rather than a blind spot.
- **The daemon's two-connection topology is gone**, in a follow-up change made
  immediately after this one so that a regression would be attributable to it.
  A daemon now holds one connection: four channels that live for the process
  (control consumer, two SAC election consumers, one publish channel) and up to
  two that churn — work, probe and redrive, of which at most two can coexist,
  because a probe excludes the other two and work and redrive only overlap
  while CLOSED. Closing a channel requeues everything it held, which is the
  property dropping a connection was being used for.

  Measured on the compose stack: **13 broker connections before, 8 after** —
  five daemons each losing one — with each daemon's connection carrying five
  channels at rest. Through a full incident (three HALF_OPEN probes, two
  failed, 539 messages redriven) the channel count returned to its steady
  value, so the churn leaks nothing.

  This is also what made `cancelConsumer` and `closeConsumer` separate
  operations: the probe cancels, because its channel has to outlive the
  cancellation long enough to settle the message it is holding, while
  `reconcile` closes, because the point there is to hand everything back.

### Publishing, after the fact

Two defects in the publish path turned up once the connection collapse put
every publish on one channel, and both are fixed here rather than left:

- **A channel with no recovery is a single point of failure.** amqplib closes a
  channel on any channel-level error, and publishing to an exchange that does
  not exist is enough. Without a reopen, one such error ended publishing from
  the whole process for good, silently — the send that caused it does not fail,
  because a plain publish is fire-and-forget. For a daemon that means no probe
  triggers, no redrive triggers and no replayed work, with a heartbeat still
  reporting health. The channel is now reopened on demand, one reopen at a
  time, and a test pins it.

- **`send` did not mean what the rest of the repo assumed.** It resolved when
  the frame reached a socket, not when the broker had the message.
  `Redrive.ts` publishes work back onto the work queue and only then acks it
  off the dead-letter queue, and calls that ordering a guarantee against loss —
  which it was not. The publish channel is a *confirm* channel now, so `send`
  waits for the broker.

  That surfaced as a flaky test rather than as an outage: the durability test
  publishes, restarts the broker, and expects the messages back. At HEAD it
  passed 4 runs out of 4; with the reopen in place it failed 3 in 5. The
  reopen did not break it — it perturbed the timing of a race that was always
  there, because nothing had ever waited for the broker to accept a message.
  With confirms it is 5 out of 5.

  Confirms cost a round trip per publish, and the producer paid it twenty times
  inside each 100ms tick: **195/s before, 165/s after**. Publishing the batch
  concurrently gets it back to **190/s**, which is what confirms are designed
  for — AMQP pipelines them, and a batch in flight at once is the ordinary way
  to use them. The ordering this repo actually guarantees is per-API on
  `circuit.control`, which the aggregator publishes one event at a time.

## What would change this

- amqplib going the way of the 1.0 client. It is one dependency and the same
  audit applies; the integration suite is what would tell us, since every
  property this repo relies on is pinned there against a real broker.
- Needing something 0-9-1 genuinely cannot express. Nothing in the current
  design qualifies — but AMQP 1.0 filters, or modified-outcome semantics, would.
