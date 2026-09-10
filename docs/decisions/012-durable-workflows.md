# 012 — Durable workflows: evaluated, deliberately not adopted

**Status**: decided — not adopted. Revisit under the conditions at the end.
**Date**: 2026-09-10.
**Context**: `effect/unstable/workflow` ships in the runtime this repo already
pins, and every other module from `unstable` that fits has been taken up —
`http`, `observability`, and most recently `cli`. This one was evaluated the
same way and the answer came out differently.

## What it offers

Durable execution: a workflow is a deterministic sequence of `Activity`s whose
progress is persisted, so a process that dies mid-way resumes from the last
completed activity rather than starting again. Around that: `withCompensation`
for undoing a step whose successors failed, `DurableClock` for sleeping across
restarts, `DurableDeferred` and `DurableQueue` for waiting on things that
outlive a process, and an idempotency key per execution.

That is a genuinely good fit for a class of problem. It is not the class this
repo has.

## Why it does not fit here

**The unit of work is one activity.** A daemon takes a message and makes one
call to a third party. There is no second step to resume before, nothing to
compensate, and no partial progress to persist — the call either happened or it
did not. A workflow engine around a single activity is bookkeeping with no
question to answer.

**Durability is already the broker's, and that was a measured decision.** The
work queue is a quorum queue; `x-delivery-limit` counts attempts and
dead-letters at the budget. `WORK_DELIVERY_LIMIT`'s note in
`@egress/rmq/ControlPlane.ts` says why it must be the queue's job: *an
in-process counter is lost the moment the message moves to another daemon,
which is exactly what an outage makes happen.* A workflow engine would keep its
own execution state, so "has this work been done" would have two sources of
truth — the broker's unacked delivery and the engine's execution record — that
have to agree across a crash. Adding a second answer to that question is a
strange fix for a system whose whole point is producing one coherent answer.
[011](011-the-ceiling-belongs-to-the-broker.md) made the same call about the
concurrency ceiling and for the same reason.

**The core is a fold, not a sequence.** The aggregator's state machine is
superficially workflow-shaped — `CLOSED → DEGRADED → OPEN`, wait out a backoff,
`HALF_OPEN`, probe, `CLOSED` — and `DurableClock` is exactly the shape of the
OPEN backoff. But every one of those transitions is decided by *observations
arriving*, not by the process reaching the next step. `Breaker.step` folds an
unbounded stream of replica reports; it does not advance through a plan. A
workflow that suspends until its next step cannot express "recompute the verdict
every 250ms from whatever the fleet is currently saying".

The durability that machine does need — resuming a sequence across a failover
rather than restarting it at zero — already exists as `CheckpointStore`, fenced
against a shared lease token, and is exercised by a real two-container kill.

**The durable engine needs a database this stack does not have.** Measured, not
assumed: `WorkflowEngine.layerMemory` documents itself as *"not suitable for
production workflows that require durability"*, so real durability means
`ClusterWorkflowEngine.layer`, whose signature is

```
Layer<WorkflowEngine, never, Sharding.Sharding | MessageStorage>
```

and the only durable `MessageStorage` is `SqlMessageStorage` — the alternatives
are `layerNoop` and `layerMemory`. So adoption means the cluster runtime plus a
SQL database, in a system whose durability today is RabbitMQ and Redis. A third
stateful dependency is a large thing to add for a benefit the first two already
provide.

## What would change this

One thing, and it is specific: **work that is more than one step.**

If a message meant *charge the card, then write the ledger entry, then notify* —
three activities with real side effects — the broker's model breaks down.
Redelivery would replay steps that already succeeded, because an unacked
delivery says only "this message was not finished", never "it got as far as the
ledger". That is precisely the gap durable execution fills, and no amount of
`x-delivery-limit` closes it.

The demo's third party is deliberately a single call, so the repo has never had
that problem. If it grew one, this decision should be reopened rather than
worked around with a table of "already charged" ids — which is a workflow engine
with the durability left out.
