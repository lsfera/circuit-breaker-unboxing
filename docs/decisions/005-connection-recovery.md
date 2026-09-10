# 005 — The client recovers its own connection

**Status**: decided — implemented.
**Date**: 2026-09-07.
**Context**: the review that produced
[the zombie finding](004-downgrade-to-amqp-0-9-1.md), which was fixed by making
a lost connection fatal. This replaces that with recovery, and keeps the
fatality as a backstop.

## The question

Restarting the broker under the fleet used to leave every process up, healthy
and consuming nothing. That was fixed by exiting on an unexpected close and
letting `restart: unless-stopped` rebuild the process — correct, and blunt: a
broker restart became a fleet restart, with every in-memory counter reset and
every container bounced.

amqplib 2.x ships reconnection (`connect(opts, { recovery })`). The question
was whether it is enough on its own.

## What it does, and what it does not

It reopens the socket. That is all.

`RecoveringCore.createChannel()` creates a channel on whatever connection is
current; nothing tracks the channels already handed out. A `Channel` belongs to
the connection that died, consumers are not re-registered, and no queue or
binding is redeclared. Enabling `recovery` and stopping there would produce a
process that is *connected* and consuming nothing — the same zombie as before,
now with a healthy-looking socket to hide behind.

The hook that makes it work is `setup`, which amqplib runs after each
successful connect and **before** the connection is handed to anyone. What goes
in it is the application's problem.

## The decision

`@egress/rmq` records what it was asked to build and rebuilds it in `setup`:

- **Topology first.** Every `declareQueue`, `declareTopicExchange` and `bind`
  is recorded, deduplicated, and replayed in the order it was first made. This
  is not optional: each daemon's control queue is `durable: false` and does not
  survive a broker restart, so a consumer rebuilt against it would fail
  `NOT_FOUND`.
- **The publish channel next**, so a send during recovery has somewhere to go.
- **Consumers last**, each on its own channel with its own prefetch.

`Consumer` handles are **mutated in place** rather than replaced, because
`@egress/rmq-consumer` keeps them in `Ref`s and compares them by identity — a
replaced handle would make `reconcile` believe a live consumer was gone.
Cancelling or closing a consumer removes it from the live set, so a recovery
never resurrects one the daemon deliberately retired.

Recovery is **bounded**: 60 attempts, 200ms growing to a 5s ceiling, roughly
five minutes. On `reconnect-failed` the process exits, which is the previous
decision kept as a backstop — a process that cannot reach its broker for five
minutes has nothing a restart would lose, and something the platform should
know about.

## What was measured

**Connection killed from the broker** (`rabbitmqctl close_all_connections`),
pinned as an integration test:

```
[rmq] disconnected (320 CONNECTION-FORCED ... "recovery test") — recovering
[rmq] reconnect attempt 1 in 215ms
[rmq] reconnected — 1 topology entries and 1 consumer(s) restored
```

The consumer registered before the kill delivered the message published after
it.

**Full broker restart under the running stack** — the case that produced the
zombie:

```
[rmq] disconnected (320 CONNECTION-FORCED ... reason 'shutdown') — recovering
[rmq] reconnect attempt 1 in 213ms
... 4 attempts, backing off ...
[rmq] reconnected — 7 topology entries and 4 consumer(s) restored
```

Every container stayed up, `calls ok` resumed from 3191 rather than from zero —
which is the proof the process never restarted — and the broker showed all
consumers reattached, including `payments-provider.control.daemon-0`, a
transient queue the restart had destroyed and topology replay recreated. Zero
fatal exits across the fleet.

A full incident afterwards behaved normally: `OPEN`, two failed probes,
`PROBE_SUCCEEDED`, `CLOSED`, 19,359 messages redriven and drained, `gaps=0
dup=0`, every queue empty.

## Consequences

- A broker restart is no longer a fleet restart. In-memory counters, the
  delivery-contract tracker and the ramp state survive it.
- The client is now stateful about topology. That is a real cost: it holds a
  record of every declare for the life of the connection, and a caller that
  declared queues in a loop would grow it. Bounded here — declares happen at
  startup — and deduplicated, but it is a thing to know.
- `Consumer` is mutable. Documented at the type, because it is the one piece of
  mutability in that file that a reader would otherwise be right to object to.
- The crash-fast stance survives where it belongs: unrecoverable means exit.

## What would change this

- Needing recovery to be observable as a *metric* rather than only a log line.
  A daemon reconnecting is a daemon not consuming, and right now the only
  signals are the log and the absence of work — the same shape of gap that made
  the original zombie hard to see. A gauge for "connected" is the obvious next
  thing, and it belongs to whichever component owns the alert.
- A workload that declares topology dynamically, at which point the recorded
  list needs an eviction story.

## What it still did not cover: one channel

Added 2026-09-08, from a review pass over `Client.ts`.

The section above is about the connection. Every consumer here has its own
channel, and a channel can die on its own — a protocol error, a queue deleted
underneath it, a settle on a tag the broker has already seen. amqplib does not
recover channels, and the connection stays healthy, so nothing fires: not the
`disconnect` handler, not `setup`, not the restart policy. The `Consumer` handle
the caller holds still looks live, and `@egress/rmq-consumer` keeps it in a
`Ref` and reconciles against it, so the daemon believes it is consuming.

Measured against a real broker by closing a consumer's channel out from under
it: the next message was never delivered, and nothing in the process said
anything. That is the deaf-daemon failure this repo has an alert and a runbook
for, reachable without any connection loss at all.

A consumer whose channel closes while the client still considers it live is now
rebuilt on a fresh channel. The same `live`/`forget` bookkeeping that keeps a
deliberately retired consumer from being resurrected keeps it from being rebuilt
here, and closing the connection forgets all of them at once, so a deliberate
shutdown is covered by the mechanism that was already there rather than by a
second one.

**The repair is deliberately unbounded**, and the first version was not. It
carried a budget of five attempts, reset by a delivery, on the reasoning that a
channel the broker keeps rejecting should stop rather than spin. Measured, that
reasoning was backwards. Every failure seen here either stops itself — a queue
that is gone makes `attach` reject, and a rejection leaves no channel to close
again — or makes progress. What the budget did instead was punish the queues
that are *idle by design*: the two SAC election queues exist to hold registered,
empty candidates, so a budget reset by deliveries never reset on them. Six
channel deaths over the life of a process and that daemon left the election
permanently, over a condition the next rebuild fixed immediately. A test pins
that case now. If a genuine spin ever turns up, the answer is a delay, not a
limit on how often a consumer may be repaired.

The `connected` flag survives in exactly one place — deciding whether a *failed*
rebuild is worth reporting. It is not usable as a guard before the attempt: a
channel's `close` arrives before the connection's `disconnect`, so it still says
`true` while a rebuild is being decided, and only says `false` by the time that
rebuild's attach rejects, which is where it is read.

A broker-initiated cancel (`message === null`, the queue was deleted) cannot be
rebuilt and is now logged instead of returned from in silence.

