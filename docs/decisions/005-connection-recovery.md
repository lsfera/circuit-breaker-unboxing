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

One boolean for one log line invites deleting it, so here is what that costs,
measured on a client holding the four consumers a daemon really has. Every
broker restart then logs:

```
[rmq] disconnected (…CONNECTION-FORCED…) — recovering
[rmq] consumer on payments.work: repair failed — Connection closed
[rmq] consumer on payments.control.daemon-0: repair failed — Connection closed
[rmq] consumer on payments.probe-trigger: repair failed — Connection closed
[rmq] consumer on payments.redrive-trigger: repair failed — Connection closed
[rmq] reconnected — 4 topology entries and 4 consumer(s) restored
```

Four warnings per process, around thirty across this fleet, each announcing a
failure that the next line contradicts. There is nothing to query instead:
amqplib's `Connection` is `{ serverProperties }` and exposes no state, so the
alternatives are matching on the text of a library's error or keeping the flag.
The flag stays.

A broker-initiated cancel (`message === null`, the queue was deleted) cannot be
rebuilt and is now logged instead of returned from in silence.


## Where those lines go

Added 2026-09-10. The transcripts above are verbatim, and their `[rmq] …` lines
were `console.warn`. Every one of them now goes through the process's logger
instead, so they carry a level and a timestamp and reach whatever sink that
logger has:

```
[13:55:44.350] WARN (#16): [rmq] consumer channel on idle-election… closed — rebuilt
```

The transcripts are left as they were recorded rather than reformatted, because
a measurement rewritten after the fact is not one.

The reason it was `console.warn` is the same reason
[003](003-tracing.md) gives for a span that reached nothing: these run in
amqplib event handlers, off any fiber, and the bare `Effect.run*` entry points
build a fresh runtime with *default* services. `@egress/rmq-consumer` had
already captured its context for exactly this; the client had not. Measured
before changing it — a bare `runFork(logWarning(...))` never reaches a provided
`Logger`, it goes to the default one.

One line stays on `console.error`, and says so: `connectionLost` is the last
thing the process does before `process.exit(1)`, which will not wait for a
logger that batches or writes asynchronously.

## Fatal, but not by `process.exit`

Added 2026-09-10. The stance above is unchanged: past `maxRetries` a process
that cannot reach its broker is no use, and stopping hands it to
`restart: unless-stopped`. What changed is who does the stopping.

`@egress/rmq` called `process.exit(1)` from inside the client. That is exactly
what [008](008-configuration-is-a-boundary.md) removed from `@egress/config`,
for a reason that applies here word for word: fail-fast is right, and owning
the process's fate from inside a library module is not. It also had an escape
hatch, `onLost`, whose comment said "tests that deliberately take a broker away
override it" — no caller anywhere passed it, including those tests.

The service exposes `lost: Effect<never, RmqError>` instead: never completes
while the connection is usable, fails once recovery gives up. `launchWithRmq`
builds a layer graph and blocks on it, so a lost connection ends the program
the way every other failure does.

It has to be the launching fiber that observes this, and that was measured
rather than assumed: a defect in a fiber forked into the layer's scope does
**not** end `Layer.launch` — a probe with a 1200 ms timeout ran the full 1200 ms
either way. Hence one helper rather than a rule each `main.ts` has to remember.

Verified end to end: with the broker stopped, the daemon retries on schedule
(`[rmq] reconnect attempt 38 in 5423ms`, logged through the logger), and past
the budget the process stops for the restart policy to pick up.

`@egress/aggregator` is deliberately not converted. Its `Rmq` is optional —
`--rmq` may be absent — and lives inside the control-plane sink's own layer
rather than the top-level graph, so `launchWithRmq` does not fit it without
reshaping how that sink is provided. Its remaining `process.exit(1)` is in a
`main.ts`, which is a composition root and the one place that may legitimately
end a process.

### The aggregator, a day later

Added 2026-09-11, correcting the paragraph above rather than leaving it to be
read as current. Both of its claims are now false.

`@egress/aggregator` **is** converted, just not through `launchWithRmq` — the
reason that helper does not fit still stands. Its `Rmq` is sealed inside the
control-plane sink's layer, so nothing outside could observe `lost`, and
leaving it unobserved is what made a permanently dead broker silent: the
instance kept serving 200s while the daemon fleet stopped hearing about state
changes. The sink now forks `Effect.catch(rmq.lost, …)` into its own scope,
which logs fatal and completes a `Fatal` deferred that `main.ts` blocks on with
`Deferred.await(fatal)`. Same stance, same fail-fast, reached by the shape that
suits an optional dependency.

There is also no `process.exit` left to point at. `grep -rn "process.exit"
packages/*/src` returns five hits and every one is a comment describing what
used to be there. The two fatal paths — a lost control plane and a failed
startup — both complete that one deferred, so the program ends the way every
other failure does.

### Control queues are durable now

Added 2026-09-13. The topology bullet above says each control queue is
`durable: false`. RabbitMQ 4.3 refuses a transient queue that is not exclusive
by closing the connection, so the control and floor queues are durable classic
queues with `x-expires` — see `packages/rmq/src/ControlPlane.ts`. Topology
replay is still required: a queue that expired during a long outage, or a
broker whose data was lost, has to be declared again before a consumer can
attach to it.

That upgrade also found a gap in this recovery. If `setup` fails because the
broker closes the connection partway through the replay, amqplib emits `error`
on the inner connection it is rebuilding. Nothing listens there, so the process
exits instead of scheduling another attempt. The restart policy brings it back,
but a server-initiated close during the rebuild window is a restart, not a
reconnect.

**Fixed 2026-09-24** (article 4). amqplib 2.0.1 binds its own `error` listener
only after `setup` resolves, and emits `error` for a missed heartbeat, a socket
error or a fatal close, though not for a broker's `CONNECTION_FORCED`. The
client's setup hook now adds a listener first, so the replay's pending calls
reject, `connect-failed` fires, and recovery schedules another attempt.
`test/integration/ReplayClose.test.ts` reproduces it: the broker closes every
connection, then suspends the ones that reopen 15 ms in, so the replay stalls
and the 1 s heartbeat fails inside `setup`. Without the listener the test fails
with `Unexpected close` thrown uncaught and the consuming connection never
returns. With it, the connection logs the error, fails that attempt, and
reconnects with all 400 topology entries restored.

### A silent, deterministic reconnect, and a fiber `launchWithRmq` still couldn't see

Added 2026-09-14, chasing the same symptom three ways: a daemon that looked
healthy and consumed nothing after a queue-arg mismatch, in each case with no
signal that anything was wrong.

**The socket had no timeout.** `amqp.connect()` reused its options on every
reconnect, and none of them bounded the underlying `net.connect`. A one-sided
network partition — outbound packets to the broker silently dropped, not
refused or reset — falls back to Linux's own `tcp_syn_retries`: measured at
~135s per attempt against a real one. "Roughly five minutes" above assumes 60
attempts that each fail fast; three at 135s each already blows past it, and
the full budget would have taken over two hours before `reconnect-failed` said
anything — indistinguishable from hung to anyone watching less than that.
Fixed with a 5s `timeout` on the connection options (`packages/rmq/src/Client.ts`),
the same order of magnitude as `maxDelay`, so a partition now fails each
attempt fast enough that the five-minute budget is the real bound again.

**Nothing listened for a failed attempt, only the last one.** `connect-failed`
fires on every failed reconnect, not just the one that exhausts the budget —
measured against a real broker, a queue redeclared with different arguments
retried in total silence for 21 attempts and 84 seconds before
`reconnect-failed` said anything at all. `Client.ts` now logs every attempt,
and reads the numeric AMQP reply code amqplib stamps on a deterministic
rejection (406 `PRECONDITION_FAILED` and the like) to tell "the broker is
actively saying no" from "the broker is not there right now": the former
abandons the retry budget immediately, on attempt 1, rather than retrying a
setup that is exactly as doomed on attempt 60 — see the churn on
`x-delivery-limit` in [016](016-the-retry-budget-travels-with-the-message.md)
for one way a redeclare drifts.

**A third instance of the gap "Fatal, but not by `process.exit`" already
describes.** That section's finding — a defect in a fiber forked into the
layer's scope does not end `Layer.launch` — was fixed there for `Rmq.lost`
and, a day later, for the aggregator's control-plane sink. It had not been
applied to the daemon's and producer's own long-running loop
(`runDaemon` / `runProducer`), each forked with
`Effect.forkScoped(Effect.orDie(...))` in its `main.ts` and never observed
again. This failure mode was worse than a slow reconnect: a startup failure in
that fiber — the same queue-arg mismatch, this time hit by `runDaemon`'s own
`declareQueue` on first connect, never a reconnect at all — is a defect
nothing was waiting on, so the process hung forever instead of exiting for the
restart policy to find.

Fixed by extending `launchWithRmq` to take a second effect to race against
`Rmq.lost` (`Effect.raceFirst`), and having each `main.ts` catch its loop's
defect and fail a local `Fatal` deferred into that race — the same
`Fatal`-deferred shape the aggregator already uses, reached here because
`launchWithRmq` has an `Rmq` to race against and the aggregator's sink does
not. Verified against a real broker: a work queue declared by hand with a
mismatched argument, then a daemon started against it directly, now logs
`FATAL: daemon died` and exits in well under a second, instead of hanging.

