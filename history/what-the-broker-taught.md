# What the broker taught

A record of what building the RabbitMQ control plane surfaced: what was
verified and how, the client bugs it exposed, and the live runs that proved the
fleet behaves. Written as it was found, and kept out of `docs/` for the reason
[history/README.md](README.md) gives — a record stops being useful the moment
it is edited to stay current.

> **Two things have changed underneath this since it was written.**
>
> The client. Everything below was written against `rabbitmq-amqp-js-client`
> (AMQP 1.0); the repo runs on `amqplib` (AMQP 0-9-1) now, and the sections
> about *client* behaviour are history rather than current fact — concurrent
> link creation, the stranded-delivery stall, the two ways the client took the
> process down, and the missing redelivery signal are all gone, not worked
> around, absent. See [ADR 004](../docs/decisions/004-downgrade-to-amqp-0-9-1.md).
>
> The fleet. It selected its active daemons by index and a configured fleet
> size; `activeIndices`, cited below, no longer exists, and the target is a
> fraction each daemon applies to its own position. See
> [ADR 013](../docs/decisions/013-the-target-as-a-fraction.md).
>
> What survives unchanged is everything about the *broker*: quorum queues,
> `x-delivery-limit`, single-active-consumer election, dead-lettering and
> `x-first-death-*`, durability. That split is the point, and it is why the
> client migration touched no call site.

The current description of this system is
[docs/rmq-control-plane.md](../docs/rmq-control-plane.md).

## What's actually verified, and how

**Client**: [`rabbitmq-amqp-js-client`](https://github.com/coders51/rabbitmq-amqp-js-client)
— AMQP 1.0, RabbitMQ 4.x's native protocol support, not the older AMQP 0-9-1
that a library like `amqplib` speaks. Its own README describes it as an
early-stage project, which turned out to matter (see the gap below), so it
was verified directly rather than trusted from its docs.

Everything this design leans on is pinned by
`packages/rmq/test/integration/Client.test.ts` — run it with
`pnpm run test:rmq` (opt-in, needs Docker; not part of `pnpm test`). It
drives the real `@egress/rmq` service against a real
`rabbitmq:4.0-management-alpine` container via Testcontainers, and covers
seven things:

```
✔ concurrent publisher creation routes each message to its own binding
✔ concurrent consumer creation binds each consumer to its own queue
✔ a killed connection comes back with its consumers still registered
✔ a poisoned publish channel reopens rather than ending publishing
✔ x-single-active-consumer elects one consumer and promotes another when it closes
✔ closing a consumer stops delivery without closing the connection
✔ closing a consumer with deliveries in flight leaves the rest of the connection alone
```

The SAC line and the one below it are the primitives the `OPEN`/`HALF_OPEN`
mechanism is built on: SAC really does elect exactly one of several registered
consumers and promote a different one when the active one closes (no election
code of our own), and closing a consumer really does stop delivery while
leaving the connection up.

The first two and the last are inherited from the AMQP 1.0 client, where each
pinned a bug. On amqplib the first two hold by construction — there are no
publisher links to race — and the last is *inverted*: it used to assert that a
shared connection stalled, and now asserts that it does not. They stay because
"by construction" is a claim and this is what checks it. The poisoned channel
test is newer, and pins a defect of this repo's own making rather than a
client's: a publish channel with no way to reopen ends publishing for the whole
process on the first channel-level error, silently.

### Concurrent link creation is broken in this client — and silently

`AmqpControlPlaneSink` publishes each API to its own `circuit.<apiId>`
routing key. Under the client this repo started on, that meant a `Publisher`
*handle* per apiId, created lazily on first delivery. The aggregator's very
first tick reports on all three APIs at once, so the first real run created
three publishers *concurrently* — three forked deliveries, each calling
`createPublisher` on the same connection at nearly the same moment.

That broke it: every message, regardless of apiId, arrived on the *first*
publisher's queue. A minimal repro nailed it down —
`Promise.all([conn.createPublisher(a), conn.createPublisher(b), conn.createPublisher(c)])`
on one connection, then one `publish` per handle, and all three messages
landed on `a`'s queue. Sequential creation (`await` each one before starting
the next) never showed the problem; only concurrent creation did.

Consumers turned out to be affected the same way, and worse: three
`createConsumer` calls in flight at once, on three different queues, left
all three consumers receiving the *first* queue's messages. So this is not
a publisher quirk — it is a race in link setup on a shared connection,
plausible for an early-stage client, and the kind of thing that only shows
up under the load shape a real system produces (every API reporting on one
tick; a daemon fleet starting N consumers at once). Both failure modes are
**silent** — no error, just plausible-looking traffic going to the wrong
place.

**Fix**: the guard lives in `@egress/rmq`'s `Client.ts`, not at any call
site — one semaphore permit owned by the connection, serializing every
operation that touches it. (On amqplib, which
[004](../docs/decisions/004-downgrade-to-amqp-0-9-1.md) moved this repo to, a
`Publisher` is not a link at all — it is the `{ exchange, routingKey }` pair a
send is addressed with, so there is nothing left to create concurrently on that
path. The guard stays because consumers and declares still are links, and
because the property is worth holding by construction rather than by which
client happens to be underneath.) This is not about sharing a connection between
daemons: each daemon is its own process with its own connection, as in
production. It is about one process opening several links on its own
connection at once, which is ordinary — the aggregator creates a publisher
per API on the same tick, and a single daemon opens a control-plane
consumer, a SAC probe-trigger consumer and a trigger publisher on its
control connection at startup. At this repo's volumes the serialization
costs nothing.

`packages/rmq/test/integration/Client.test.ts` (`pnpm run test:rmq`,
opt-in, needs Docker) pins both failure modes against a real broker, along
with SAC election/promotion and cancel/resume. The two concurrency tests
were confirmed to *fail* with the semaphore temporarily removed and pass
with it restored — a regression test that passes either way would not be
worth having.

### Closing a consumer with deliveries in flight kills the connection

This one only appeared once real daemons were running, and it is the more
dangerous of the two because the symptom is *silence*, not misrouting.

After a few minutes of the live run below, one daemon stopped reacting to
`circuit.control` entirely. Its container was up, its CPU at 0.01%, its file
descriptors and TCP sockets identical to a healthy daemon's — and its
control queue had 64 undelivered messages sitting behind a consumer the
broker still considered registered. It had also taken the whole
`probe-trigger` queue down with it: as the SAC-elected consumer it was
holding the election while unable to consume, so no other daemon could be
promoted and no `HALF_OPEN` probe ran at all.

The cause is the probe's own shape. `HALF_OPEN` opens a consumer on a work
queue holding tens of thousands of messages, takes one, and closes. The
broker has already pushed a full credit window (rhea's default is 1000) of
deliveries by then; closing strands every one of them, and enough strandings
exhaust the session, taking down *every link on that connection* — including
the control-plane subscription that was never touched.

Reduced to a loop against a real broker: fill a queue with 4000 messages,
then repeatedly open a consumer, take one message, close. A long-lived
consumer on an unrelated queue on the same connection goes deaf on the
seventh cycle. No error, no close event, nothing in any log. The identical
loop with each probe on its own throwaway connection ran clean.

**Fix at the time**: the daemon used **two connections**, and which links
lived on which was the whole point.

- The connection from the `Rmq` layer carried the control plane, and only
  ever *opened* links — the control-queue consumer, the SAC probe-trigger
  consumer, the trigger publisher — all created at startup and never closed.
  Nothing that could strand a delivery ever happened on it.
- Everything that churned got a second connection that `daemon.ts` opened and
  destroyed itself. Destroying the connection returned the stranded capacity,
  so the damage never accumulated.

**Fix now**: none needed. The stall was a property of that client, and the
test that used to reproduce it asserts its absence — twelve probe cycles on a
*shared* connection with a canary consumer that stays live throughout. A
channel is the unit that was wanted all along: closing one requeues what it
held and leaves its neighbours alone, so the daemon is back to one connection.

`makeRmq` is exported from `Client.ts` alongside `RmqLive` for exactly this:
a connection is a scoped resource here, not a process-lifetime one. The test
above asserts both halves — that the shared-connection loop *does* stall
(so a future client release that fixes it will fail the test and tell us the
workaround can go), and that the per-probe connection does not.

A second lesson came with it: the daemon now logs a **heartbeat** every 15
seconds, independent of the control plane. Every log line it had until then
was emitted while handling a `circuit.control` message, so a daemon that had
gone deaf looked exactly like a daemon whose circuit simply had not moved.
That is why the bug survived several transitions before anyone noticed.

### A daemon can be its own thundering herd

Smaller in code, and it changed a conclusion this document had already drawn.

The first version accepted every message the moment it arrived and fired the
egress call afterwards. Draining a 50k backlog therefore meant tens of
thousands of concurrent `fetch` calls from a *single* daemon — the broker
pushes as fast as the handler returns, and a handler that only starts a
promise returns instantly. The fleet-level policy would have been scaling
daemons down while each surviving daemon hammered the recovering upstream
harder than it ever did healthy.

Capping concurrency and dropping the excess was the first fix, and it was
the wrong one: the daemons shed 32,000 messages draining one backlog, which
makes the throughput numbers meaningless and hides the pressure instead of
transmitting it.

The right lever turned out to be **when the message is settled**. AMQP 1.0
replenishes link credit on settlement, so:

- `@egress/rmq`'s `consume` now takes a handler that may return a promise,
  and accepts the delivery only once that promise settles;
- the daemon's work handler returns the egress call, and parks behind a
  `maxInFlight` prefetch (default 32, `MAX_IN_FLIGHT`) when saturated;
- a parked handler holds its delivery unsettled, credit stops refilling, and
  the broker stops pushing.

That is genuine backpressure all the way to the queue — the backlog stays in
the queue where it is visible, rather than in a process-local buffer or a
burst of concurrent requests. Nothing is dropped: after the fix the same
35,772-message backlog drained with 34 failures and no shedding at all.

Worth being precise about what this does and does not contradict: the
terminology note above is still right that this client exposes **no API to
set link credit**, and the receiver's link configuration is hardcoded (no
`credit_window` passthrough — `getConsumerReceiverLinkConfigurationFrom` in
the client's bundle). Settlement timing is the one credit-adjacent lever
that *is* reachable, and it is enough for backpressure even though it is not
enough to implement `DEGRADED` as credit reduction.

> **Amendment, 2026-09-06.** The mechanism is the same and the vocabulary has
> changed: on amqplib the bound is `prefetch`, set per consumer when it is
> created, and a handler that has not acked yet counts against it. Deferring
> the ack is still what transmits the pressure, so every conclusion above
> holds — what is no longer true is that the lever is unreachable. It is an
> argument now (`DEFAULT_PREFETCH`, and `prefetch: 1` for the HALF_OPEN
> probe). See [decisions/004](../docs/decisions/004-downgrade-to-amqp-0-9-1.md).

### Two ways the client takes the process down

Both surfaced only once the daemon was tearing connections down under load,
and both throw from inside a socket callback — where there is no listener to
attach, because the client creates a private rhea container per connection
and never exposes it.

- **`transfer after detach`.** Closing a *connection* from inside a message
  handler leaves the rest of the frames in that same TCP read addressing a
  link that no longer exists. Fixed by ordering: the probe closes its
  *consumer* inline (which is what stops delivery at the first message) and
  lets `reconcile` retire the connection later, from outside any handler.
- **`Receiver link is closed`.** A direct consequence of deferred
  settlement: `OPEN` tears down the work connection while calls are still in
  flight, and accepting those deliveries afterwards throws. Fixed in
  `Client.ts` — settlement on a vanished link is genuinely moot, since the
  broker requeues an unsettled delivery when the link goes.

A narrowly filtered `uncaughtException` handler in `rmq-consumer`'s
`main.ts` remains as a backstop for the first of these, and only for it;
every other uncaught exception is still fatal on purpose.

### There is no way to say "this attempt failed, try again"

The work queue is declared with `x-dead-letter-exchange`, and a daemon whose
third-party call fails rejects the message rather than accepting it, so the
work lands on `<apiId>.work.dead` instead of disappearing. That much is
ordinary. What is not ordinary is why there is no retry in front of it.

The client offers three outcomes, and the middle one is missing:

| Outcome | What it sends | What RabbitMQ does |
|---|---|---|
| `accept()` | `accepted` | removes the message |
| `requeue()` | `modified{delivery_failed: false}` | requeues it, **delivery-count unchanged** |
| `discard()` | `modified{delivery_failed: true, undeliverable_here: true}` | dead-letters it |

AMQP 1.0's `delivery-count` is the only attempt counter that survives a
message moving between consumers — an in-process counter is lost the moment
the message goes back on the queue and another daemon picks it up. RabbitMQ
increments it only for a delivery marked *failed*, and `requeue()` is
hard-coded to mark it not-failed. There is no `modified{delivery_failed:
true, undeliverable_here: false}` on the public surface, so "failed, let
someone else try, and remember that this was attempt two" cannot be
expressed at all.

Measured rather than read off the spec: releasing the same message three
times returned `delivery_count: 0` every time. `Client.test.ts` pins both
halves — the dead-letter arriving, and the count not moving — so a client
release that exposes the missing outcome turns that test red and says a real
redelivery budget has become possible.

Given that, one attempt then dead-letter is the honest policy rather than a
shortcut past one. An unbounded requeue against a dead upstream is a hot
loop with no counter to stop it, which is strictly worse than a queue full
of messages you can look at.

**Amendment, 2026-09-05: this is a fact about the client, not about the
system.** The budget is a *queue* property. A quorum queue declared with
`x-delivery-limit` makes the broker count the attempts and dead-letter the
message itself, and it works through the client already in use. Measured
against a live broker, with the handler returning `requeue` every time:

```
deliveryCount reported to the handler : [0, 0, 0, 0]   (still always zero)
total deliveries before it stopped    : 4
dead-lettered                         : reason "delivery_limit"
```

So the client genuinely cannot count attempts, and does not have to.

**This is now what the fleet does.** The work queue is a quorum queue with
`x-delivery-limit: 3` (`WORK_DELIVERY_LIMIT` in `ControlPlane.ts`), and a
daemon whose third-party call fails returns `requeue` rather than `discard`.
Three retries, then the broker parks the message on `<apiId>.work.dead`
itself, with `reason "delivery_limit"` where it used to say `"rejected"`.

Three rather than more because RabbitMQ redelivers immediately, with no
backoff: each extra attempt is extra load on a third party that is already
failing, and what ends the amplification is the circuit opening, which stops
the daemons consuming at all.

The interaction with `REDRIVE_ON_CLOSE` is the part worth knowing, because it
is easy to read `WORK_DELIVERY_LIMIT` and conclude the wrong thing. **The
budget resets on redrive.** The redrive replays work by publishing the body
again, and a republished message is a new message to the broker, with a full
budget. Measured — one message, always requeued, through one redrive cycle:

```
work deliveries total : 8        (4 + 4)
dead-letter arrivals  : ["delivery_limit", "delivery_limit"]
```

So it is three attempts *per outage*, not three ever. A genuinely poison
message therefore comes back once per recovery rather than looping hot — a
bound worth having, and not the same thing as a stop. Capping the number of
redrives a single message may receive is not built; it would go where
`Redrive.ts` already stamps `x-egress-redrive-pass`.

`DeadLetter.test.ts` pins both halves against a real broker. See
`docs/decisions/001-amqp-client.md` for the full measurement, including the
same property seen from AMQP 0-9-1, where the consumer can also read
`x-delivery-count` directly.

### Dead-lettering stops being reliable once the connection bug has been provoked

Found by a test that kept failing about half the time, which is the only
reason it was found at all.

The setup is trivial: two queues declared identically, both with the same
`x-dead-letter-exchange`, a consumer on each that rejects its one message,
and a consumer on the dead-letter queue counting arrivals. In isolation it
passes every time. Run immediately after the stranding test above — the one
that deliberately provokes the client into stalling a connection — and about
half the time only *one* of the two rejections dead-letters. The other
message simply disappears.

What was ruled out, one experiment at a time:

- Not a slow broker: waiting fifteen seconds instead of two changed nothing.
- Not the consumer being starved: the consumer received its message and
  called `discard()` on it.
- Not `discard()` failing: instrumenting the client's `settle` to log instead
  of swallow produced no error at all.
- Not creation order: swapping which consumer is created first left the
  failure attached to the same queue.

So the client reports success, the broker acknowledges the rejection, and the
message is neither delivered onward nor left behind. Both the connection and
the queues are freshly created; the only thing shared with the stranding test
is the broker process itself.

There is no fix here, only a boundary: the dead-letter tests now run in their
own file, which means their own broker, and the stranding test cannot reach
them. It is not pinned by a test of its own because it reproduces roughly
half the time, and a test that fails half the time teaches nobody anything —
but anything running a workload that provokes the stranding bug should not
also be trusting dead-lettering on that broker.

### One dead-letter queue for everything, and how to drain it anyway

Every queue the fleet declares dead-letters to `<apiId>.work.dead`: the work
queue, both SAC election queues, and each daemon's own control queue. Only
the dead-letter queue itself has no target, because that would be a cycle.

The reason to bother is the second silent-loss path, which looked nothing
like the first. A control event that failed `CircuitEvent`'s schema used to
be logged and *accepted* — so the only evidence of a version skew between
publisher and fleet was a line in `docker logs`, and the message itself was
gone. Rejecting it instead means it is still there to look at.

Rejecting is bounded, though. A control event goes to every daemon's own
queue, so a persistent schema mismatch is every event times the fleet size,
landing on a single queue at the full event rate — a version skew becoming a
second incident. Each daemon preserves the first 20 unreadable messages and
accepts the rest with one log line saying so, which answers the question a
human actually has ("what does the message look like?") without the flood.
The metric counts past the bound, so the rate is still visible. Verified: 25
malformed events published, 20 preserved, 25 counted.

That immediately raises the question the redrive has to answer: the queue now
holds two kinds of thing, and replaying a poison control message onto the
*work* queue would be nonsense. RabbitMQ 4 answers it — a dead-lettered
message carries AMQP 1.0 annotations naming where it came from:

```
message_annotations = {
  "x-first-death-queue": "payments-provider.control.daemon-0",
  "x-first-death-reason": "rejected",
  "x-opt-deaths": [{ queue: "...", reason: "rejected", count: 1, ... }]
}
```

So the redrive replays only what was dead-lettered from the work queue, and
moves anything else to the tail rather than releasing it — releasing puts a
message straight back at the head, where one poison message starves
everything behind it forever.

Two things that took a live run to get right:

- **A republish drops the annotations.** Moving a message to the tail loses
  exactly the provenance the filter depends on, so the pass restamps it with
  its origin as an application property on the way past
  (`x-egress-origin-queue`). That property is also how the next delivery is
  recognised as one this pass has already handled.
- **Without that recognition, the pass eats itself.** The first version
  re-parked whatever it could not replay and relied on an idle timer to stop:
  measured at **17,703 republishes of two messages in 2.5 seconds**. With the
  stamp it notices it has come full circle and stops — same scenario, 204ms,
  five work messages replayed and both non-work messages left where a human
  can find them.

### Recovering the dead-letter queue, once, by the broker's choice

Dead-lettering preserves failed work; it does not recover it. Left alone the
queue only grows, which is a slower and more dignified way of losing the same
messages. `REDRIVE_ON_CLOSE` closes that loop: on the transition back to
`CLOSED`, the dead-lettered messages are replayed onto the work queue.

The interesting constraint is *who*. Five daemons each replaying the same
backlog turns a recovery into a fivefold burst at a third party that has just
come back — the exact herd the fleet policy exists to prevent, arriving
through the back door. So the redrive is elected the same way the prober is,
on a second `x-single-active-consumer` queue (`<apiId>.redrive-trigger`),
deliberately separate from `probe-trigger`: the two elections are
independent, and coupling them would let one daemon's failure take out both.

Each pass is bounded and runs on its own throwaway channel — it closes a
consumer with a backlog behind it, which is the stranding hazard this document
is largely about, and closing the channel is what hands that backlog back. Passes repeat while the cap keeps being hit, so a
backlog larger than one cap does not need one outage per `REDRIVE_MAX`
messages to recover. A pass ends on the cap, an empty queue, the circuit
leaving `CLOSED`, or a hard deadline, whichever comes first.

Two things the first live run taught, both now fixed or written down:

- **A cap checked before an `await` is not a cap.** The broker delivers with
  a credit window in the hundreds, so every in-flight handler passed
  `moved >= redriveMax` before any of them incremented it: a cap of 5,000
  let 5,739 through. Reserving the slot before the await makes it exact.
- **A fleet at full strength drains faster than it looks.** A 62,000-message
  work queue went to zero inside one 5-second sample once the circuit closed
  — five daemons × 32 in flight against a local upstream. Worth knowing
  before reading a queue-depth graph and concluding something purged it.

Verified end to end: a 2,070-message dead-letter backlog, elected to
`daemon-2` (not the daemon that had been elected prober earlier, which is the
evidence the two elections are independent), drained in a single pass three
seconds after recovery, leaving both queues at zero.

### Rejecting a message preserves it only if the queue outlives the broker

Everything above assumes the dead-letter queue is still there afterwards. It
was not.

Every queue the fleet declared was transient — `durable: false` was hardcoded
in `@egress/rmq`'s `declareQueue`, which made it a default rather than a
decision. Measured on the running stack: 24 messages on
`payments-provider.work.dead`, then `docker compose restart rabbitmq`, then
zero. The work queue was declared the same way.

What makes this worth a section rather than a line is that the aftermath looks
healthy. The first daemon back redeclares the queue with the same name and the
same arguments, so a dead-letter queue that lost everything is
indistinguishable from one that never received anything — same name, same
arguments, depth 0, in the management UI and in `rabbitmq_prometheus` alike.
The only evidence is a number that had been climbing and then was not.

Durability is two flags rather than one. A durable queue keeps durable
messages; a message published without the durable header is dropped on restart
even from a durable queue, which would have moved the loss one level down and
left the fix looking like it had worked. `send` therefore sets it on every
publish and offers no per-call flag: on a transient queue the broker ignores
it, on a durable one it is the difference between keeping a message and
appearing to, and this is the one place where getting it wrong is silent.

Which queues get it is a real decision, made once in `ControlPlane.ts` so the
producer and the daemons cannot disagree:

- **Durable — the work queue and the dead-letter queue.** Nothing can
  reconstruct them. They hold work this system promised to keep, and the
  dead-letter queue is where that promise is most visible.
- **Transient — each daemon's control queue and both SAC election queues.** A
  control queue is a live subscription: a daemon that restarts learns the real
  state from the aggregator's next snapshot, which is what `snapshotMs` is
  for. Keeping those events across a restart buys nothing, and a control queue
  that survives its daemon is a queue growing behind a consumer that may never
  come back. The election queues are always empty by design.
- **Durable — the `circuit.control` exchange**, so the topology itself
  survives even though what it feeds does not need to.

One thing to know before changing any of this on a broker that is already
running: durability is part of a queue's identity, so a redeclare that
disagrees is refused rather than merged. Measured, in both directions:

```
409 "inequivalent arg 'durable' for queue 'mix.q' in vhost '/':
     received 'true' but current is 'false'"
```

The upgrade path over a live broker is therefore to delete the old queues —
or the broker's data — first, exactly as it would be for a changed
`x-dead-letter-routing-key`.

`DeadLetter.test.ts` pins the property against a real broker rather than
reasoning about it: five messages onto a durable queue and five onto a
transient one, one `container.restart()`, then five kept and none. Both halves
are asserted together because the contrast is the decision — transient is the
right choice for a live subscription and the wrong one for work you promised
to keep, and the two differ by a single flag.

Queue durability is necessary and not sufficient. The broker now has a volume,
so its data directory outlives the container and not only the process — but
one broker is a quorum of one: the durability and the delivery limit are real
on a single node, and tolerating the loss of a node needs three, which is a
topology this repo does not run.

**Verified on the running stack**, with the circuit `OPEN` and the producer
stopped so the numbers hold still:

```
before  docker compose restart rabbitmq redis
        payments-provider.work        6249
        payments-provider.work.dead    379     (total 6628)

after   payments-provider.work        5692
        payments-provider.work.dead    936     (total 6628)
```

Nothing lost, and the 557 that moved are the deliveries the daemons had in
flight when the broker went down, retried and then parked by the limit. The
aggregator carried on from its checkpoint through the same restart —
`sequence` 7 → 9 → 23, no reset — and every daemon reported `gaps=0 dup=0`
across both, which is the contract holding through an event the contract has
never been tested against before.

One caveat on measuring this yourself: a quorum queue reports 0 messages until
its Raft leader has been elected, which happens *after* `rabbitmq-diagnostics
ping` starts answering. Reading depths the instant the broker says it is up
shows an empty queue that is not empty.

### Changing queue topology on a broker that already has the queues

A queue's properties are part of its identity, and a redeclare that disagrees
is refused rather than merged. Measured, in both directions:

```
409 "inequivalent arg 'durable' for queue 'mix.q' in vhost '/':
     received 'true' but current is 'false'"
```

`x-queue-type` and `x-delivery-limit` behave the same way. So every change in
this document that touched a queue argument — dead-lettering, durability,
quorum, the delivery limit — is a migration on any broker that has been
running, and a first-boot detail everywhere else. The failure is loud, which
is the good news: the first daemon to start dies with a 409 rather than
running against a queue that is not what its code says it is.

The procedure, in order:

1. **Stop the producers.** Nothing new arriving.
2. **Let the daemons drain the work queue**, or accept that what is in it is
   about to be lost. There is no in-place upgrade: a queue cannot change type,
   and deleting it deletes its messages.
3. **Stop the daemons.** A queue with a consumer attached cannot be deleted
   cleanly, and a daemon that redeclares mid-migration re-creates the old
   shape.
4. **Delete the queues** — work, dead-letter, control, and both election
   queues. The dead-letter queue is the one to think about: it holds work the
   fleet could not complete, so drain it with `REDRIVE_ON_CLOSE` or copy it
   somewhere before it goes.
5. **Start the fleet.** The first daemon up redeclares everything with the new
   arguments, exactly as it does on a cold broker.

The alternative to steps 2-4, for a queue whose contents cannot be lost, is a
versioned name — `<apiId>.work.v2` — declared alongside the old one, with
consumers moved over and the old queue drained and then deleted. That trades a
maintenance window for a naming scheme, and this repo has not needed it.

### The fleet is on the dashboard now, because that bug was an observability bug

Every daemon serves `/metrics` on `METRICS_PORT` from the same in-process
`effect` registry the aggregator uses, and Prometheus scrapes all five plus
RabbitMQ's own exporter (`rabbitmq_prometheus`, enabled by default in the
management image, on 15692 — per-queue depth lives behind
`/metrics/detailed`, since the plain endpoint aggregates every queue into
one number).

The panel that matters is `egress_daemon_target_fraction` against
`sum(egress_daemon_self_active)`. Every daemon derives the same target from
the same events, so every daemon should report the *same* fraction — and when
one stops matching the others, it has stopped hearing the control plane while
still looking perfectly healthy. That is exactly the failure described above,
which took a session to find by hand and is now a glance.

Two of these metrics are the delivery contract rather than fleet mechanics.
`egress_daemon_control_gaps_total` and `_duplicates_total` apply the same
per-API sequence rule `/api/subscriber` applies to the webhook stream —
snapshots repeat and are exempt, `state_changed` must be gapless — but on
the AMQP transport, from five processes the publisher does not control. A
non-zero duplicate count is what a leader resuming from a stale in-memory
sequence looks like from the outside.

## Verified end to end

> A record of a run, not a description of the present. It was taken while the
> fleet selected its active daemons by index — `activeIndices`, cited below, no
> longer exists, and the target is a fraction now rather than a count. What
> replaced it and why is
> [ADR 013](../docs/decisions/013-the-target-as-a-fraction.md); what the run proved
> about the broker is unchanged.

`docker compose up` runs the whole thing: `rabbitmq`, `rmq-producer` at
200 msg/s onto `payments-provider.work`, and `rmq-daemon` scaled to five as
separate containers. Failure is injected the same way the rest of the repo
does it, on the upstream rather than anywhere in this stack:

```bash
for p in 8080 8081 8082; do
  curl -sX POST http://flaky-upstream:$p/__fail -d '{"rate":1.0}'
done
```

What the live run showed:

- **All five daemons act on the same event, independently.** Each logs
  `seq=<n> (<reason>) <STATE> target=<k>/5 self=ACTIVE|idle`, and the five
  agree on target every time — no coordination between them, just the same
  event stream through the same pure `DaemonPolicy.step`.
- **`OPEN` really stops the calls.** Every daemon's `ok` counter freezes at
  the value it had when the transition landed, and the work queue's depth
  starts climbing in the management UI — the backlog is the *point*, it is
  what a fleet that stopped hammering a dead service looks like.
- **SAC picked the prober, and it was not index 0.** RabbitMQ elected
  `daemon-1`, which is the evidence that the election is the broker's and
  not our indexing: `activeIndices` would have chosen index 0 every time.
- **Killing the prober mid-incident promotes another.** `docker kill` on
  `daemon-1` during `OPEN`, and the next `HALF_OPEN` was probed by
  `daemon-4`. No election code, no heartbeat, no failover logic of ours.
- **Recovery ramps rather than snapping**, and drains cleanly. The observed
  progression was `target=0` → `1` (the `HALF_OPEN` prober) → `4` → `5`, one
  rung per event, exactly `RAMP_SCHEDULE`. The 35,772-message backlog that
  had accumulated during the outage drained to single digits with 34 failed
  calls across the fleet, no shedding, and `inFlight`/`queued` back at zero.

A representative slice of one daemon's log through the recovery:

```
seq=219 (PROBE_FAILED)        OPEN      target=0/5 self=idle   ok=0     failed=0
seq=220 (OPEN_TIMEOUT_ELAPSED) HALF_OPEN target=1/5 self=idle   ok=0     failed=0
seq=221 (PROBE_SUCCEEDED)     CLOSED    target=4/5 self=ACTIVE ok=0     failed=0
seq=221 (PROBE_SUCCEEDED)     CLOSED    target=5/5 self=ACTIVE ok=11305 failed=34
```

Every bug described above was found by this run, not by reading the client's
source.

### The same run, once the fleet had metrics

Repeated after `/metrics`, dead-lettering and the Prometheus/Grafana wiring
went in — same injection, read from Prometheus rather than from logs:

| | mid-outage | after recovery |
|---|---|---|
| `egress_circuit_state` | 2 (`OPEN`) | 0 (`CLOSED`) |
| `max(egress_daemon_target_active)` | 0 | 5 |
| `sum(egress_daemon_self_active)` | 0 | 5 |
| `payments-provider.work` depth | 3,007 | 0 |
| `payments-provider.work.dead` depth | 413 | 413 |
| `sum(egress_daemon_dead_lettered_total)` | 429 | 430 |
| gaps + duplicates, AMQP side | 0 | 0 |
| `sum(egress_circuit_ejections_active)` | 9 | 0 |

Two things in that table are worth more than the rest.

The dead-letter queue holding 413 messages after recovery is the whole
argument for this change: that work used to be accepted and gone. It is now
sitting somewhere you can look at it.

And `dead_lettered` (430) exceeding the dead-letter queue's depth (413) is
not a discrepancy, it is the guarded settle doing its job. When `OPEN` tears
down the work connection with calls still in flight, the rejection lands on
a link that has already gone; `Client.ts` swallows that, and the broker
requeues the delivery rather than dead-lettering it. Those messages were
retried and succeeded. The counter says "this many calls failed"; the queue
says "this much work is still unfinished", and they are different questions.

The recovery in this run went `target=0 → 1 → 4 → 5` again, through
`PROBE_FAILED → OPEN → HALF_OPEN → PROBE_SUCCEEDED`, with one snapshot
repeating `seq=9` in the middle — correctly not counted as a duplicate,
which is the rule the AMQP-side contract check exists to apply.

### `DEGRADED`, finally reached

This sat under "what's still missing" for two passes, blamed on tuning. It
was topology: every Envoy cluster had exactly one endpoint, so a replica
could only ever report `0/1` or `1/1` and `healthy < total` — the condition a
`DEGRADED` vote is derived from — was unreachable by construction. Failing
the one endpoint went straight to `ALL_ENDPOINTS_EJECTED`.

With six endpoints on `payments-provider` and two of them failed:

```
 state = DEGRADED | reason = OUTLIER_EJECTION | healthy/total = 4/6
 votes = {'OK': 0, 'DEGRADED': 3, 'DOWN': 0}
   envoy-00 DEGRADED 4/6 ejected=2
   envoy-01 DEGRADED 4/6 ejected=2
   envoy-02 DEGRADED 4/6 ejected=2
```

The fleet's `DEGRADED → target = ceil(fleetSize / 2)` rung is therefore
reachable on the real stack now, not only in `DaemonPolicy.test.ts`.

