# RabbitMQ control plane

**Status: the publisher side is real, built, and running against a live
broker; the daemon fleet is not written yet.** `packages/rmq` (the Effect
client + `circuit.control` helpers) and `packages/aggregator/src/
AmqpControlPlaneSink.ts` (the sink itself, mounted via `main.ts --rmq=<host>:
<port>`) exist and are verified end to end below — a running aggregator
correctly publishes every API's events to its own routing key on a real
RabbitMQ 4.x broker. What follows the architecture section is the design for
the *consumer* side (the daemon fleet), diagrammed but not yet built as
`packages/rmq-consumer`.

## The scenario

A high-throughput RabbitMQ queue feeds several competing-consumer daemons,
each of which calls a flaky third-party service per message. When that
service degrades, the daemon fleet needs to back off — without a thundering
herd on the way down (every daemon retrying in lockstep) or on the way back
up (every daemon resuming at full throughput against a barely-recovered
service). This repo's circuit breaker events are the natural trigger for
those compensating actions; this document is about *how* they'd drive them.

## Architecture

```mermaid
flowchart LR
  classDef leader fill:#dcf3f1,stroke:#0c8b86,stroke-width:3px,color:#04302e,font-weight:bold;
  classDef box fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,color:#161d2b;
  classDef idle fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,stroke-dasharray:4 3,color:#5b6478;

  agg["aggregator<br/>(this repo, unchanged)"]:::leader

  subgraph RMQ["RabbitMQ"]
    direction TB
    ex{{"circuit.control<br/>topic exchange"}}:::box
    wq[("payments-provider.work<br/>competing consumers, normal queue")]:::box
    cq[("probe-trigger<br/>x-single-active-consumer=true")]:::idle
  end

  subgraph FLEET["daemon fleet"]
    direction TB
    d1["daemon-1"]:::box
    d2["daemon-2"]:::box
    d3["daemon-3"]:::box
  end

  subgraph EGRESS["egress — transparent to every daemon"]
    direction TB
    envoy["one address<br/>(no replica names, no admin ports)"]:::box
    svc[("flaky third-party service")]:::box
    envoy --> svc
  end

  agg -->|"AmqpControlPlaneSink<br/>state_changed"| ex
  ex -.->|"circuit.payments-provider.*"| d1
  ex -.->|"circuit.payments-provider.*"| d2
  ex -.->|"circuit.payments-provider.*"| d3

  d1 -.->|registers, idle| cq
  d2 -.->|registers, idle| cq
  d3 -.->|registers, idle| cq

  wq --> d1
  wq --> d2
  wq --> d3
  d1 --> envoy
  d2 --> envoy
  d3 --> envoy
```

Two things worth being explicit about, because they're the parts most likely
to be built wrong on a first pass:

- **The coordination queue (`probe-trigger`) is not the work queue.** `x
  -single-active-consumer` guarantees exactly one active consumer — apply
  that to the *work* queue and you've disabled the N-way parallelism the
  whole fleet exists for. It's scoped to a separate, always-idle queue used
  only to elect a prober during `HALF_OPEN`.
- **The daemons never learn Envoy's topology.** Same invariant as
  `infra/traffic-generator.mjs` in the main repo: one configured egress
  address, no replica names, no admin ports. Replica-level detail stays
  exactly where it already lives — the aggregator's `FleetSource.ts`.

## State → action mapping

A note on terminology first: this design was originally drafted in AMQP
0-9-1 terms ("prefetch"), but the client this repo actually uses speaks AMQP
1.0, and **AMQP 1.0 has no prefetch** — RabbitMQ's own comparison of the two
protocols doesn't call it a rename, it lists 0-9-1's "simple: consumer
prefetch" against 1.0's "sophisticated: link flow control and session flow
control" as different mechanisms entirely. AMQP 1.0's closest analogue,
**link credit**, isn't exposed by the pinned client at all (checked its
actual `Consumer` type, not assumed — see below). Rather than approximate
credit control by closing and recreating consumers, the mapping below is
built entirely on the two primitives already verified live: opening/closing
a consumer, and SAC election. No credit control anywhere.

| Circuit state | Daemon fleet action | Mechanism (verified) |
|---|---|---|
| `CLOSED` | All daemons active | Every daemon holds an open consumer on the work queue |
| `DEGRADED` | Fewer daemons active | Some daemons close their consumer, the rest stay open |
| `OPEN` | No daemons active | Every daemon closes its consumer — the connection and the control-plane subscription both stay up |
| `HALF_OPEN` | Exactly one daemon probes | SAC promotion on `probe-trigger`; the elected daemon opens a work-queue consumer for one message, then closes it again |
| → `CLOSED` (recovery) | Ramp the active-daemon count back up (1→…→N) | Consumers reopened gradually, not all at once — never a snap to full, which is the actual thundering-herd risk on the way *back* |

Judgment of `PROBE_SUCCEEDED` vs `PROBE_FAILED` is **not** reimplemented
here — it stays with Envoy's outlier detection and the aggregator's existing
quorum logic. The elected daemon's only job during `HALF_OPEN` is to
guarantee one real call happens through the egress listener; the aggregator
decides what that call meant, exactly as it already does today.

## The `HALF_OPEN` election, end to end

```mermaid
sequenceDiagram
  participant Agg as aggregator
  participant Ex as circuit.control
  participant CQ as probe-trigger (SAC)
  participant D1 as daemon-1 (active)
  participant D2 as daemon-2 (standby)
  participant WQ as work queue
  participant Envoy as egress

  Agg->>Ex: state_changed · HALF_OPEN
  Ex-->>D1: HALF_OPEN
  Ex-->>D2: HALF_OPEN
  D1->>CQ: publish trigger
  Note over CQ: SAC delivers to the one active consumer, whichever daemon that is
  CQ-->>D1: trigger delivered
  D1->>WQ: create a consumer, take one message
  D1->>Envoy: real call through the egress listener
  Note over Envoy,Agg: Envoy's outlier detection + the aggregator's quorum judge the call — not the daemon
  D1->>WQ: close the consumer after the one message
  Agg->>Ex: state_changed · PROBE_SUCCEEDED or PROBE_FAILED
```

If the elected daemon dies mid-probe, RabbitMQ's own SAC promotion hands the
role to a different registered consumer — no heartbeat, no hand-rolled
election, confirmed below.

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
four things:

```
✔ concurrent publisher creation routes each message to its own binding
✔ concurrent consumer creation binds each consumer to its own queue
✔ x-single-active-consumer elects one consumer and promotes another when it closes
✔ closing a consumer stops delivery without closing the connection
```

The last two are the primitives the `OPEN`/`HALF_OPEN` mechanism is built
on: SAC really does elect exactly one of several registered consumers and
promote a different one when the active one closes (no election code of our
own), and closing a consumer really does stop delivery while leaving the
connection — and so the control-plane subscription — up. The first two pin
the concurrency bug described below, and were confirmed to fail with the
fix removed.

### Concurrent link creation is broken in this client — and silently

`AmqpControlPlaneSink` publishes each API to its own `circuit.<apiId>`
routing key via a dedicated `Publisher` per apiId, created lazily on first
delivery. The aggregator's very first tick reports on all three APIs at
once, so the first real run created three publishers *concurrently* — three
forked deliveries, each calling `createPublisher` on the same connection at
nearly the same moment.

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
operation that touches it. Callers get a connection that is safe to share
across concurrent fibers by construction; that is what makes it viable for
the daemon fleet to run N consumers over one connection instead of N
connections. At this repo's volumes the serialization costs nothing.

`packages/rmq/test/integration/Client.test.ts` (`pnpm run test:rmq`,
opt-in, needs Docker) pins both failure modes against a real broker, along
with SAC election/promotion and cancel/resume. The two concurrency tests
were confirmed to *fail* with the semaphore temporarily removed and pass
with it restored — a regression test that passes either way would not be
worth having.

## What's still missing

- **The DEGRADED-as-credit-reduction idea is abandoned, on purpose, not
  worked around.** Checked the actual public surface rather than assumed it:
  `Consumer` exposes exactly `start()`/`close()`/`id`/`replyTo` — no method
  to grant or withdraw link credit after a consumer is created, which is the
  only lever AMQP 1.0 has for this (see the terminology note above —
  [RabbitMQ's own comparison](https://www.rabbitmq.com/blog/2024/08/05/native-amqp)
  treats 0-9-1 prefetch and 1.0 flow control as different mechanisms, not a
  rename). Rather than approximate credit reduction by closing and
  recreating consumers — heavier than the design assumed, and it
  reintroduces some of the reconnect cost the design chose cancel-over
  -disconnect specifically to avoid — the state → action mapping was
  rebuilt on only the two primitives verified above: `DEGRADED` scales the
  *number of active daemon consumers* down (some close, some stay open),
  not any one consumer's credit. `CLOSED`/`DEGRADED`/`OPEN`/`HALF_OPEN` and
  the ramp back to `CLOSED` are now all buildable on `open`/`close` and SAC
  alone — see the updated state table below.
- **The daemon package itself** — connection handling, the state-machine
  reaction to `circuit.control` messages, the active-count ramp schedule.
  Not written; would be `packages/rmq-consumer`, following this repo's
  existing `packages/domain` / `aggregator` / `subscriber` / `demo` split,
  and reusing `@egress/rmq`'s `Rmq` service directly rather than
  reimplementing the connection wrapper.
- **The ramp-back schedule** (1→4→16→max active daemons) is specified but
  its timing (gated on elapsed time? successful message count? both?) isn't
  pinned
  down.
- **No end-to-end run with real daemons.** The aggregator side is verified
  against a live broker (above); what's not yet verified is a real daemon
  fleet reacting to those events against the actual egress stack in this
  repo — that needs `packages/rmq-consumer` to exist first.
