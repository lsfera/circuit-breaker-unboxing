# Scaling it, and adopting it

Two questions that the rest of this documentation answers only by implication:
what it costs to change the size of the consumer fleet, and what you would
actually take from here into a repository of your own.

Both answers are more honest than they are flattering.

## Scaling the fleet

### What it costs today

Nothing. One command:

```
docker compose up -d --scale rmq-daemon=12
```

No index to assign, no fleet size to keep in step, and no restart of the
daemons already running — verified by watching: scaling five to twelve left all
five original containers with their original start times, and Prometheus found
the seven new ones on its own through DNS discovery rather than a config edit.

That is recent, and it is worth knowing what it replaced, because the
replacement has a cost of its own.

### How it works

The aggregator publishes a state; each daemon turns that into a **fraction** of
the fleet that should be pulling, and applies it to its own position in a hash
space derived from its instance id:

```ts
runsWork(policy, self)   // self.position < policy.fraction || (policy.floor && self.isFloor)
```

Nothing is coordinated and nothing is configured. A daemon needs to know only
its own identity, which Docker already gives it as a hostname.

Two things fall out that are not obvious:

- **`DEGRADED` is approximate.** Hash selection is independent per daemon, so
  "half the fleet" lands near half rather than on it — ±60% at five daemons,
  ±16% at a hundred. `docker compose` prints the gap rather than hiding it:
  `target=50% (~6 of 12) pulling=5`.
- **"Exactly one" cannot be a fraction**, so it is not one. The `HALF_OPEN`
  prober and the first rung of the recovery ramp are both elected by the broker
  on a single-active-consumer queue, and so is the **floor** — the one daemon
  that runs whenever any work is wanted, which is what stops an approximate
  half from ever being none.

[ADR 013](decisions/013-the-target-as-a-fraction.md) has the measurements and
the argument.

### What to watch

| Metric | What it means |
|---|---|
| `egress_daemon_target_fraction` | Should be identical on every daemon. A spread is a daemon that has gone deaf to `circuit.control` — the `FleetDisagreesWithTarget` alert. |
| `egress_daemon_floor_held` | Should sum to exactly 1 whenever the target is non-zero — the `FloorUnheld` alert. Zero means a `DEGRADED` fleet could stop entirely. |
| `sum(egress_daemon_self_active)` against `fraction × count` | The cost of approximation, in daemons. A persistent gap means a fleet too small for the fraction to land. |

### What it costs to run small

Below about ten daemons the approximation is loose enough to notice, and the
floor is doing real work rather than being a safety net. If the fleet is fixed
at three or five and never changes, the index this replaced was more accurate;
ADR 013 says exactly when each is the better trade.

## Adopting it

There are three sizes of answer, and most readers want the first.

### 1. Subscribe to the event contract

You do not need any of this code. The system publishes CloudEvents with a
per-API sequence that is gapless and never repeats, over webhook, SSE or AMQP,
and the whole contract is one schema in
[`Model.ts`](../packages/domain/src/Model.ts) plus one rule:

```ts
classifySequence(highest, sequence)   // "first" | "duplicate" | "gap" | "next"
```

Implement that rule on your side and you can detect a missed or repeated event
without trusting the publisher. [ADR 007](decisions/007-message-contracts.md)
is why the rule is written once and shared by all three observers here, and
[subscriber.ts](../packages/subscriber/src/subscriber.ts) is a complete
consumer in about a hundred lines, meant to be read as the shape yours should
take rather than depended on.

### 2. Take the breaker, leave the transport

[`packages/domain`](../packages/domain) has no Effect runtime, no clock and no
I/O in it. `Breaker.step` is a total function of `(state, now, config)` and
every timing rule is arithmetic on timestamps you pass in, which is what makes
the state machine testable without a runtime and portable without a broker.

If you have your own way of collecting per-replica health and your own way of
telling people about it, this is the part worth lifting, and it is a few
hundred lines with a test suite that runs in milliseconds.

The quorum arithmetic is one line and is worth understanding before you copy
it: [ADR 009](decisions/009-what-the-quorum-is-a-quorum-of.md) is about what
that denominator is, and the three ways a replica can leave it in silence.

### 3. Run the whole thing

The composition root of each process is a `main.ts` and the packages are
deliberately separable — `@egress/domain` knows nothing about Envoy, RabbitMQ
or Redis; `@egress/rmq` knows nothing about circuit breakers. But be clear
about what is production-shaped and what is demonstration:

| | Status |
|---|---|
| `@egress/domain` | the state machine and the contract — portable |
| `@egress/aggregator` | the control loop, lease, fencing, outbox — production-shaped |
| `@egress/rmq` | an Effect wrapper over amqplib — production-shaped |
| `@egress/rmq-consumer` | a worked example of a self-throttling consumer |
| `@egress/subscriber`, `@egress/demo` | demonstration only |
| `infra/flaky-upstream.mjs`, the simulated fleet | demonstration only |

Nothing here is published to a registry. Cross-package imports go through
`package.json#exports` and the whole workspace runs with no build step, so
taking a package means vendoring it rather than depending on it.

Before running any of it against something that matters, read
[what is a prototype, not production](../README.md#what-is-a-prototype-not-production)
and [security.md](security.md). The short version: the control plane is
unauthenticated, the metrics endpoints are open, and the node id a replica
claims is believed. Those are deliberate omissions in a demonstration and
unacceptable in anything else.

## The honest summary

The event contract is the part of this worth adopting, and it costs almost
nothing to adopt — a schema and a four-case function.

The control plane is worth running when the verdict is a fact several parties
act on, and [the approaches review](approaches.md) is the argument for when
that is and is not true.

The consumer fleet is a worked example, and the one part of this repository
whose ergonomics were rebuilt rather than defended: it scales with
`--scale rmq-daemon=N` and nothing else, at the cost of a `DEGRADED` target
that lands near its number rather than on it. What that trade is, and when it
is the wrong one, is [ADR 013](decisions/013-the-target-as-a-fraction.md).
