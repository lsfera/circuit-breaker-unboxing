# Scaling it, and adopting it

Two questions that the rest of this documentation answers only by implication:
what it costs to change the size of the consumer fleet, and what you would
actually take from here into a repository of your own.

Both answers are more honest than they are flattering.

## Scaling the fleet

### What it costs today

A daemon is active when its index falls under the fleet's target:

```ts
activeIndices(targetActive, fleetSize).has(index)   // index < min(target, fleetSize)
```

That single line is the ergonomics of this design. It buys something real —
**which** daemons are active is deterministic and stable, so a target moving
from 3 to 2 idles the same daemon every time rather than reshuffling the fleet
— and it costs two pieces of static configuration that every replica has to
agree on.

Adding a sixth daemon means:

1. giving it `DAEMON_INDEX=5` — unique, contiguous, and 0-based;
2. raising `FLEET_SIZE` to 6 on **all six**;
3. restarting the five that were already running, because `fleetSize` is read
   once at startup.

Step 3 is the part that makes this unsuitable for an autoscaler, and it should
be said plainly rather than buried: **this fleet does not scale without a
rolling restart.**

### What goes wrong if you skip a step

| Mistake | What happens |
|---|---|
| Two daemons share an index | They activate and idle together. The fleet is one smaller than it looks, and nothing says so. |
| `index >= fleetSize` | That daemon is never active. It starts, connects, consumes nothing, and reports healthy. |
| `FLEET_SIZE` raised on the new daemon only | The old ones cap their ramp at the old size, and the `DEGRADED` target — `ceil(fleetSize / 2)` — is computed differently across the fleet. |
| A daemon is removed | Its control queue is left bound to `circuit.control` with nobody reading it. |

The second and fourth of those used to be silent. Both are now caught:

- A daemon whose index is outside its fleet **refuses to start**, with the
  reason and the fix in the message. It is a configuration error, and
  [ADR 008](decisions/008-configuration-is-a-boundary.md) is the argument for
  failing at the boundary rather than running a process that cannot work.
- A control queue is declared with `x-expires`, so the broker deletes one that
  has gone ten minutes without a consumer. Before that, a departed daemon's
  queue kept filling: **three messages in forty seconds**, measured by stopping
  `rmq-daemon-4` and watching, and growing for as long as the broker lived.

The first and third remain yours to get right. They are a deployment concern —
a StatefulSet gives ordinal indices for exactly this reason — and this repo
does them with five hand-written compose services.

### What would have to change for it to scale properly

Worth writing down, because it is the obvious next piece of work and the shape
of it is not obvious.

The index exists so that a *count* can be turned into a *set* without the
daemons talking to each other. Any replacement has to preserve that, and the
options are:

- **Ordinal identity from the platform.** A StatefulSet's pod ordinal is the
  index, and `fleetSize` comes from the replica count. Cheapest, and it moves
  the problem to somewhere that already solves it — but it still restarts the
  fleet when the count changes.
- **Let the broker assign the slots.** The prober and the redrive are already
  elected by RabbitMQ through `x-single-active-consumer`; `targetActive` slots
  could be elected the same way, with one SAC queue per slot. No index, no
  fleet size, no restart — and considerably more machinery than a comparison.
- **Publish the target as a fraction.** The aggregator says "run at 50%"
  rather than "run 3 of 5", and each daemon decides for itself by hashing its
  own instance id. Removes both the index and `fleetSize`, and growing the
  fleet changes no existing daemon's state at all. Explored and measured in
  [ADR 013](decisions/013-the-target-as-a-fraction.md): the ergonomics claim
  holds completely, and the accuracy is worst at exactly this fleet size —
  ±60% at five daemons, and 3.2% of five-daemon fleets would run none at all on
  `DEGRADED` unless a floor of one is elected by the broker.

None of these is implemented. The first is what a production deployment would
most likely do.

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

The consumer fleet is a worked example. Its index-and-size configuration is the
weakest ergonomics in this repository, it is documented above rather than
defended, and a real deployment would replace it with ordinals from the
platform.
