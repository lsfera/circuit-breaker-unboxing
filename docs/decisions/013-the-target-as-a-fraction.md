# 013 — The target as a fraction, not a count

**Status**: adopted 2026-09-12. The section below was written against the
decision *not* to adopt it; the amendment at the end says what changed and what
implementing it turned up.
**Date**: 2026-09-12.
**Context**: an ergonomics pass on
[scaling the fleet](../adopting.md#scaling-the-fleet), which named three ways
out of the index-and-size configuration and did not evaluate them.

## The problem it would solve

A daemon is active when `index < min(targetActive, fleetSize)`. That requires
every replica to hold a unique contiguous index and to agree on the fleet size,
and `fleetSize` is read once at startup — so **resizing the fleet needs a
rolling restart of the whole fleet**, which rules out an autoscaler.

The proposal: the aggregator's verdict already implies *what proportion* of the
fleet should be pulling — all of it, half, none. Publish that proportion and
let each daemon decide for itself, with no index, no fleet size, and nothing to
restart.

## How a daemon would decide

Hash the instance id to `[0, 1)` and run when that value is below the published
fraction:

```ts
active = hash(instanceId) < fraction
```

No coordination, no peers, no shared state. It is deterministic per daemon, so
the same daemons idle from one tick to the next, which is the property the
index was bought for in the first place.

Three things make this cheaper than it first appears:

- **The event contract does not change.** The mapping from circuit state to a
  target is already computed locally in `DaemonPolicy`; making it produce a
  fraction instead of a count touches one module, and nothing about what the
  aggregator publishes.
- **`HALF_OPEN` is already outside this mechanism.** `desired()` explicitly
  refuses to use the index there, because "exactly one prober" is elected by
  the broker through `x-single-active-consumer`. A fraction cannot express
  "exactly one" either — and does not have to.
- **So the index is only load-bearing for `DEGRADED` and the middle rungs of
  the `CLOSED` ramp.** Everywhere else the answer is all or none, which needs
  neither an index nor a size.

## What was measured

Five thousand random fleets per row, fraction `0.5`.

| fleet | median active | 5th–95th | relative error | ran **zero** |
|---|---|---|---|---|
| 3 | 1 | 0–3 | ±100% | **13.6%** |
| 5 | 2 | 1–4 | ±60% | **3.2%** |
| 10 | 5 | 2–8 | ±60% | 0.2% |
| 20 | 10 | 6–14 | ±40% | 0.0% |
| 50 | 25 | 19–31 | ±24% | 0.0% |
| 100 | 50 | 42–58 | ±16% | 0.0% |

And the thing the proposal exists for — growing the fleet with the fraction
held at `0.5`:

```
  fleet  5: 4 active   —
  fleet  6: 4 active   0 existing daemons changed state
  fleet 10: 7 active   0 existing daemons changed state
```

Moving the fraction on a fleet of 20 changes only the daemons that have to
change: `1.0 → 0.5` flips 8, `0.5 → 0.25` flips 6, and coming back flips the
same ones again. Stability is preserved exactly.

## What the numbers say

**The operational claim holds completely.** No index, no fleet size, no restart,
and zero churn among existing daemons when the fleet grows. That is the whole
of the ergonomics problem, solved.

**The precision claim inverts with fleet size**, and that is the finding:
relative error shrinks as √n, so the scheme is *least* accurate exactly where
this repository runs it. At a fleet of 5, "half" means somewhere between 1 and
4 — and 3.2% of fleets would run **no daemons at all** on `DEGRADED`, which is
not imprecision but a different state: a fleet that stops entirely is
indistinguishable from `OPEN`, and `DEGRADED` exists to say the opposite.

At a fleet of 3 that is 13.6%. At 50 it is zero and the error is ±24%.

So the two mechanisms are complements, not rivals:

| | index and size | fraction |
|---|---|---|
| Accuracy | exact | ±60% at 5, ±16% at 100 |
| Can run zero by accident | no | yes, below ~10 |
| Resize without restart | no | yes |
| Churn on resize | whole fleet restarts | none |
| Config per replica | unique index + agreed size | nothing |

**The index is right for a small fixed fleet. The fraction is right for a large
elastic one.** This repository runs five, by hand, in compose.

## The correctness hole is closable

A floor of one — the daemon the broker has already elected on a
single-active-consumer queue always runs while the target is non-zero — removes
the zero case entirely, using machinery that is already here for the prober and
the redrive:

| fleet | median | 5th–95th | ran zero |
|---|---|---|---|
| 3 | 1 | 1–3 | 0.0% |
| 5 | 2 | 1–4 | 0.0% |
| 10 | 5 | 2–8 | 0.0% |

That leaves imprecision, which is a real but survivable property: `DEGRADED`
means "reduced", and 1 of 5 instead of 2 of 5 is still reduced. It is the
difference between a fleet running slower than intended and a fleet that has
silently stopped.

## Why it is not adopted here

Not because it is wrong. Because at this repository's fleet size it trades an
ergonomics problem that is *visible* — you must edit compose and restart — for
an accuracy problem that is *invisible*: nothing reports that the fleet meant
to run 2 and is running 4. Every finding in
[findings.md](../findings.md) is some version of that trade going badly.

The conditions under which this becomes the right answer are specific and worth
writing down, because they are not hypothetical:

- a fleet large enough for the binomial to concentrate — **20 or more**, where
  the error is ±40% and falling;
- the floor of one, so `DEGRADED` cannot become a stop;
- and a metric for the gap between intended and actual active count, so the
  imprecision is observable rather than assumed.

The first rung of the `CLOSED` ramp is "exactly one daemon", which is also not
a fraction, and would move to the same SAC election as the prober. The rest of
the ramp — 4, 16, all — expresses as fractions cleanly.

## What would be built

For the record, so this is a decision rather than a sketch:

1. `DaemonPolicy.step` returns a fraction instead of a count; the ramp becomes
   `[elected-one, 0.25, 0.5, 1.0]`.
2. `desired()` takes `hash(instanceId)` where it took `index`, and drops
   `fleetSize`.
3. `--index` and `--fleet-size` are removed from `rmq-consumer`'s flags, along
   with the startup guard that exists only because they can disagree.
4. A third SAC queue, or a reuse of the probe trigger, provides the floor.
5. `egress_daemon_target_active` becomes intended-vs-actual, because the gap is
   now a real quantity rather than always zero.
6. docker-compose collapses five near-identical services into one with
   `deploy.replicas`, which is the outcome that motivated the whole exercise.

Steps 1 to 3 are an afternoon. Step 5 is the one that makes it safe.


---

## Adopted, and what building it found

Added 2026-09-12. The reasoning above stands — this is imprecise at five
daemons, and the floor is what makes that survivable rather than dangerous —
but the ergonomics were judged worth it, so it is what the fleet does now.

Three things came out of building it that the exploration did not predict.

**The hash has to actually mix.** FNV-1a was the first choice: cheap, no
imports, nothing here is adversarial. It does not mix identifiers that differ
in one character, which is exactly what fleet instance ids are. `daemon-0`
through `daemon-5` came out as

    0.9426  0.9465  0.9504  0.9543  0.9582  0.9621

a straight line covering 2% of the space, with 60 of 200 positions below 0.5
instead of about 100. A fraction applied to that selects the wrong proportion
*systematically* rather than noisily — the worse failure, and a silent one. It
is SHA-256 now, which is what the simulation had measured all along, and 102 of
200 fall below 0.5. `position` runs once per process, so nothing is paid for it.

**The floor needs a lease, not a flag.** Single-active-consumer promotes
silently: a daemon that dies holding the floor is replaced by the broker, but
the replacement has no way to learn it has been promoted except by receiving
something. The floor queue is *bound to `circuit.control`* rather than published
to, so every event the aggregator already publishes elects the floor as a side
effect, and a sixty-second lease means a dead holder's claim expires rather than
outliving it.

**The monitoring had to change with it.** `FleetDisagreesWithTarget` compared
`sum(self_active)` to `max(target_active)` for equality, which a fraction makes
permanently false. It compares the daemons *to each other* now —
`max(target_fraction) != min(target_fraction)` — which is a better detector of
the fault it was written for: a daemon that has gone deaf reports the last
fraction it heard, and nothing else disagrees. A second alert, `FloorUnheld`,
watches the thing that now prevents a silent stop.

## What it looks like

Five daemons became one service scaled to five, and `--scale rmq-daemon=12`
resizes it. Verified end to end on twelve replicas:

```
== The daemon fleet reacts ==
  target=0% (~0 of 12) pulling=0 work=13 dead-lettered=331
== Fleet: ramp back, drain, and the same contract on AMQP ==
  target=100% (~12 of 12) pulling=12 work=0 dead-lettered=0 (deepest backlog seen: 1765)
  ... checked by 12 consumers the publisher does not control
```

Scaling five to twelve left all five original containers with their original
start times, and Prometheus discovered the seven new ones through DNS rather
than a config edit. The intended count is printed next to the actual one on
that line on purpose: the gap between them is the cost of this decision, and it
should be visible rather than assumed.
