# 014 — Every service declares what it may use

**Status**: adopted 2026-09-12.
**Date**: 2026-09-12.
**Context**: every resource number this repository has published — the RSS
figures in [measurements.md](../measurements.md), the soak, the scale probe's
memory column — came from a human reading `docker stats` on one machine, on a
stack with nothing limiting it.

## The problem

Both halves of that are wrong, and the second is the one that matters.

A reading is a moment. `docker stats --no-stream` reports the second it was
run in, so "the aggregator uses 103 MiB" means "it did when somebody looked",
and nothing re-runs it — the same failure [chaos.mjs](../../infra/chaos.mjs)
was written to fix for the adversarial tests.

And an unbounded container is sized by whatever the host had spare. That is not
a small effect. Running the demo scenario on this stack with no limits and then
again with them, on the same machine, minutes apart:

| | peak RSS, no limit | peak RSS, 512m limit | |
| --- | --- | --- | --- |
| rmq-daemon | 121 MiB | 81 MiB | −33% |
| aggregator | 106 MiB | 74 MiB | −30% |
| rmq-producer | 96 MiB | 68 MiB | −29% |
| traffic | 66 MiB | 31 MiB | −52% |
| grafana | 119 MiB | 72 MiB | −39% |
| rabbitmq | 391 MiB | 324 MiB | −17% |
| envoy-00 | 35 MiB | 34 MiB | −4% |
| redis | 11 MiB | 10 MiB | −8% |

The pattern is exactly the one you would predict once you see it: **everything
with a garbage collector shrank, and everything without one did not.** V8, the
BEAM and Go's collector all size their heaps from what they believe memory to
be, so a Node process on a 47 GiB host collects lazily and holds more. Envoy
and Redis allocate what they need and were unmoved.

So "the daemon uses 121 MiB" was never a fact about the daemon. It was a fact
about this laptop, and every number in that column was a third too high for
anyone who ran it in a container that was actually constrained.

## The decision

Two things, and the second is what makes the first mean anything.

**Every service in [docker-compose.yml](../../docker-compose.yml) declares
`deploy.resources.limits`** — six classes, sized at roughly twice the highest
one-second peak measured during a demo run, listed at the top of the file with
what each one is for.

**[`infra/instrument.mjs`](../../infra/instrument.mjs) measures the stack
automatically and refuses to produce a record from one that has no limits.** It
reads Docker's own stats stream over the Engine API — one sample per second per
container, pushed rather than polled — and exits non-zero when the run was not
repeatable: a service without a ceiling, a container the kernel throttled
beyond its budget, an OOM kill, or a scenario that failed. The numbers it
prints are therefore either inside a declared envelope or not quoted.

## What building it found

**A limit the workload never reaches still gets hit.** The CPU quota is
enforced per 100ms period, so the traffic generator — whose busiest *second*
was 0.23 cpus against a 0.50 ceiling, more than twice its peak — was throttled
in five of 428 periods. A second that averages a quarter of a core still
contains tenths that do not. No headroom short of no limit removes this, so
the harness carries a budget (1% of periods, `--throttle-budget`) rather than
vetoing on the first occurrence, and the two load fixtures were moved to a full
core each — not for headroom, but because they are what the system under test
is measured *through*: a throttled generator sends fewer requests, so Envoy
sees a different upstream and the run measures the fixture.

**The broker does not read its own cgroup limit.** With
`deploy.resources.limits.memory: 2g`, RabbitMQ 4.0 still logged
`Memory high watermark set to 28853 MiB … of 48089 MiB total` — the host's RAM,
under cgroup v2, whose `memory.max` the container can read perfectly well. Left
alone that is the worst of both: flow control engages at a depth that depends
on whose machine it is, and the ceiling the kernel *does* enforce is reached
first, by the OOM killer, in the one component whose backlog is the thing being
demonstrated. [`infra/rabbitmq.conf`](../../infra/rabbitmq.conf) declares
1 GiB against the 2 GiB limit. `RABBITMQ_VM_MEMORY_HIGH_WATERMARK` would have
been one line in compose; RabbitMQ 4 removed it, and the image refuses to start
with "deprecated environment variables detected".

**One container's peak is not the stack's.** Summing per-container peaks gave
3.3 cpus for a stack whose busiest actual second was 1.65, because the daemon
running the redrive and the broker feeding it do not peak together. Samples are
bucketed by the second they were taken in, so the aggregate is an instant that
happened rather than an arithmetic one — which is the number that answers "what
machine does this need".

## What this does not fix

**It is a ceiling, not a reservation.** Two machines with different cores still
produce different wall-clock times for the same work: a limit says what a
container *may* take, never what it is guaranteed. What the envelope removes is
the other direction — a container growing to fit a host that happened to be
generous — and that turned out to be the larger error.

**Nothing pins CPU speed, kernel or architecture.** Every record stamps them
(`host.cpus`, `host.arch`, `host.kernel`, `host.engine`) so a comparison knows
what it is comparing, and `--baseline` reports the shape rather than the
absolute: a service whose peak moved by more than a quarter, folded by service
so that five daemon replicas compare as a fleet.

**The ceilings are generous on purpose.** They sum to 21.5 cpus against a stack
that peaked at 1.65, which is not a budget for a 4-core laptop — it is a bound
on each container so that no single one expands to fill the machine it found.
Sizing the *sum* to a reference host would be a different decision, and would
start by measuring contention rather than peaks.
