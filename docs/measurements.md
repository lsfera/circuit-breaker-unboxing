# Measured limits

Every number here comes from a run on this stack.

## Inside a declared envelope

Every compose service declares `deploy.resources.limits`, and
`infra/instrument.mjs` refuses to produce a record from a stack that does not,
or from a run in which anything was throttled or OOM-killed
([ADR 014](decisions/014-the-measurement-envelope.md)). A container without a
ceiling is sized by whatever the host has spare, so its numbers describe the
laptop, not the system.

```bash
node infra/instrument.mjs demo     # the full incident, instrumented
node infra/instrument.mjs idle --seconds=60
```

The demo incident on 2026-09-12 (16 cpus, arm64): the whole stack's busiest
second used **1.65 cpus and 1.3 GiB across 19 containers**, while the work queue
held 1,730 messages and the fleet made 242 egress calls a second. The busiest
single process was the daemon running the redrive (0.65 cpus); each Envoy peaked
at 0.12, each aggregator at 0.09. Every garbage-collected process shrank 28–52%
once it could see a ceiling.

## At a thousand APIs

`--apis=N` replaces the named APIs with N synthetic ones on the simulated fleet;
`infra/scale-probe.mjs` samples a running instance.

| APIs × replicas | ticks/s | mean poll | Prometheus series | `/metrics` | RSS |
|---|---|---|---|---|---|
| 3 × 3 | 4.00 | 0.04 ms | 88 | 7.5 KB | — |
| 200 × 10 | 3.93 | 0.54 ms | 3,240 | 186 KB | 272 MB |
| 1000 × 10 | 3.73 | 2.42 ms | 16,040 | 911 KB | 462 MB |

The control loop is not what breaks: 333× the APIs costs 7% of the tick rate.
What grows is the observability surface (16 series per API), and the console
breaks first: it re-sends the whole state every 400 ms, about 2.75 MB/s per
browser at a thousand APIs. The frame is now built once for every connection,
so a hundred open consoles no longer slow the control loop (3.69 ticks/s either
way); [ADR 015](decisions/015-the-console-at-a-thousand-apis.md) is the plan for
sending each browser less.

## Chaos under load

`infra/chaos-load.mjs` injects process, network and third-party faults at the
start of a traffic spike and judges each run per message: every message the
broker confirmed must have been processed by the fake third party (by its
`<run>:<n>` key) or still be in a queue, and the dead-letter queue must be empty
at the end. Then it checks the breaker's transitions against the reducer's
table and the published sequence for gaps and repeats.

```bash
node infra/chaos-load.mjs --list
node infra/chaos-load.mjs --profiles=low --faults=flaky-full-cycle,kill-leader
```

Results for 2026-09-24/25 are in the README. Runs are voided when the host
suspends mid-run, since timings are meaningless across a sleep.

A soak of 27 minutes, deliberately disrupted (two chaos runs, a one-sided Redis
partition, a Redis outage, rebuilds): aggregator RSS 98.7 → 103.4 MiB, a daemon
95.7 → 97.1 MiB, 6,619 ticks, zero gaps, zero duplicates. That rules out a fast
leak and nothing more.
