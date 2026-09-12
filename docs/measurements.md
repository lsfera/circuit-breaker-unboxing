# Measured limits

Every number in this repo comes from a run on this stack, with the command
that produced it. Numbers about *ingestion* live with the decision they
settled, in [architecture.md](architecture.md#ingestion-push-or-poll-decided-by-measurement);
numbers about *failover* live in
[high-availability.md](high-availability.md).

## What the numbers are measured inside

A container with no ceiling is sized by whatever the host had spare, so a
resource number taken from one is a fact about somebody's laptop. Every service
in [docker-compose.yml](../docker-compose.yml) declares
`deploy.resources.limits`, and [`infra/instrument.mjs`](../infra/instrument.mjs)
refuses to produce a record from a stack that does not —
[ADR 014](decisions/014-the-measurement-envelope.md) is why, and what it cost
to find out.

```bash
docker compose up -d
node infra/instrument.mjs demo          # the full incident, instrumented
node infra/instrument.mjs idle --seconds=60
node infra/instrument.mjs chaos:leader
node infra/instrument.mjs demo --baseline history/runs/baseline-demo-2026-09-12.json
```

It reads Docker's own stats stream over the Engine API — one sample per second
per container, pushed rather than polled — while a scenario runs, and writes a
JSON record to `history/runs/`. It exits non-zero when the run was not worth
quoting: a service without a ceiling, a container throttled beyond its budget,
an OOM kill, or a scenario that failed.

The demo scenario, on 2026-09-12, 16 cpus / arm64 / engine 29.7.2
(`history/runs/baseline-demo-2026-09-12.json`):

| | peak cpus | mean | peak RSS | of its limit |
| --- | --- | --- | --- | --- |
| rmq-daemon (the one running the redrive) | 0.65 | 0.06 | 81 MiB | 16% |
| rabbitmq | 0.51 | 0.13 | 324 MiB | 15% |
| traffic | 0.23 | 0.11 | 31 MiB | 12% |
| envoy (each of three) | 0.12 | 0.05 | 34 MiB | 13% |
| aggregator (each of two) | 0.09 | 0.04 | 75 MiB | 15% |
| rmq-producer | 0.07 | 0.02 | 68 MiB | 13% |
| prometheus | 0.07 | 0.03 | 149 MiB | 19% |
| redis | 0.01 | 0.00 | 10 MiB | 4% |

**The whole stack, in its busiest single second: 1.65 cpus and 1.3 GiB across
19 containers**, while the work queue held 1,730 messages and the fleet made
242 egress calls a second. Summing the per-container peaks gives 3.3 — the
daemon running the redrive and the broker feeding it do not peak in the same
second, so the harness buckets samples by the second they were taken in and
reports an instant that happened rather than an arithmetic one.

One daemon is an order of magnitude above its own mean because exactly one —
the one the broker elected — runs the redrive, and that burst is what the ramp
back is made of. It is the only thing here that needs its ceiling.

**Every memory figure further down this page is an upper bound, not a
measurement of need.** The soak below ran on this stack before it had limits,
and the scale probe runs the aggregator as a bare host process, which has no
container to be limited by at all. Both are V8 sizing its heap from what it can
see: every garbage-collected process here shrank by 28–52% once it could see a
ceiling, and the two that do not collect — Envoy, Redis — did not move. The
before and after is in
[ADR 014](decisions/014-the-measurement-envelope.md).

## At a size nobody runs it at

The section above is this stack: three APIs, three replicas, five daemons,
which is enough to demonstrate the properties and useless for predicting
anything. `--apis=N` replaces the named APIs with N synthetic ones
(`--source=sim` only) and [`infra/scale-probe.mjs`](../infra/scale-probe.mjs)
samples a running instance:

```bash
node --experimental-strip-types packages/aggregator/src/main.ts \
  --source=sim --apis=1000 --replicas=10 --port=8098 --no-webhook &
node infra/scale-probe.mjs http://127.0.0.1:8098 15
```

| APIs × replicas | ticks/s | mean poll | Prometheus series | `/metrics` | `/api/state` | RSS |
| --- | --- | --- | --- | --- | --- | --- |
| 3 × 3 | 4.00 | 0.04 ms | 88 | 7.5 KB in 1.9 ms | 1.8 KB in 2.7 ms | — |
| 50 × 10 | 3.93 | 0.12 ms | 840 | 50 KB in 3.5 ms | 55 KB in 1.5 ms | 199 MB |
| 200 × 10 | 3.93 | 0.54 ms | 3,240 | 186 KB in 5.8 ms | 221 KB in 2.2 ms | 272 MB |
| 1000 × 10 | 3.73 | 2.42 ms | 16,040 | 911 KB in 22.6 ms | 1.1 MB in 5.3 ms | 462 MB |

**The control loop is not the thing that breaks.** A 333× increase in APIs
costs the tick loop 7% of its cadence (4.00 → 3.73 ticks/s) and the poll 2.4ms.
Stepping a thousand breakers is arithmetic on small objects, and it shows.

**What grows is the observability surface**, which is a cost paid in someone
else's system. Sixteen thousand series per instance at 1000 APIs, and a 911 KB
scrape — against this repo's own 2s Prometheus interval, roughly 455 KB/s per
instance, doubled because there are two. Sixteen series per API is the number
to plan with.

**The console breaks first, and by a distance.** `/api/stream` re-sends the
whole state frame every 400ms, so at 1000 APIs each connected browser costs
about **2.75 MB/s** — an order of magnitude more than everything else here put
together. Nothing in the control path notices, which is exactly why it would
be found late. A production console at this size sends diffs, or a page of
APIs, not the fleet.

A soak, of the short kind that is honest to call a soak: 27 minutes on the
compose stack, deliberately not a quiet window — it contained two chaos runs, a
one-sided Redis partition, a Redis outage and several rebuilds. Aggregator RSS
went 98.7 → 103.4 MiB and 99.2 → 105.4 MiB, a daemon 95.7 → 97.1 MiB, with
6,619 ticks, zero gaps and zero duplicates recorded across all of it, and both
queues drained at the end. That rules out a fast leak under disruption and
nothing more: hours, not minutes, is what would say anything about a slow one.

None of the rest was measured beyond one process on one machine either: no
multi-hour run at 1000 APIs, no memory profile over days, and RSS at that size
is a single reading rather than a curve.

## Chaos, on demand rather than by hand

The two adversarial tests this README describes were each performed once,
watched in the logs, and written up — which is a claim with a date on it
rather than a proof, because nothing re-runs it.
[`infra/chaos.mjs`](../infra/chaos.mjs) is the same two experiments with
assertions and an exit code:

```bash
node infra/chaos.mjs leader   # kill the publishing leader mid-incident
node infra/chaos.mjs prober   # kill the daemon the broker elected to probe
```

Run against the compose stack on 2026-09-12, on the scaled fleet:

- **leader** — circuit opened in 2.8s, leader killed at `sequence=127`, standby
  took over in **4820 ms** (the crash path: a killed process hands nothing
  back, so this is the full `leaseTtlMs`, and it is the number
  [releasing the lease on shutdown](high-availability.md) improves on for
  *planned* stops), rehydrated the API from its checkpoint, and resumed at 128
  with no sequence published twice.
- **prober** — the daemon the broker had elected was killed; another was
  promoted **7619 ms** later, and the circuit reached `CLOSED` **15227 ms**
  after the kill with its original prober gone. The fleet is one scaled service,
  so the harness identifies the elected daemon from its own metric label rather
  than being told which container it is.

The harness has needed correcting twice, and both corrections are the same
mistake at different depths — asking an observer a question it cannot answer.

The first version summed gaps and duplicates across **both** instances, so the
run after a kill reported duplicates falling 4 → 0 and `-535` events delivered.
That is not a contract violation, it is a restarted process with fresh
in-memory counters. It reads the surviving instance now.

The second was found by the run above. Each aggregator posts to its *own*
`/subscriber/webhook`, so an instance's view contains only what that instance
published — and a standby publishes nothing. If the survivor ever led before,
its high-water mark is stale by exactly the run of sequences the other instance
published, and the first event it publishes after taking over reads as a jump.
Observed: `jumped 114 -> 123`, reported as a failed check. That is a hole in one
observer's view, not a gap in delivery, and the check was **flaky by
construction** — it passed whenever the survivor happened never to have led.
The question it was asking is answered on the publishing side, where the
guarantee lives: the sequence continued 127 → 128 with nothing published twice.
Only an external subscriber reading both instances could answer it from the
receiving side, and the harness no longer pretends a self-subscriber can.
