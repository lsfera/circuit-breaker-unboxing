# One verdict, five opinions

One producer, one broker, a fleet of competing-consumer daemons, each with
its own [cockatiel](https://github.com/connor4312/cockatiel) circuit
breaker and a shared probe permit — plus, new this branch, a single
aggregator that folds every replica's own breaker transitions into one
published verdict. This is the fourth step in a circuit-breaker article
series, built on `article/03-probe-permit`. Every report in this series has
named the same standing problem: no signal about an outage leaves any one
process. This branch gives the fleet exactly one voice for that — without
changing how any replica protects itself.

That split is deliberate, not a shortcut. Checked `master` (the original,
full system this series incrementally re-derives) for how it actually
solved this, and its own design essay states the reasoning directly:
**protecting the request path and telling the rest of the system about an
outage are different problems with different correct answers.** Sharing
breaker state across replicas *before* deciding whether to call is the
tempting fix and the wrong one — it puts a network round trip and a shared
failure domain in the hot path of the one component whose job is surviving
other people's failures. So the aggregator here computes its verdict from
events published *after* each replica has already decided for itself, off
the hot path, and publishes it for anyone who wants to know — a dashboard,
an alert, another service — never back into any replica's own breaker.

```mermaid
flowchart LR
  producer["Producer"] --> queue[("payments-provider.work")]
  permit[("probe-permit\n(1 token)")]
  control[["circuit.control"]]
  aggregator["aggregator\n(one verdict)"]
  subgraph c1["consumer 1"]
    b1{{"breaker\n(cockatiel)"}}
  end
  subgraph c2["consumer 2"]
    b2{{"breaker\n(cockatiel)"}}
  end
  subgraph c3["consumer N"]
    b3{{"breaker\n(cockatiel)"}}
  end
  queue --> c1
  queue --> c2
  queue --> c3
  b1 <-.->|"half-open only"| permit
  b2 <-.->|"half-open only"| permit
  b3 <-.->|"half-open only"| permit
  b1 --> api[("Third-party API\n(flaky-upstream)")]
  b2 --> api
  b3 --> api
  b1 -.->|"onStateChange"| control
  b2 -.->|"onStateChange"| control
  b3 -.->|"onStateChange"| control
  control --> aggregator
  aggregator -.->|"verdict"| prom[("Prometheus")]
```

Each replica's breaker is still its own `CircuitBreakerPolicy` instance —
nothing draws a line *between* the subgraphs, same as before. `circuit.control`
is a one-way, off-the-hot-path fan-in: every replica publishes to it, only
the aggregator reads it, and nothing reads back from the aggregator into any
consumer subgraph.

## The aggregator

`packages/aggregator` is a small, single-instance service — no scaling, no
leader election, no persistence, a real SPOF this branch names rather than
hides (see "what this still doesn't fix"). Its job, in full:

- Bind one queue to `circuit.control` with routing key `circuit.*` — every
  API this deployment ever runs, not hardcoded to one.
- On each event (`{apiId, instance, state, at}`, published by
  `consumer.ts`'s `onStateChange`), update an in-memory
  `Map<apiId, Map<instance, {state, at}>>`, pruning any instance not heard
  from in `STALENESS_MS` (default 60s) — otherwise a scaled-down or crashed
  replica's last vote would count forever, permanently biasing the fraction
  toward whatever it last reported.
- Compute `openFraction` — the pruned registry's share currently `open` or
  `half_open` (half-open still isn't serving normal traffic, so it counts
  the same as open) — and a verdict: `open` once that fraction reaches
  `VERDICT_THRESHOLD` (default 0.5), else `closed`. Both the fraction and
  the verdict (`packages/aggregator/src/Verdict.ts`) are pure functions,
  unit-tested with no broker involved — same "pull the decision out" shape
  as `consumer.ts`'s own `decide`.
- Publish `egress_fleet_verdict_state` and `egress_fleet_open_fraction` to
  Prometheus, and log every verdict transition.

Restarting the aggregator starts it from "no data yet": the registry is
pure memory, and it repopulates within a few replicas' worth of
transitions. That's an explicit trade for staying single-instance this
branch, not an oversight.

## The permit

## The permit

`packages/consumer/src/Breaker.ts`'s module doc has the full reasoning; the
short version:

Master's original design for this problem (a dedicated `@egress/aggregator`
publishing a canonical verdict, with a `probe-trigger`
single-active-consumer queue for electing which daemon does the actual
probing) is real prior art in this repo's own history, but it's three
separable concerns bundled into one system — an aggregator, a broadcast
verdict, and HA for the aggregator itself. This branch takes the smallest
piece: **stop the burst, without unifying the verdict.**

A bare RabbitMQ `x-single-active-consumer` queue doesn't fit here on its
own — it promotes a new active consumer on *disconnect*, not on "my own
backoff timer just elapsed and I want to try a probe," and there's no
aggregator in this branch to relay a remote probe's result back to whichever
replica asked for it. What does fit: a queue declared with `x-max-length: 1`
and `x-overflow: reject-publish`, seeded with one token by every replica at
startup — RabbitMQ keeps the first publish and rejects the rest with a nack
on the publisher's own confirm. Measured, not assumed: an earlier version of
this branch expected the rejection to be silent and crash-looped four of
five replicas on boot when it wasn't. `seedPermit`
(`packages/consumer/src/Breaker.ts`) swallows that nack deliberately — four
replicas losing this race is the successful outcome, not a failure.

When a replica's own breaker reaches `HalfOpen`, it does a non-blocking
`Rmq.get` against that queue before making the real call:

- **Gets the token** → makes the real call, then hands the token back
  (`nack`, requeuing the same message) regardless of outcome, so the next
  replica whose own backoff elapses can compete for it.
- **Queue was empty** → someone else is mid-probe. Fails immediately with
  `Breaker.NoPermit`, no network call. cockatiel treats this exactly like a
  failed probe: it reopens with the *next* backoff step, and this replica
  tries again next time its own clock elapses.

Closed and Open behavior are unchanged from article 2. This only changes
what a replica does during its own half-open window.

## The breaker

`packages/consumer/src/Breaker.ts` wraps every third-party call in a
cockatiel `CircuitBreakerPolicy`, one instance per process, created once and
reused for the process's whole life (cockatiel's own docs are explicit that
a breaker only works if the same instance sees every execution — a fresh one
per call would never accumulate a failure count).

- **Trip condition**: `ConsecutiveBreaker(BREAKER_THRESHOLD)` — opens after
  `BREAKER_THRESHOLD` (default 5) calls fail in a row. This is the direct
  in-code version of "a threshold on recent failures, never a single
  failure" from Part 1 of the article series.
- **Re-open timing**: `ExponentialBackoff` from `BREAKER_INITIAL_DELAY_MS`
  (default 1000ms) up to `BREAKER_MAX_DELAY_MS` (default 30000ms), doubling
  each time a half-open probe still fails. cockatiel's default backoff
  generator is already decorrelated-jitter — "exponential backoff and
  jitter" is what `new ExponentialBackoff()` gives you without extra
  configuration, not something layered on top.
- **Three outcomes, not two**: `"ok"` (accept), `"failed"` (a real call was
  attempted and failed, requeue), and `"open"` (no call reached the third
  party — either the breaker rejected it outright, or this replica's own
  half-open probe lost the fleet-wide permit race; see below). Both `"open"`
  cases requeue after a short jittered hold.

That hold (100–400ms, `OPEN_REQUEUE_DELAY_MIN_MS`/`MAX_MS` in
`consumer.ts`) exists because an open breaker rejects instantly — with no
hold, a rejected message goes straight back onto the queue and straight back
to the same consumer, which can spin against its own in-memory breaker at
whatever rate the broker will redeliver. The third party stops being
hammered; without the hold, the *broker* takes its place. Same shape as the
100–400ms jittered hold the pre-breaker daemon this series later removed
used for a shed `429`.

## What this still doesn't fix

**The registry's denominator is "replicas that have ever transitioned," not
"the actual fleet."** A replica that's stayed `closed` the whole time has
never published to `circuit.control` at all — `onStateChange` only fires on
a real transition — so it's invisible to the aggregator, not counted as a
healthy vote. Measured live: with 5 replicas all starting closed, the
moment the *first two* tripped open, the registry only knew about those
two, so `openFraction` was 2/2 = 100% and the verdict opened instantly —
0.0s measured lag — rather than waiting for something closer to half the
real fleet. The verdict is honest about what it's heard, not about the
fleet's actual size.

**The verdict doesn't gate anything.** No consumer reads
`egress_fleet_verdict_state` — that's the point, per the design essay
quoted above, but it means a real subscriber (a status page, an alert, a
dependent service) still has to exist for this to matter. Right now the
verdict's only reader is Prometheus and whoever looks at Grafana.

**The aggregator is an unreplicated single point of failure.** One
instance, no leader election, no persistence. If it's down, every
replica's own breaker keeps protecting itself exactly as before — but the
fleet has zero published opinion until it's back, and its in-memory
registry starts over from nothing when it is.

**A dropped `circuit.control` publish is silently lost.** `consumer.ts`
logs and moves on rather than retrying — an honest simplification, not
master's `AmqpControlPlaneSink` (bounded retry, a dead-letter buffer for
inspection). A replica whose one publish attempt fails during a network
blip can leave the aggregator believing it's still in whatever state it
last successfully reported, until its next transition.

**The staleness window and the threshold are both heuristics, not measured
constants.** 60 seconds and 50% are defaults that happened to work for this
scenario's timings; nothing here tunes them against a real distribution of
replica counts or outage shapes.

**A replica that keeps losing the permit race backs off longer, whether or
not the third party is actually still down.** Unchanged from article 3:
cockatiel has no way to tell "my probe failed because the third party is
unhealthy" apart from "my probe never got the permit" — both call
`recordHalfOpenFailure` and grow the same backoff.

**A message can still be dead-lettered without ever reaching the third
party**, and **dead-lettered work still has no way back** — both unchanged
since articles 1–3.

## Running it

```bash
pnpm install
docker compose up -d
docker compose up -d --scale rmq-consumer=12   # resize the fleet, no restart needed
```

- RabbitMQ management UI: <http://localhost:15672> (guest/guest) — watch
  `payments-provider.work`'s depth and `payments-provider.work.dead`'s
  growth.
- Grafana: <http://localhost:3000/d/in-process-breaker/in-process-breaker-e28094-five-not-one>
  — same dashboard as articles 2 and 3, one new panel on top: "Fleet
  verdict," the one calm line `egress_fleet_verdict_state` draws next to
  the five disagreeing ones in "Breaker state per replica" right below it.
  Anonymous viewer access, no login needed. (Plain `:3000` lands on
  Grafana's own "Welcome" screen, not this dashboard — Grafana 13's
  anonymous Viewer role can't be granted the permission a *default home
  dashboard* needs, so there's no way to make `:3000` redirect here without
  requiring login. Use the direct link, or `Dashboards` in the left nav.)
  Panels: fleet verdict, breaker state per replica, work-queue depth,
  dead-letter-queue depth, calls by outcome, breaker trips, active
  consumers.
- Grafana's own nav bar fires two calls (`/api/user/teams`,
  `/api/user/stars`) that need a real signed-in user and 401 for an
  anonymous session — a known rough edge in Grafana's anonymous-auth mode,
  not something this compose file controls. It shows as a stray toast on
  first load; the dashboard and its data are unaffected.
- Prometheus: <http://localhost:9090>.

## Injecting a failure

`flaky-upstream` stands in for the third party. Every field is optional and a
POST replaces the whole behaviour, so `{}` restores health:

```bash
curl -X POST localhost:8080/__fail -d '{"rate":1.0}'                # 503s
curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"hang"}'  # never answers
curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"reset"}' # drops the connection
curl -X POST localhost:8080/__fail -d '{"delayMs":1500}'            # slow, still correct
curl -X POST localhost:8080/__fail -d '{}'                          # healthy again
```

## The incident script

```bash
pnpm run incident
MODE=hang node infra/incident.mjs
```

Injects a full failure, watches the work queue and dead-letter queue for a
fixed window, restores the third party, and reports peak backlog, total
dead-lettered, time to drain, and a processed/duplicate count from
`flaky-upstream`'s own audit trail — same as article 1's version, plus one
thing this branch actually has to measure: at every poll tick it also reads
each replica's `egress_consumer_breaker_state` from Prometheus
(`PROMETHEUS`, default `http://localhost:9090`) and reports what fraction of
ticks saw every replica in the *same* state, and the peak number open at
once. That's the concrete, measured version of "five independent breakers
disagree" — produced by this branch's own run, not asserted.

New this branch: it also reads `egress_fleet_verdict_state` and reports how
many seconds the published verdict lagged behind the first replica to
individually notice the outage (or, if the threshold happens to trip on a
sparser sample before every replica's own state is visible again, how many
seconds it led — that's a polling artifact of two separate Prometheus
queries per tick, not a claim the aggregator somehow knew first).

## Load

The producer's rate is configurable and never reacts to anything:

```bash
RATE_PER_SECOND=500 docker compose up -d rmq-producer
```

Combine with `--scale rmq-consumer=N` and `infra/incident.mjs`'s `RATE`/
`WINDOW_MS` env vars to drive a specific incident shape.

## Layout

```
packages/
  config/      @egress/config — settings declared once, decoded at boot
  rmq/         @egress/rmq — Effect wrapper over amqplib, plus the generic
               work-queue naming/options a producer and a consumer fleet
               share, and (as of this branch) the circuit.control naming
               convention (ControlPlane.ts). `get` (src/Client.ts, article
               3) is what the probe-permit queue runs on.
  rmq-producer/  the load: a steady stream onto <apiId>.work, never backing off
  consumer/    the competing-consumer fleet, each with its own in-process
               breaker (src/Breaker.ts) sharing one probe-permit queue
               during half-open, and now publishing every transition to
               circuit.control — see src/consumer.ts for what that channel
               is and isn't for
  aggregator/  @egress/aggregator — single instance, folds circuit.control
               into one published verdict per apiId (src/Verdict.ts is the
               pure, unit-tested decision; src/aggregator.ts wires it to
               the broker and Prometheus)
  tracing/     the /metrics HTTP route every process serves; OpenTelemetry
               tracing is wired but off unless OTEL_EXPORTER_OTLP_ENDPOINT is set
infra/
  flaky-upstream.mjs   the fake third party: configurable failures, an audit trail
  rabbitmq.conf        the broker's flow-control watermark
  incident.mjs         drives one incident, reports what happened including
                        whether the fleet's breakers agreed and how the
                        published verdict's timing compared
  monitoring/          Prometheus scrape config and the Grafana dashboard
docker-compose.yml     the whole stack
```

Built on **Effect 4 (4.0.0-rc.115)**, same as the full system this branch was
pruned from — see `AGENTS.md` for why that version matters when writing
Effect code here. No build step: every package runs straight off its
`src/*.ts` through Node's built-in type stripping.

## Verification

```bash
pnpm run check       # typecheck + unit tests, including Breaker.test.ts against the real library
pnpm run test:rmq    # optional — needs Docker: AMQP behaviour against a real broker
```
