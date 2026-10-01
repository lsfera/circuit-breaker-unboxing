# Per-API egress circuit breaker events
> [Main overview](https://github.com/lsfera/reasoning-over-circuit-breaker/blob/main/README.md)



The last step of the series: the design at **platform level**. Articles 1–4
stop a fleet of consumers hammering one flaky third party with nothing but
RabbitMQ and Prometheus (`article/03-rabbitmq-coordination`,
`article/04-rabbitmq-only-breaker`): no lost work, one probe at a time, a redrive,
one fleet verdict and backoff on a `429`, in 2,200–2,900 lines. This costs about
three times the code and 19 containers (Envoy for egress, two aggregators with a
lease in Redis or PostgreSQL, a published event stream). Build it only when you need something
articles 1–4 cannot give:

- **other systems act on the verdict** — a producer that stops accepting work,
  a status page, billing. They need a gapless per-API event sequence, not a
  Prometheus rule;
- **you can see the third party's hosts**, so `DEGRADED` (some hosts ejected)
  means something. Behind one load-balanced address it does not;
- **many services call many APIs through one egress path**, so outlier
  detection and concurrency limits belong in a shared proxy, not in each
  service;
- **tens to thousands of APIs**, each with its own breaker.

If none applies, stop at article 3 or 4. And before building any of them, see
[what is available off the shelf](docs/off-the-shelf.md): meshes, gateways,
egress proxies and AI gateways, commercial and open source, judged on the same
three constraints. This repo is Envoy's answer plus what it lacks; the same page
[lays the two side by side](docs/off-the-shelf.md#envoy-alone-and-what-this-repo-adds).

## The problem

For each third-party API, publish an event when it starts failing and when it
recovers, to subscribers outside the request path. Every design lives or dies on
three constraints:

- **One verdict per API, not one per proxy.** Each egress replica samples the
  upstream on its own. That is right for protection and wrong for
  notification: ten replicas give up to ten `circuit_opened` events at ten
  timestamps, and every subscriber has to de-duplicate the fleet's internal
  disagreement.
- **Emission cannot sit in the request path.** A degrading upstream produces
  tens of thousands of per-request rejections; a subscriber wants one event.
  Whatever publishes has to coalesce, buffer with a bound, and shed rather than
  backpressure into the proxy.
- **The stream is a contract, not a feed.** Per-API ordering, a sequence a
  subscriber can check, and full state in each event, so a subscriber joining
  mid-incident is not blind.

| | One verdict per API | Off the request path | Event contract |
|---|---|---|---|
| Breaker library per service (article 2) | no — per process | n/a | build it, per language |
| Library coordinated through the broker (article 3) | yes, as a Prometheus rule | yes | no — a metric, not events |
| Publish from the proxy (Envoy's ejection log) | no — per replica | no | partial: saturation is a counter, not an event |
| Shared breaker state in Redis | yes | **no** — a round trip in the hot path | still to build |
| Gateway (APISIX `api-breaker` + `http-logger`) | per route, not per host | no — the logger fires per request | yes |
| Service mesh | no — per sidecar | yes | metrics, not events |
| **Enforce locally, decide centrally, publish once** | yes | yes | yes |

## What this repo does

Envoy enforces, per replica, immediately, and never makes a call on behalf of
an event. An aggregator watches every replica, resolves their disagreement into
one state per API, and publishes. The daemon fleet consumes those events.

```mermaid
flowchart LR
  classDef leader fill:#dcf3f1,stroke:#0c8b86,stroke-width:3px,color:#04302e,font-weight:bold;
  classDef standby fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,stroke-dasharray: 4 3,color:#5b6478;
  classDef box fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,color:#161d2b;

  subgraph ENFORCE["enforcement — per replica, immediate"]
    e0["envoy-00"]:::box
    e1["envoy-01"]:::box
    e2["envoy-02"]:::box
  end
  up[("third party<br/>(flaky-upstream)")]:::box
  subgraph DECIDE["decision — one breaker per API"]
    lead["aggregator (leader)"]:::leader
    stby["aggregator-2 (standby)"]:::standby
    redis[("redis or postgres<br/>lease · checkpoints · outbox")]:::box
  end
  subgraph FLEET["daemon fleet"]
    ctl{{"circuit.control"}}:::box
    work[("payments-provider.work")]:::box
    d["5 daemons"]:::box
  end

  e0 & e1 & e2 --> up
  e0 & e1 & e2 -->|"stats, gRPC push"| lead
  lead --> redis
  stby -.->|"waits for the lease"| redis
  lead -->|"state_changed · snapshot"| ctl
  lead -->|"webhook + outbox"| sub["subscribers"]:::box
  ctl --> d
  work --> d
  d -->|"one address"| e0
```

- **The aggregator** takes a quorum of what the replicas report, holds it for a
  dwell time, and steps one pure state machine per API
  (`CLOSED → DEGRADED → OPEN → HALF_OPEN`). Only the leader polls, steps and
  publishes; its `OPEN` is observational, never pushed back into Envoy
  ([ADR 002](docs/decisions/002-enforcement-authority.md)).
- **The events** carry a gapless per-API sequence, full state and the
  publishing leader's lease, so a subscriber can detect loss and ignore a
  leader that was paused past its lease.
- **The daemons** consume a RabbitMQ work queue and call the third party
  through Envoy. They learn the circuit from `circuit.control`: a fraction of
  the fleet works while `DEGRADED`, none while `OPEN`, and exactly one message
  is probed during `HALF_OPEN`, by the daemon RabbitMQ elects. Failed calls are
  retried against a per-message budget, dead-lettered, and redriven once the
  circuit closes.

[docs/architecture.md](docs/architecture.md) has the detail;
[docs/high-availability.md](docs/high-availability.md) covers the lease,
fencing, checkpoints and the outbox.

## Running it

```bash
pnpm install
docker compose up -d redis                        # the aggregator always takes its lease in a shared store
pnpm start --coordination=redis://redis:6379      # one aggregator over a simulated fleet, console on :8088
# or PostgreSQL: docker compose --profile postgres up -d postgres, then
# pnpm start --coordination=postgres://egress:egress@postgres:5432/egress
docker compose up --build -d                      # HOST_WORKSPACE_FOLDER: this repo's path on the Docker host
pnpm run demo:envoy                               # an incident, driven through the aggregators' own routes
node infra/chaos-load.mjs --profiles=low --faults=flaky-full-cycle,kill-leader
```

The stack is three Envoy replicas, two aggregators, Redis, RabbitMQ, the
producer, five daemons, the fake third party and a traffic generator, and
Prometheus, Alertmanager, an alert sink and Grafana. To coordinate through
PostgreSQL instead, start it with
`COORDINATION=postgres://egress:egress@postgres:5432/egress docker compose
--profile postgres up -d`; Redis still starts, unused. From the
devcontainer, by service name (from the host, `localhost` with the published
port):
- Console <http://aggregator:8088> and <http://aggregator-2:8088> (`:8088`, `:8089` on the host)
- Grafana <http://grafana:3000/d/egress-circuit-breaker>
- Prometheus <http://prometheus:9090>
- RabbitMQ <http://rabbitmq:15672> (guest/guest)

## Measured

On this stack, 2026-09-24/25, chaos-load at the low profile (200/s with spikes to
3,000/s), judged per message:

| Fault | Lost | Dead-letter queue at end | Recovered after heal |
|---|---|---|---|
| none | 0 | 0 | 0.1 s |
| kill a daemon | 0 | 0 | 3.2 s |
| kill the floor daemon | 0 | 0 | 5.3 s |
| kill the leading aggregator | 0 | 0 | 0.1 s |
| kill the broker | 0 | 0 | 2.2–20 s |
| full outage, then recovery | 0 | 0 | 17–22 s |
| upstream hangs | 0 | 0 | 23 s |
| upstream slower than the timeout | 0 | 0 | 23 s |
| lease partition during an outage | 0 | 0 | 29 s |
| Envoy overflow | 0 | 0 | 0.1 s |

A third party that is full rather than broken (at most 2 calls in flight per
endpoint, 100 ms each, 200/s offered): the daemons' learned concurrency limit
drew 20 `429`s a second against 341 with a fixed limit, for 48/s goodput
against 57/s.

At a thousand APIs one aggregator holds 3.7 ticks/s and 462 MB; what breaks
first is the console, not the control loop. Scale, the resource envelope and
the failover timings are in [docs/measurements.md](docs/measurements.md).

## What is a prototype

- **No security.** No authentication, authorization or TLS on any hop; the
  failure-injection route and the gRPC stats sink accept anything that reaches
  them.
- **HTTPS egress needs TLS interception** for any L7 signal. Through `CONNECT`
  the breaker only sees connection failures.
- **One coordination store, one broker.** Losing the store's data (Redis or
  PostgreSQL) loses the checkpoints: sequences start over and the half-open
  backoff resets. The fencing token's epoch still stops a
  stale leader. Quorum queues on one node survive a restart, not a node loss.
- **The console re-sends the whole fleet** every 400 ms: 2.75 MB/s per browser
  at a thousand APIs ([ADR 015](docs/decisions/015-the-console-at-a-thousand-apis.md)).
- **Nothing has run longer than half an hour.**

## Layout

```
packages/
  domain/        pure: the event schema, the breaker state machine, supersedes()
  aggregator/    tick loop, webhook and AMQP sinks, Envoy push/poll sources,
                 console, /metrics
  coordination/  the ports the aggregator coordinates through: lease and fencing
                 token, checkpoints, outbox
  coordination-redis/     those ports as Lua scripts over Redis
  coordination-postgres/  those ports as SQL over PostgreSQL
  rmq/           amqplib in Effect; queue names, arguments and codecs
  rmq-consumer/  the daemon: pure policy and reducer, Attempts, Limiter, Redrive
  rmq-producer/  the load onto the work queue, message_id as idempotency key
  subscriber/    a standalone consumer of the event stream
  config/        settings declared once, decoded at boot
  tracing/       OpenTelemetry when OTEL_EXPORTER_OTLP_ENDPOINT is set
  demo/          drives the demo incident over HTTP
infra/           envoy.yaml, flaky-upstream, chaos harnesses, monitoring
docs/            architecture, high availability, measurements, off the shelf, decisions/
```

**Effect 4 (4.0.0-rc.117)**; see `AGENTS.md`. No build step: Node runs the
`src/*.ts` directly, which makes `tsc --noEmit` load-bearing.

```bash
pnpm run check       # vendored version, typecheck, unit tests
pnpm run test:rmq    # needs Docker: against a real broker
pnpm run test:redis     # needs Docker: coordination and the outbox against a real Redis
pnpm run test:postgres  # needs Docker: the same suite against a real PostgreSQL
```

> [Main overview](https://github.com/lsfera/reasoning-over-circuit-breaker/blob/main/README.md)
