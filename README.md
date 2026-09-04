# Per-API egress circuit breaker events

Services call third-party APIs through a shared egress path. When one of those
third parties degrades, everything downstream needs to be **told** — per API,
as discrete events, reliably enough to act on.

That sounds like a circuit breaker, and a breaker is part of it. The
interesting half is that protecting the request path and telling the rest of
the system about it are different problems with different correct answers,
and most of the obvious designs conflate them.


Who wants what from it:

```mermaid
flowchart LR
  classDef actor fill:#eaeef6,stroke:#5b6478,stroke-width:2px,color:#161d2b;
  classDef enforce fill:#fbe2dd,stroke:#c9432f,stroke-width:2px,color:#3a1c17;
  classDef publish fill:#dcf3f1,stroke:#0c8b86,stroke-width:2px,color:#04302e;
  classDef react fill:#fbedd6,stroke:#c07f16,stroke-width:2px,color:#3a2c12;
  classDef ext fill:transparent,stroke:#5b6478,stroke-width:1.5px,stroke-dasharray:4 3,color:#5b6478;

  caller["Calling service"]:::actor
  worker["Queue worker fleet"]:::actor
  subscriber["Subscriber<br/>status page · billing · incident tooling"]:::actor
  operator["Operator"]:::actor

  subgraph SYSTEM[" "]
    direction TB
    u1(["Reach a third party<br/>through one address"]):::enforce
    u2(["Stop hammering it<br/>once it is failing"]):::enforce
    u3(["Be told when an API<br/>breaks and recovers"]):::publish
    u4(["Detect a missed<br/>or repeated event"]):::publish
    u5(["Scale my own work<br/>to the agreed state"]):::react
    u6(["Recover work that<br/>failed during the outage"]):::react
    u7(["See why the fleet<br/>decided that"]):::publish
  end

  third[("Third-party API")]:::ext

  caller --> u1
  caller --> u2
  worker --> u5
  worker --> u6
  worker --> u3
  subscriber --> u3
  subscriber --> u4
  operator --> u7
  operator --> u3
  u1 --> third
  u2 -.->|"ejects failing hosts"| third
```

Red is enforcement, in the request path and immediate. Teal is publication,
off the request path and seconds later. Amber is what a *consumer* of those
events does with them — and it is a first-class actor here, not an
afterthought: the fleet that reacts to an outage is where the cost of getting
this wrong actually lands.

## The problem

Concretely, the requirement is: for each third-party API, publish an event
when it starts failing and when it recovers, to subscribers who were not in
the request path. Three constraints fall out of that, and every design below
lives or dies on them.

**One verdict per API, not one per proxy.** Egress runs behind more than one
proxy instance, and each one samples the upstream independently. That is
correct for protection — a replica should eject a host it can see is bad,
immediately, without asking anyone. It is wrong for notification: with ten
replicas and a degrading upstream you get up to ten `circuit_opened` events at
ten timestamps, then recoveries out of step. A subscriber sees an API flap
several times for a single incident and has to de-duplicate a distributed
system's internal disagreement on its own.

**Emission cannot sit in the request path.** At egress throughput no proxy
holds its p99 while making an outbound POST per event, and a degrading
upstream generates tens of thousands of per-request rejections when what a
subscriber wants is one `circuit_opened`. Whatever publishes has to coalesce,
buffer with a bound, and shed rather than backpressure into the proxy.

**The stream has to be a contract, not a feed.** Per-API ordering, a sequence
a subscriber can check, and enough state in each event that a consumer joining
mid-incident is not blind. Otherwise every subscriber reimplements
loss-detection, differently and mostly wrongly.

## Approaches, and where each one runs out

These are the real options, in roughly increasing order of how much you build.
Each is defensible; each fails one of the three constraints above.

**A breaker library in every service** — Resilience4j, Polly, opossum,
gobreaker. Mature, well understood, and the state lives in a variable in one
process. Ten instances of a service hold ten independent opinions about the
same third party, and two *different* services calling it learn separately.
There is also nothing to publish from: you would add an event path to every
service, in every language you run. Fails constraint one, at the worst
possible granularity.

**Publish straight from the proxy.** Envoy already emits discrete ejection
records (`outlier_detection.event_log_path`); point them at a webhook and
you are done in an afternoon. Except those records are *per replica*, so the
flapping above is exactly what subscribers get — and the most important
signal isn't in there at all: threshold saturation is a counter delta
(`*_overflow`), not an event. Fails constraints one and two.

**Share breaker state across replicas in Redis** so they agree before anyone
publishes. This is the tempting fix, and it puts a network round trip and a
shared failure domain in the hot path of the component whose entire job is
surviving other people's failures. Fails constraint two, in the direction that
hurts most.

**A batteries-included gateway — Apache APISIX.** Genuinely less to build:
`api-breaker` for breaking, `http-logger` to POST JSON to an endpoint (a
literal webhook sink, no bridge), etcd for cluster config, a forward-proxy
plugin for egress. The trade is that `api-breaker` is per-route and much
cruder than outlier detection — no per-host ejection, no success-rate
statistics — it is Lua/OpenResty, and `http-logger` fires *per request*, which
is precisely the hot-path coupling constraint two rules out.

**A service mesh.** Istio's outlier detection is Envoy's, configured through
`DestinationRule`, so the enforcement is the same quality. But it runs per
sidecar, which leaves the aggregation problem exactly where it was, and mesh
telemetry is metrics-shaped rather than a discrete per-API event contract.
Egress through a mesh means an egress gateway anyway, so you arrive at this
repo's topology having also adopted a mesh.

**Build the proxy — Pingora.** It would give precisely the breaker and event
semantics wanted, and Cloudflare's cache runs on it, so it is proven at scale.
But it is a library, not a proxy: no config plane, no admin API, no
clustering. River is not a product yet. That is a quarter of engineering to
arrive where Envoy starts.

| | One verdict per API | Off the request path | Event contract |
|---|---|---|---|
| Breaker library per service | no — per process | n/a | build it yourself, per language |
| Publish from the proxy | no — per replica | no | partial: saturation isn't an event |
| Shared state in Redis | yes | **no** — round trip in the hot path | still to build |
| APISIX | per route, not per host | no — `http-logger` is per request | yes, out of the box |
| Service mesh | no — per sidecar | yes | metrics, not events |
| Pingora | yes | yes | yes — and you build all of it |
| **Enforce locally, publish centrally** | yes | yes | yes |

## What this repo does instead

Split the two jobs and let each be correct on its own terms. Envoy enforces,
per replica, immediately, and never makes a call on behalf of an event. A
control-plane aggregator watches all the replicas, resolves their
disagreement into one state per API, and publishes.

| | Enforces | Publishes | Latency |
|---|---|---|---|
| Envoy, per replica | yes, immediately | no | sub-second |
| Aggregator, one machine per API | no — observational here (could push config; see [The fork this defers](#the-fork-this-defers)) | yes | seconds |

```mermaid
flowchart LR
  classDef ok fill:#e2f5e8,stroke:#2e9e52,stroke-width:2px,color:#123423;
  classDef deg fill:#fbedd6,stroke:#c07f16,stroke-width:2px,color:#3a2c12;
  classDef down fill:#fbe2dd,stroke:#c9432f,stroke-width:2px,color:#3a1c17;
  classDef gate fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,color:#161d2b;
  classDef out fill:#dcf3f1,stroke:#0c8b86,stroke-width:3px,color:#04302e,font-weight:bold;

  r0["envoy-00<br/>vote: DOWN"]:::down
  r1["envoy-01<br/>vote: OK"]:::ok
  r2["envoy-02<br/>vote: DEGRADED"]:::deg
  r3["envoy-03<br/>vote: DOWN"]:::down
  r4["envoy-04<br/>vote: DEGRADED"]:::deg
  q{{"quorum ≥ 60% impaired<br/>AND held 2s (dwell)"}}:::gate
  out(["state_changed<br/>seq=1 · DEGRADED"]):::out

  r0 --> q
  r1 --> q
  r2 --> q
  r3 --> q
  r4 --> q
  q -->|"4 of 5 impaired (80%)"| out
```

Five independent proxies, three different verdicts at the same instant — none
of them wrong, outlier detection is deliberately local per replica. The
aggregator counts votes against a quorum and waits for that count to hold for
`dwellMs` before publishing anything; disagreement never reaches a subscriber.

The console makes this visible. The coloured strip on each API is one block per
replica showing that replica's local view. Drive an upstream to a partial
failure rate and you will see the blocks disagree while the published state
stays steady.

### Three components, because the constraints say three

| | Owns |
|---|---|
| **Envoy** | enforcement: local, immediate, per replica. Never makes a call on behalf of an event. |
| **Aggregator** (this repo) | fleet-wide state per API, sequencing, coalescing, idempotency, retry, dead-lettering, load shedding. |
| **Broker / subscribers** | fanout, replay, subscriber state. |

Coalescing is most of the value: a degrading upstream produces tens of thousands
of per-request `UO` rejections, and what a subscriber wants is one
`circuit_opened`. The aggregator's buffers are bounded and shed load rather than
backpressuring into the proxy. In production the first hop is a broker (Kafka,
NATS) with webhook delivery as a consumer of it, so replay is the broker's
problem; `WebhookSink` here stands in for that hop.

```mermaid
flowchart LR
  classDef leader fill:#dcf3f1,stroke:#0c8b86,stroke-width:3px,color:#04302e,font-weight:bold;
  classDef standby fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,stroke-dasharray: 4 3,color:#5b6478;
  classDef box fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,color:#161d2b;
  classDef broker fill:transparent,stroke:#5b6478,stroke-width:1.5px,stroke-dasharray:4 3,color:#5b6478;

  subgraph ENFORCEMENT["ENFORCEMENT — per replica, immediate"]
    direction TB
    e0["envoy-00<br/>outlier_detection"]:::box
    e1["envoy-01<br/>outlier_detection"]:::box
    e2["envoy-02<br/>outlier_detection"]:::box
    up[("flaky-upstream")]:::box
    e0 --- up
    e1 --- up
    e2 --- up
  end

  subgraph AGGREGATION["AGGREGATION — one machine per API"]
    direction TB
    lead["aggregator — LEADER<br/>Breaker.step (pure)"]:::leader
    stby["aggregator-2 — standby<br/>polls nothing, publishes nothing"]:::standby
    redis[("redis<br/>lease · fencing token · checkpoint")]:::box
    lead -->|renew + checkpoint| redis
    stby -.->|tryAcquireOrRenew → blocked| redis
  end

  subgraph PUBLICATION["PUBLICATION — fanout, replay"]
    direction TB
    bus["EventBus<br/>console · SSE"]:::box
    hook["WebhookSink<br/>retry · dead-letter · shed"]:::box
    broker[["broker (Kafka/NATS)<br/>stands in for"]]:::broker
    sub["subscriber"]:::box
    hook --> broker --> sub
  end

  e0 -->|"poll :9901/stats, 250ms"| lead
  e1 -->|"poll :9901/stats, 250ms"| lead
  e2 -->|"poll :9901/stats, 250ms"| lead
  lead -->|"state_changed<br/>seq, previousState"| bus
  lead -->|state_changed| hook
```

Only the node in teal is doing anything at a given moment. The standby holds
one connection to Redis and nothing else — see
[High availability](#high-availability) for what makes that safe.

## Running it

```bash
pnpm install
pnpm start           # simulated 5-replica fleet
pnpm run check       # typecheck + 48 tests
pnpm run test:redis  # optional — needs Docker: HA coordination against a real Redis
pnpm run test:rmq    # optional — needs Docker: AMQP behaviour against a real broker
```

Then open <http://localhost:8088>. For the full thing — three real Envoy
replicas, two aggregators, a real broker and the daemon fleet — see
[Running against real Envoy](#running-against-real-envoy).

Built on **Effect 4 (4.0.0-rc.112)**. Requires Node 22.6+ and TypeScript 5.9+.
TypeScript runs natively via Node's type stripping, so there is still no build
step — but `tsc --noEmit` is now load-bearing, because Effect's guarantees are
type-level.

> Effect 4 is a release candidate. Versions are pinned exactly (`effect` and
> `@effect/platform-node` ship in lockstep at the same version now) because RC
> APIs still move — `ServiceMap` was renamed back to `Context`, and `Effect.fork`
> was replaced by explicit `forkChild`/`forkScoped`/`forkIn`, between beta and
> rc.112.

## Why Envoy for the data plane

Having ruled out the alternatives above, the reason it is Envoy specifically:
it is the only option in this class with genuinely independent layers, which
matters because "circuit breaking" on egress is really several different
problems that get given one name:

- **`circuit_breakers` thresholds** — `max_connections`,
  `max_pending_requests`, `max_requests`, `max_retries`,
  `max_connection_pools`, per priority. This is *bulkheading*: it caps resource
  consumption and detects nothing. `track_remaining: true` turns overflow into a
  gauge instead of a bare counter.
- **`outlier_detection`** — the actual breaker. Ejects hosts on
  `consecutive_5xx`, `consecutive_gateway_failure`,
  `failure_percentage_threshold`, or success-rate deviation. The `enforcing_*`
  knobs let ejection ramp in gradually rather than flipping on at 100%.
- **Active health checking** — an out-of-band probe per host, which is what
  makes `successful_active_health_check_uneject_host` mean anything. Without
  it that flag is inert and a recovered upstream serves its full
  `base_ejection_time × ejection_count` sentence anyway; with it, a host that
  passes its next check is un-ejected immediately. Measured on the running
  stack: **3.1s to un-eject six hosts carrying ~25s of accumulated backoff.**
- **`retry_budget`** — caps concurrent retries as a fraction of active requests.
  Without it, outlier ejection plus retries amplifies load onto whatever hosts
  are left. Not in `infra/envoy/envoy.yaml`, which uses the older `max_retries`
  threshold; a budget is the better shape at real fleet size.
- **`adaptive_concurrency` filter** — infers a concurrency limit from observed
  latency (a gradient controller against a periodically recalculated minimum
  RTT) instead of a hand-picked static number. It sits in the shared
  `http_filters` chain ahead of the router, so it applies fleet-wide across
  every cluster on the listener. For third-party egress, where you do not know
  the upstream's real capacity, that adapts where a fixed `max_requests`
  threshold would either be too conservative or trip too late. It is a
  request-shedding layer, not a breaker: it does not eject hosts or feed
  `ReplicaReport`, so it does not show up in the aggregator's published states —
  only `circuit_breakers` overflow and `outlier_detection` do.

These layers are also why the config is worth reading rather than copying:
`infra/envoy/envoy.yaml` turns each of them on deliberately, and the section
on [running against real Envoy](#running-against-real-envoy) explains what
each one is doing in the demo. The alternatives to Envoy itself are covered
in [Approaches, and where each one runs out](#approaches-and-where-each-one-runs-out).

## Demo script

1. **Steady state.** Three APIs, five replicas, all `CLOSED`.
2. **Drag payments-provider to ~45%.** Replica blocks start diverging — the
   label reads "5 replicas disagree". The published state moves to `DEGRADED`
   once a quorum agrees, and *stays there*. One event, not five.
3. **Drag to 100%.** Every replica ejects every host. `OPEN`, reason
   `all endpoints ejected`.
4. **Watch it probe.** After the open window it moves to `HALF_OPEN`; the
   upstream is still dead so it fails the probe, returns to `OPEN`, and doubles
   its backoff. It does not hammer a dead upstream.
5. **Hit Restore.** Active health checks un-eject hosts, the next probe
   succeeds, and it closes. Total lifecycle ~20s.
6. **Check the right-hand panel** throughout: sequence gaps and duplicates both
   stay at zero. That is the delivery contract holding.
7. **Watch the daemon fleet react** (real-Envoy stack only): the five
   consumers stop pulling on `OPEN`, the work queue builds, and they ramp
   back `1 → 4 → 5` as it recovers — with no coordination between them. See
   [The RabbitMQ daemon fleet](#the-rabbitmq-daemon-fleet).

A seventh thing worth trying by hand, because it is the state hardest to
reach on purpose: fail *some* of an API's endpoints rather than all of them.
`curl -X POST localhost:8080/__fail -d '{"rate":1.0}'` takes down one of
payments-provider's six, every replica ejects that host, and the fleet
reports `DEGRADED` at `4/6` healthy — partial ejection, which is what
`DEGRADED` exists to describe.

```mermaid
flowchart LR
  classDef ok fill:#e2f5e8,stroke:#2e9e52,stroke-width:2px,color:#123423;
  classDef deg fill:#fbedd6,stroke:#c07f16,stroke-width:2px,color:#3a2c12;
  classDef down fill:#fbe2dd,stroke:#c9432f,stroke-width:2px,color:#3a1c17;
  classDef gate fill:#eaeef6,stroke:#5b6478,stroke-width:1.5px,color:#161d2b;
  classDef out fill:#dcf3f1,stroke:#0c8b86,stroke-width:3px,color:#04302e,font-weight:bold;

  subgraph LIFECYCLE["published state — one event per real transition, ~20s end to end"]
    direction LR
    c0["① CLOSED<br/>5 replicas agree"]:::ok
    d1["DEGRADED<br/>seq=1 · OUTLIER_EJECTION"]:::deg
    o2["OPEN<br/>seq=2 · ALL_ENDPOINTS_EJECTED"]:::down
    h3["HALF_OPEN<br/>seq=3 · OPEN_TIMEOUT_ELAPSED"]:::gate
    o4["OPEN<br/>seq=4 · PROBE_FAILED<br/>backoff 4s → 8s"]:::down
    h5["HALF_OPEN<br/>seq=5"]:::gate
    c6["CLOSED<br/>seq=6 · PROBE_SUCCEEDED"]:::ok

    c0 -->|"② failure 45% — replicas diverge<br/>quorum 60%, held for dwellMs"| d1
    d1 -->|"③ failure 100%<br/>every replica ejects every host"| o2
    o2 -->|"④ openMs elapses"| h3
    h3 -->|"upstream still dead"| o4
    o4 -->|"⑤ Restore — active health<br/>checks un-eject hosts"| h5
    h5 -->|"probeSuccesses healthy observations"| c6
  end

  panel(["⑥ subscriber's view<br/>received=14 · gaps=0 · duplicates=0"]):::out
  LIFECYCLE -.->|"every state_changed, checked outside the process"| panel
```

Six published events for an incident that produced tens of thousands of
per-request rejections and five replicas' worth of disagreement. The two
passes through `HALF_OPEN` are the same code path — only the backoff differs,
which is what keeps step ④ from hammering a dead upstream.

Run `pnpm run subscribe` in a second terminal for a consumer's view of the same
stream.

### Running it hands-free

```bash
pnpm run demo                    # payments-provider, against localhost:8088
pnpm run demo -- shipping-rates  # a different API
```

`packages/demo/src/driver.ts` drives exactly the steps above through the same
`/api/failure` route the console's slider calls, and narrates every published
transition as `/api/events` reports it — so a demo is one command in a second
terminal, and the console (or the [Grafana dashboard](#metrics--monitoring)) is
what the audience actually watches. Nothing about the incident is scripted or
mocked: the driver only sets the failure rate and waits for the real aggregator
to publish, on the real wall clock. It ends by reading `/api/subscriber` and
failing loudly if a gap or duplicate shows up — the automated form of step 6.

Two details that only matter once the stack is real. `AGGREGATOR` takes a
comma-separated list, because only the leader publishes and which instance
that is depends on who won the lease — the driver asks each candidate and
drives the one that answers `isLeader`. And if `PROMETHEUS` is set, it also
asserts step 7 from the fleet's own metrics: that every daemon stopped on
`OPEN`, that the backlog it built drained afterwards, and that the per-API
sequence contract held on the AMQP transport too. Unset, that step is
skipped rather than failed.

```
== Drag payments-provider to 45% ==
  seq=1   CLOSED     -> DEGRADED   OUTLIER_EJECTION

== Drag payments-provider to 100% ==
  seq=2   DEGRADED   -> OPEN       ALL_ENDPOINTS_EJECTED

== Watch it probe — upstream is still dead, so this reopens with doubled backoff ==
  seq=3   OPEN       -> HALF_OPEN  OPEN_TIMEOUT_ELAPSED
  seq=4   HALF_OPEN  -> OPEN       PROBE_FAILED

== Hit Restore ==
  seq=5   OPEN       -> HALF_OPEN  OPEN_TIMEOUT_ELAPSED
  seq=6   HALF_OPEN  -> CLOSED     PROBE_SUCCEEDED

== Delivery contract, read from outside the process ==
  received=14 snapshots=8 duplicates=0 gaps=0
  gapless and non-repeating through the full incident.
```

Against the real stack, with `PROMETHEUS` set, it also prints the other half —
this is a verbatim run:

```
== The daemon fleet reacts — no coordination, same events ==
  target=0/5 pulling=0 work=290 dead-lettered=2343
  nothing is calling the dead upstream; the backlog is the point.

== Fleet: ramp back, drain, and the same contract on AMQP ==
  target=5/5 pulling=5 work=0 dead-lettered=2343 (deepest backlog seen: 2170)
  the same per-API sequence guarantee held on the AMQP transport too,
  checked by 5 consumers the publisher does not control.
```

## Metrics & monitoring

Every metric is plain `effect` `Metric` (`packages/aggregator/src/Telemetry.ts`) — the aggregator's
tick loop, the webhook sink, and the subscriber route all update the same
in-process registry, and `GET /metrics` formats it as Prometheus text. That
endpoint is live on the same port in both modes (`--source=sim` or
`--source=envoy`), so `curl localhost:8088/metrics` works the instant
`pnpm start` is up — no extra process, no extra port.

| Metric | What it shows |
|---|---|
| `egress_circuit_state` | Published state per API, 0=CLOSED…3=HALF_OPEN — a stepped line, not an inference from logs |
| `egress_circuit_healthy_endpoints` / `_total_endpoints` | Fleet-averaged endpoint counts per API |
| `egress_circuit_reporting_replicas` | Replicas still within `replicaTimeoutMs` — what quorum is computed against |
| `egress_circuit_ejections_active` | Ejected hosts summed across replicas — the one signal that separates "outlier detection ejected a host" from "membership changed" |
| `egress_circuit_transitions_total` | Published `state_changed` events, by API/state/reason |
| `egress_circuit_snapshots_total` | Periodic full-state republishes, by API |
| `egress_fleet_poll_duration_ms` | Time to poll and parse every replica once per tick |
| `egress_aggregator_is_leader` | 1 if this instance currently holds the publishing lease, 0 otherwise — see [High availability](#high-availability) |
| `egress_aggregator_fencing_conflicts_total` | Checkpoint writes rejected because a newer lease holder already took over, by API |
| `egress_webhook_delivered_total` / `_failed_total` / `_dead_lettered_total` | Sink outcomes, by API |
| `egress_webhook_delivery_duration_ms` | Successful-delivery latency, including retries |
| `egress_subscriber_events_received_total` / `_gaps_total` / `_duplicates_total` | The delivery contract, read from outside the process — same numbers the console's right-hand panel shows, as counters |

And from the daemon fleet — the same in-process `effect` registry, served on
`METRICS_PORT` by every daemon and by the producer:

| Metric | What it shows |
|---|---|
| `egress_daemon_circuit_state` | The state each daemon *received*, against `egress_circuit_state`, the state the aggregator *published*. They should be indistinguishable |
| `egress_daemon_target_active` / `_self_active` / `_fleet_size` | The agreed active count, and whether this particular daemon is one of them |
| `egress_daemon_calls_total` | Third-party calls through the egress listener, by outcome |
| `egress_daemon_in_flight` / `_queued` | Concurrency against the per-daemon ceiling, and deliveries parked behind it — unsettled, which is where backpressure becomes the broker's problem |
| `egress_daemon_dead_lettered_total` | Work rejected onto `<apiId>.work.dead` because its call failed |
| `egress_daemon_redriven_total` | Dead-lettered work replayed onto the work queue after recovery — the two together are the round trip |
| `egress_daemon_undecodable_total` | Messages the fleet could not read — a control event failing the published schema, a malformed election trigger — rejected onto the canonical dead-letter queue rather than logged and dropped |
| `egress_daemon_control_events_total` / `_gaps_total` / `_duplicates_total` | The same per-API sequence contract, checked on the AMQP transport by five processes the publisher does not control |
| `egress_daemon_probes_total` | `HALF_OPEN` probes this daemon was elected by the broker to run |
| `egress_producer_published_total` | Arrival rate, against the fleet's completion rate — the difference is the queue |

### Watching it live

```bash
docker compose up prometheus grafana
```

Grafana at <http://localhost:3000> (anonymous, pre-provisioned — no login) opens
straight to the **Egress circuit breaker** dashboard: a state timeline per API,
healthy/total endpoints, transitions, webhook delivery and latency, the two
delivery-contract stat tiles that should read zero through an entire incident,
an aggregator-leadership timeline (one line per instance — see
[High availability](#high-availability)) and its fencing-conflict counter, and
— scraped directly from each Envoy's own `/stats/prometheus` — the raw
per-replica healthy-host count, so you can see the disagreement the console's
replica strip visualizes, in a second tool, at the same time.

A **RabbitMQ daemon fleet** row sits underneath it, so the reaction is on the
same screen as the cause: work-queue and dead-letter depth (from RabbitMQ's
own `rabbitmq_prometheus`, enabled by default in the management image on
15692), arrival rate against completion rate, each daemon's own view of the
circuit state, and the AMQP-side delivery-contract tiles. The panel worth
knowing is **agreed target vs actually pulling** — every daemon derives the
same target from the same events, so those two lines track each other, and
when they stop tracking, a daemon has gone deaf while still looking healthy.
Finding that by hand once is what put the panel there.

This works for *either* demo mode: [infra/monitoring/prometheus.yml](infra/monitoring/prometheus.yml)
scrapes both `aggregator:8088` (the `docker compose up` / real-Envoy path) and
`host.docker.internal:8088` (a `pnpm start` sim fleet running on the host), so
you can run just `docker compose up prometheus grafana` alongside `pnpm start`
without bringing up Envoy at all. Prometheus scrapes every 2s, so an incident
you trigger in the console shows up within a point or two.

## Event contract

```json
{
  "specversion": "1.0",
  "type": "egress.circuit.state_changed",
  "source": "egress-proxy/control-plane",
  "subject": "api://payments-provider",
  "id": "01J8...",
  "time": "2026-09-02T14:22:31.004Z",
  "data": {
    "apiId": "payments-provider",
    "sequence": 47,
    "previousState": "CLOSED",
    "state": "OPEN",
    "reason": "ALL_ENDPOINTS_EJECTED",
    "healthyEndpoints": 0,
    "totalEndpoints": 6,
    "observedSince": "2026-09-02T14:22:18.900Z",
    "reportingReplicas": 9
  }
}
```

Four properties the middleware hop depends on:

- **Partitioned by `apiId`,** never by event type. Ordering per API is the only
  ordering subscribers need and the only one that is cheap to guarantee.
  `WebhookSink` sends it as `ce-partitionkey`; on Kafka it is the message key.
- **Full state, not a delta.** A subscriber that joins mid-incident or drops a
  message can sync from any single event. `sequence` plus `previousState` makes
  loss detectable rather than silent.
- **Snapshots as well as transitions.** `egress.circuit.snapshot` republishes
  current state per API on a timer, so a late subscriber is not blind until the
  next incident. On a compacted Kafka topic keyed by `apiId` this gives
  current-state-on-subscribe for free. Snapshots deliberately repeat the current
  sequence; only `state_changed` carries the gapless guarantee.
- **`DEGRADED` is a published state, not an inference.** Binary open/closed
  throws away real information once an API has many endpoints: "3 of 10 hosts
  ejected" and "the API is gone" call for different reactions. The extra state
  costs almost nothing in the machine and saves every subscriber from deriving
  it from `healthyEndpoints` — badly, and in three different ways.

`WebhookSink` never blocks the control loop: bounded retry with backoff, a
dead-letter list, and load shedding above a concurrency ceiling.

## What Envoy actually emits

Half the aggregator exists because the signals differ in kind:

| Signal | Source | Discrete event? |
|---|---|---|
| Host ejected / un-ejected | `outlier_detection.event_log_path`, JSON lines | yes |
| Health-check state change | health check event log | yes |
| Per-request breaker rejection | gRPC Access Log Service, response flag `UO` | yes |
| Threshold saturation | `*_overflow` stats counters | **no** |

That last row is the sharp edge: "the circuit breaker tripped" is not an event in
Envoy, it is a counter delta. `ReplicaReport.overflowTotal` is monotonic for
exactly that reason, and the breaker edge-detects on it.

The state machine is fed from cluster stats, which map onto `ReplicaReport`
one-for-one:

```
cluster.<api>.membership_healthy / membership_total   -> healthy / total
cluster.<api>.outlier_detection.ejections_active      -> ejectionsActive
upstream_rq_pending_overflow + upstream_cx_overflow
  + upstream_rq_retry_overflow                        -> overflowTotal
```

## Running against real Envoy

```bash
docker compose up
```

Needs `$HOST_WORKSPACE_FOLDER` set to this repo's path *on the Docker host*,
not inside whatever container you're running this from — every service
bind-mounts something from the repo, and Docker's file-sharing permission
check is keyed on the host path, not a container-internal one. In a
devcontainer this is normally already exported correctly; overriding it with
a container-internal path (e.g. `/workspace`) produces "mounts denied...not
shared from the host" even though Docker itself is working fine. If your own
shell can't reach the published ports afterward (`curl localhost:8088`
hangs or refuses), you're likely on a different Docker network than the
compose project's — every service here also joins the host devcontainer's
own external `devcontainer` network for exactly that reason, so
`curl http://aggregator:8088/metrics` from a shell on that same network
works without needing a published port at all.

Startup is ordered rather than raced: a one-shot `deps` service installs the
workspace and everything running from `packages/` waits on it having
*completed*. Eight containers racing `pnpm install` against one bind-mounted
`node_modules` used to make the losers fail on pnpm's store lock, which four
of them papered over with a retry loop; the dependency is real, so it belongs
in `depends_on`. Every long-running service also carries `restart: unless-stopped`, which is
what makes the crash-fast stance in `rmq-consumer/src/main.ts` coherent —
letting exactly one library race through an `uncaughtException` handler and
treating everything else as fatal only makes sense if "fatal" means "comes
back", and without a policy it meant "stays dead".

Worth knowing exactly what that does and does not cover, because the demo
depends on the distinction: Docker treats an operator `docker kill` as a
manual stop and does **not** restart it — which is precisely what the SAC
failover step below needs, since the point is that the killed prober stays
gone and the broker promotes another. The policy covers the other case, a
process that exits on its own. (The policy being applied is verifiable with
`docker inspect -f '{{.HostConfig.RestartPolicy.Name}}'`; synthesising a real
crash from outside is not, because PID 1 ignores `SIGKILL` from inside its
own namespace.)

Three Envoy replicas share the upstreams, so they diverge on their own.
`infra/traffic-generator.mjs` runs as part of this stack for a reason worth being
explicit about: outlier detection only reacts to requests it actually sees, so
without traffic flowing through the egress listener, setting a failure rate on
`flaky-upstream` changes nothing at all. The generator sends a steady trickle
through every replica so a real outage shows up the way production traffic
would.

It is configured with exactly one address, `EGRESS_ADDR` — the same one any
real client would be given — and never hardcodes replica names. `envoy` is a
DNS alias shared by all three `envoy-*` containers (`docker-compose.yml`), so
resolving that one hostname is what discovers the replicas; the generator
fans out to whatever comes back. Topology awareness stays where it belongs:
the aggregator, which polls `envoy-00/01/02`'s admin ports individually
because per-replica polling is its actual job — nothing that merely *calls*
the API needs to know there are three of them, or what they're named.

Drive one, by hand:

```bash
curl -X POST localhost:8080/__fail -d '{"rate":1.0}'
```

or hands-free, the same driver used for the sim fleet:

```bash
pnpm run demo:envoy                    # payments-provider
pnpm run demo:envoy -- shipping-rates  # a different API
```

This posts to `flaky-upstream`'s `/__fail` instead of the console's
`/api/failure` — real Envoy has no failure-injection hook of its own, it only
reacts to what the upstream actually returns — but everything downstream
(waiting on `/api/events`, the delivery-contract check) is the identical code
path. There is also a fully containerized version that needs nothing on the
host but Docker: `docker compose --profile demo run --rm demo`.

### The RabbitMQ daemon fleet

`docker compose up` also brings up the scenario in
[docs/rmq-control-plane.md](docs/rmq-control-plane.md): a `rabbitmq` broker,
one `rmq-producer` publishing 200 messages/second onto
`payments-provider.work`, and five `rmq-daemon-*` containers draining it —
one third-party call per message, through the same egress listener, with no
knowledge of Envoy's topology. The aggregator publishes every transition to
the `circuit.control` exchange (`--rmq=rabbitmq:5672`), and each daemon
decides *for itself* whether to keep consuming, from its own index and the
agreed state alone.

Five separate containers rather than one process simulating five, for the
same reason there are three real Envoy replicas: the daemons have to be
independently killable, and the `HALF_OPEN` prober is elected by RabbitMQ's
`x-single-active-consumer` across real connections.

Work whose third-party call fails is **rejected onto `<apiId>.work.dead`**,
not accepted — and then, when the circuit closes again, replayed. There is deliberately no retry in front of that: the client's
`requeue` sends `modified{delivery_failed: false}`, and RabbitMQ only
increments AMQP 1.0's `delivery-count` for a delivery marked *failed* — so a
released message comes back looking brand new, forever, and a redelivery
budget that survives the message moving to another daemon cannot be
expressed at all. Measured, not assumed, and pinned by a test that will fail
if a client release fixes it. One attempt then dead-letter is what is honest
given that; the failures are at least countable and replayable instead of
gone.

**Every queue in the fleet dead-letters to that one canonical queue** — the
work queue, both SAC election queues, and each daemon's own control queue.
The dead-letter queue itself is the only exception, because a queue that
dead-letters to itself is a cycle. This closes a second silent-loss path that
looked nothing like the first: a control event that failed the published
schema used to be logged and accepted, so the only trace of a version skew
between the aggregator and the fleet was a line in `docker logs`. It is now
rejected, which means the message that could not be read is still in your
hands. `egress_daemon_undecodable_total` counts them.

Preserving one unreadable message is right; preserving every one is not, and
the arithmetic is unkind. Control events fan out to *every* daemon's own
queue, so a schema mismatch between publisher and fleet is not one bad
message — it is every message multiplied by the fleet size, arriving on one
queue at the full event rate. Each daemon therefore preserves a bounded
sample (20) and accepts the rest, saying so once in its log;
`egress_daemon_undecodable_total` keeps counting past the bound, so the rate
stays visible after the samples stop. Measured: 25 malformed events in, 20
on the dead-letter queue, all 25 in the metric.

One canonical queue only works if whatever drains it can tell the messages
apart, and RabbitMQ 4 supplies exactly that: a dead-lettered message arrives
annotated with `x-first-death-queue` and `x-first-death-reason`. The redrive
below replays only what was dead-lettered from the *work* queue and leaves
everything else, so a poison control message is never replayed as work.

**Replayable is not the same as replayed**, though, and a dead-letter queue
nobody drains is a slower way of losing things. So `REDRIVE_ON_CLOSE` turns
on self-healing: on the transition back to `CLOSED`, one daemon replays the
dead-lettered messages onto the work queue in bounded passes until it is
empty. Which daemon is the broker's decision, on a second
`x-single-active-consumer` queue — the same mechanism as the prober election
and separate from it, because five daemons each replaying the same backlog
would turn a recovery into a fivefold burst at a third party that has just
come back. A pass stops on whichever comes first: the per-pass cap, the queue
running dry, the circuit leaving `CLOSED`, or a hard deadline.

It is off by default in code and on in `docker-compose.yml`, and that split is
deliberate: whether a two-minute-old payment attempt is still worth making is
a question about the workload, not about the transport. Observed on the
running stack — a 2,070-message dead-letter backlog, drained in one pass three
seconds after recovery:

```
daemon-2: redriving payments-provider.work.dead (max 5000 per pass)
daemon-2: redrive finished — 2070 replayed (drained)
```

```bash
# watch the fleet react — target=<k>/5 is the agreed active count
docker compose logs -f rmq-daemon-0 rmq-daemon-3

# take the upstream down; the queue depth is the story
curl -X POST localhost:8080/__fail -d '{"rate":1.0}'
open http://localhost:15672        # guest / guest

# kill whichever daemon the broker elected as prober, mid-incident
docker kill workspace-rmq-daemon-1-1

# every daemon serves the same /metrics route the aggregator does
docker compose exec prometheus wget -qO- http://rmq-daemon-0:9464/metrics
```

`infra/envoy/envoy.yaml` carries the config discussed:

- **Several endpoints per cluster** — six for payments-provider, four for
  shipping-rates, three for tax-calc, matching the simulated fleet exactly so
  the two `FleetSource` layers can reach the same states rather than merely
  producing the same record shape. With one host per cluster a replica can
  only ever report `0/1` or `1/1`, so `healthy < total` is unreachable, no
  replica can vote `DEGRADED` from partial ejection, and
  `failure_percentage_*` never evaluates at all
  (`failure_percentage_minimum_hosts` is 3). `infra/flaky-upstream.mjs` serves
  one port per endpoint.
- **Active health checking** against `/__health`, which is what makes
  `successful_active_health_check_uneject_host` do anything. The health
  endpoint samples the same failing service normal traffic does rather than
  being a separate truth — a deterministic one would mark every host down at
  once and collapse `DEGRADED` into `OPEN`.
- **One cluster per API.** Every stat, outlier event and access-log record is
  keyed by cluster name, so cluster identity *is* API identity. Traffic through
  a `dynamic_forward_proxy` catch-all cannot produce per-API events — and worse,
  under DFP outlier detection works per resolved host while the
  `circuit_breakers` thresholds apply to the whole DFP cluster, so one noisy
  destination eats the request budget for all of them. Generate a cluster per
  known API from the control plane; reserve DFP for the long tail.
- **`circuit_breakers`** for bulkheading, with `track_remaining: true` so
  saturation is a gauge rather than a bare counter.
- **`outlier_detection`** with `failure_percentage_*` alongside
  `consecutive_5xx`, because ratio-based thresholds scale with replica count
  while consecutive counts get divided by fleet size.
- **`split_external_local_origin_errors`** to separate an upstream 503 from a
  local connect failure — for egress you almost always want to distinguish these.
- **`cluster_manager.outlier_detection.event_log_path`** for the discrete
  ejection stream.

### Two real aggregator instances, one shared Redis

`docker compose up` runs `aggregator` and `aggregator-2` — two separate
containers, both `--ha=redis` against the one `redis` service — not one.
This is the [High availability](#high-availability) design actually
deployed, not just tested in-process: exactly one of them holds
`egress_aggregator_is_leader=1` at a time, and it was verified by force —
inject a real failure, let the current leader publish into `OPEN`, then
`docker kill` its container outright:

```mermaid
sequenceDiagram
  participant A2 as aggregator-2 (LEADER)
  participant R as redis
  participant A1 as aggregator (standby)

  A2->>R: renew lease (token=4)
  A2->>A2: publish state_changed · seq=7 · OPEN
  Note over A2: docker kill — no graceful shutdown
  A1->>R: tryAcquireOrRenew → blocked (lease still live)
  Note over A1,R: ~8s pass — lease_ttl_ms=5000 expires
  A1->>R: tryAcquireOrRenew → token=5 (genuine handoff)
  R-->>A1: checkpoint: state=OPEN, seq=7
  A1->>A1: rehydrate BreakerState from checkpoint
  A1->>R: publish + checkpoint · seq 8 → 13
  Note over A1: seq=13 · CLOSED · PROBE_SUCCEEDED
```

The standby took over within `lease_ttl_ms`, rehydrated `payments-provider`
from its last Redis checkpoint (`OPEN`, not `CLOSED` — the state survived,
not just the fact that *something* is now leading), and kept publishing
from `sequence=7` onward, through the rest of the probe cycle to
`PROBE_SUCCEEDED` / `CLOSED` at `sequence=13` — no reset, no gap, no
duplicate, across a hard kill of a different OS process mid-incident. That
is the property `Coordination.test.ts` and `RedisCoordination.test.ts` prove
in a single test process; this is the same property, watched happen between
two real containers.

Prometheus scrapes both instances (`infra/monitoring/prometheus.yml`) with
Prometheus's own `instance` label distinguishing them, so
`egress_aggregator_is_leader` in Grafana shows exactly one of the two lines
at 1 and the other at 0, flipping on a real failover.

### Fleet state stays per replica, on purpose

Replicas are stateless behind an L4 load balancer, configured by xDS. That is
the whole HA story and it is the right one — but outlier detection state is per
instance. With 10 replicas, `consecutive_5xx: 5` means up to 50 failures
fleet-wide before every replica agrees. Two ways out: divide thresholds by
replica count and accept the variance, or use `failure_percentage_*`, which is
ratio-based and scales cleanly. This config does the latter.

The tempting third option is shared breaker state in Redis. Don't: that puts a
network round trip and a shared failure domain in the hot path of a
high-throughput proxy, which is what the breaker exists to avoid. Sharing
belongs in the aggregator, off the request path.

## What is a prototype, not production

- **Ingestion is polling.** `packages/aggregator/src/FleetSource.ts` polls each replica's admin
  `/stats`. It needs no proto codegen, which is why it is here. In production
  swap it for the push-based `envoy.service.metrics.v3.MetricsService` sink,
  which also tags each report with the replica's node ID — it produces the same
  `ReplicaReport`, so nothing downstream changes.
- **Aggregator state is not a database — but leader election, failover, and
  the Redis backend are all real, deployed, and watched working, not just
  tested in isolation.** See
  [Two real aggregator instances, one shared Redis](#two-real-aggregator-instances-one-shared-redis):
  `docker compose up` runs two aggregator containers against a shared Redis,
  and a hard `docker kill` of the leader mid-incident was used to confirm
  the standby takes over and continues the sequence rather than resetting
  it. What's still fair to call a prototype is durability of the checkpoint
  store itself — one `redis:7-alpine` container with no persistence
  configured is a second single point of failure, just moved one level
  down. A real deployment wants Redis with AOF/replication, or a stronger
  backing store entirely (etcd, a Postgres advisory lock) behind the same
  `LeaderElection`/`CheckpointStore` interfaces — those interfaces, not the
  demo's Redis config, are the part meant to carry over.
- **Enforcement is observational here.** The aggregator publishes but does not
  push config — see [the fork this defers](#the-fork-this-defers).
- **The Envoy and monitoring stack *is* run end to end now — the earlier
  "Docker cannot bind-mount here" note was a false assumption, corrected by
  actually running it.** `docker compose up` boots three real Envoy
  replicas, a real aggregator pair, and drives a real incident through them
  (see the section linked above); what looked like a sandbox limitation was
  one environment variable pointing at the wrong path (see
  [Running against real Envoy](#running-against-real-envoy) for the exact
  failure mode and fix). `parseStats` is still additionally covered by a
  pure unit test against realistic admin output, including the noise stats
  that must not be mistaken for clusters, and the `/metrics` endpoint and
  every metric in [Metrics & monitoring](#metrics--monitoring) are verified
  in-process too — belt and suspenders, not a substitute for the real run.
- **HTTPS egress needs TLS interception** for any of the L7 signals to exist. If
  you proxy via `CONNECT` you get L4 only, `consecutive_5xx` is dead, and the
  breaker degrades to connection-level detection. Decide this early: it drives
  the whole certificate story.

## Layout

A pnpm workspace, one package per component. The split is deliberate: **the
decision logic is pure, the shell is Effect, and each has its own boundary
you can `pnpm --filter` independently.**

```
packages/
  domain/                    @egress/domain — pure, no Effect, no clock, no I/O
    src/Model.ts             vocabulary, Schema for the published event, error types
    src/Breaker.ts           the state machine — pure functions, total on (state, reports, now)
    test/Breaker.test.ts     12 tests, pure — no runtime, no clock, no mocks

  aggregator/                @egress/aggregator — depends on @egress/domain
    src/Aggregator.ts        service: tick loop over the pure machine, on a Schedule
    src/Coordination.ts      leader election + fencing-token checkpoints — see High availability
    src/Events.ts            EventBus (PubSub) + EventSink (webhook, declarative retry)
    src/FleetSource.ts       service with two layers: simulated fleet, real Envoy
    src/Http.ts              routes, SSE as a merged Stream, delivery-integrity tracking, /metrics
    src/Telemetry.ts         every Metric the app emits, in one place
    src/main.ts              layer composition, NodeRuntime.runMain
    public/index.html        operator console (unchanged — plain HTML/CSS/JS)
    test/Aggregator.test.ts   14 tests — full pipeline under TestClock, stats parsing,
                              and the delivery-integrity tracker as a pure function
    test/Coordination.test.ts  6 tests — fencing primitives, a real two-instance failover, and a re-promotion
    test/integration/          Redis-backed HA, opt-in (`pnpm run test:redis`) — needs Docker

  subscriber/                @egress/subscriber — depends on @egress/domain
    src/subscriber.ts        standalone consumer; decodes with the producer's Schema

  rmq/                       @egress/rmq — Effect wrapper over AMQP 1.0 (RabbitMQ 4 native)
    src/Client.ts            the Rmq service; two silent client bugs guarded here
    src/ControlPlane.ts      circuit.control naming, shared by publisher and consumers
    test/integration/        8 tests against a real broker, opt-in (`pnpm run test:rmq`)

  rmq-consumer/              @egress/rmq-consumer — the competing-consumer daemon fleet
    src/DaemonPolicy.ts      pure: (prior, circuit state, fleet size) -> target active count
    src/Contract.ts          pure: the per-API sequence guarantee, checked on the AMQP side
    src/daemon.ts            one daemon, one process; two connections, two SAC elections
    src/Redrive.ts           dead-letter recovery: bounded passes, own connection per pass
    src/producer.ts          floods the work queue; never backs off, on purpose
    src/Telemetry.ts         every metric the fleet emits, in one place
    src/main.ts              role dispatch — `daemon` or `producer` — plus /metrics
    test/DaemonPolicy.test.ts  9 tests, pure — no runtime, no broker
    test/Contract.test.ts      7 tests, pure — including the backwards-sequence case

  demo/                      @egress/demo — no dependency on the others, speaks only HTTP
    src/driver.ts            drives the demo script over HTTP, narrates transitions

infra/
  envoy/envoy.yaml           egress config: per-API clusters, outlier detection
  traffic-generator.mjs      keeps requests flowing through Envoy so /__fail means something
  monitoring/                Prometheus scrape config + provisioned Grafana dashboard

docs/rmq-control-plane.md    the RabbitMQ scenario: design, live run, and what it exposed
docker-compose.yml           wires infra/ and the packages/ entrypoints together
.github/workflows/ci.yml     `pnpm run check` on every push, and both Docker-backed
                             suites (`test:redis`, `test:rmq`) in a second job — they
                             are opt-in locally, which makes them the ones most likely
                             to rot unnoticed
```

Cross-package imports go through `@egress/domain`'s `package.json#exports`
(`@egress/domain/Model.ts`, `@egress/domain/Breaker.ts`) rather than relative
`../../` paths, and `workspace:*` in each consumer's `package.json` is what
`pnpm install` resolves to a symlink — so a change to the state machine is a
change in one package, felt through a real dependency edge, not a shared
folder. There is still no build step: every package runs straight off its
`src/*.ts` via `--experimental-strip-types`, and `tsc --noEmit` at the root
typechecks every package in one pass (`packages/*/src` and
`packages/*/test` in `tsconfig.json`'s `include`). The root scripts
(`pnpm start`, `pnpm run demo`, `pnpm run subscribe`) call `node` on each
package's entrypoint directly rather than going through `pnpm --filter`:
this pnpm version does not strip a trailing `--` the way npm does, so
`pnpm --filter @egress/demo start -- shipping-rates` would hand the driver
the literal string `"--"` as its API id instead of `"shipping-rates"` —
direct invocation is what makes `pnpm run demo shipping-rates` (no `--`)
actually work.

`Breaker.step` is a total function of `(state, now, config)`. Everything hard to
reason about — concurrency, scheduling, delivery, retries — lives in the Effect
layer above it. That is why the logic deciding what subscribers get told can be
tested exhaustively with plain `assert`.

## What the build surfaced

Twelve things worth knowing — eleven found by running the thing, one by
reading it afterwards:

- **A reason code cannot be derived from averaged endpoint counts.** With four of
  five replicas seeing zero healthy hosts, the mean rounds to 1, so "all
  endpoints gone" silently became false. The check is now
  `votes.DOWN === live.length` — unanimous, not merely quorate.
- **Ejection backoff outlives the outage.** `base_ejection_time × ejection_count`
  walks up to the `max_ejection_time` cap, so a recovered upstream stays ejected
  long after it heals. The fix is not shorter timers, it is
  `successful_active_health_check_uneject_host` — and that flag does nothing
  on its own. It needs an actual `health_checks` block, which the config was
  missing for a long time while the README claimed the problem was solved.
  With one configured: six hosts carrying ~25s of accumulated ejection
  backoff (34 enforced ejections between them) came back **3.1s** after the
  upstream recovered.
- **Snapshots are not duplicates.** The first delivery-integrity check counted
  them as such, because they deliberately repeat the current sequence. Only
  `state_changed` is sequence-checked; snapshots exist precisely to be re-applied
  harmlessly.
- **`event_log_path` lives under `cluster_manager.outlier_detection`,** not on
  the cluster. Easy to get wrong from memory.
- **Fencing per-key is not fencing.** The first version of `CheckpointStore`
  rejected a stale write only if a *newer* write had already landed for that
  same API. That is not a fencing guarantee — it just means a stale leader
  wins by default on any API the new leader has not gotten to yet, which is
  the exact split-brain window fencing tokens exist to close. The fix is
  checking against the one shared lease-token counter, not a per-key value —
  see [High availability](#high-availability).
- **A healthy-looking process can be a deaf one.** A daemon in the RabbitMQ
  fleet stopped reacting to circuit events entirely — container up, CPU at
  0.01%, sockets and file descriptors identical to a healthy peer, and 64
  undelivered messages behind a consumer the broker still considered
  registered. Closing a consumer while the broker has deliveries in flight
  strands them, and enough strandings stall *every* link on that connection.
  Two fixes came out of it: the daemon now keeps its control plane on a
  connection that never closes a link, and it logs a heartbeat independent
  of the event stream — because until then every log line it produced was
  emitted while handling an event, so "gone deaf" and "nothing happened"
  looked identical. See
  [docs/rmq-control-plane.md](docs/rmq-control-plane.md).
- **Backpressure is about *when you ack*, not how much you buffer.** The
  first daemon accepted each message on arrival and fired its third-party
  call afterwards, so draining a 50k backlog meant tens of thousands of
  concurrent requests from one process — the herd the fleet policy exists to
  prevent, self-inflicted. Capping concurrency and dropping the excess was
  worse (32,000 messages shed in one drain). Deferring the AMQP accept until
  the call settles is what actually works: credit stops refilling, the
  broker stops pushing, and the backlog stays in the queue where it is
  visible.
- **A documented sandbox limitation turned out to be a misconfigured
  environment variable.** "Docker cannot bind-mount the project directory
  here" had been true every time it was checked — until `HOST_WORKSPACE_FOLDER`
  got explicitly re-exported to a container-internal path while debugging
  something unrelated, silently shadowing the correct host path the
  devcontainer had already set. The fix was not a workaround, it was
  removing the override — three real Envoy replicas, two real aggregator
  instances, and a real Redis now run end to end (see
  [Running against real Envoy](#running-against-real-envoy)). The lesson
  travels further than this one variable: re-verify an environment
  assumption before designing around it, especially one written down as
  fact by an earlier pass over the same repo.
- **A demoted leader that comes back is not a cold start, and that broke the
  one guarantee this repo is about.** Rehydrating `sequence` from the Redis
  checkpoint only ever fired for APIs an instance had never seen — which is
  right on a fresh takeover and wrong on a *re*-takeover, because the
  instance still holds its own breakers in memory from last time while
  whoever led in between has already published past them. The result is two
  different payloads under one sequence number: exactly the break
  `/api/subscriber` exists to detect, invisible to every test because both
  failover tests used a freshly built instance. Losing the lease now drops
  the registry, so re-promotion takes the rehydrate path that was already
  tested, and `Coordination.test.ts` keeps one instance alive across a full
  demotion to pin it.
- **There is no way to tell this AMQP client "this attempt failed, try
  again".** `requeue()` sends `modified{delivery_failed: false}` and
  `discard()` sends `modified{delivery_failed: true, undeliverable_here:
  true}` — nothing in between. RabbitMQ increments AMQP 1.0's
  `delivery-count` only for a delivery marked failed, so releasing the same
  message three times returns `delivery_count: 0` three times, and a
  redelivery budget that outlives the message moving to another consumer
  cannot be built. Same shape as the link-credit finding: the lever is
  genuinely absent rather than merely undocumented, so the daemons
  dead-letter on the first failure and a test pins the behaviour in case a
  client release changes it.
- **The check that proves the contract had no test, and a blind spot for the
  one bug most likely to break it.** `/api/subscriber` is what this README
  points at to claim the stream is gapless and non-repeating. It counted a
  *repeated* sequence as a duplicate and a *skipped* one as a gap — and let a
  sequence going **backwards** fall through both branches, uncounted. That is
  exactly the shape a leadership bug produces: an instance resuming from
  stale in-memory state republishes numbers a later leader already used. So
  the demotion bug found the same day would have been invisible to the very
  check that exists to catch it, and the function had no unit test at all
  despite being pure, twenty lines, and load-bearing. It has six now. The
  lesson is not "write more tests" — it is that a verification mechanism is
  itself code, and an untested one is a claim, not a proof.

## What Effect actually bought here

Not ceremony — four concrete things:

- **`TestClock` replaced a shell script.** Verifying open → probe → backoff →
  reopen → close used to mean `sleep 6` between `curl`s, which was slow and
  flaky. The tick loop is `Effect.repeat(Schedule.spaced(...))` rather than
  `setInterval`, so tests drive 100 seconds of simulated time in milliseconds,
  deterministically.
- **Retry policy became a value.** `WebhookSink` delivery is
  `Effect.retry({ schedule: Schedule.exponential("100 millis"), times: 3 })`
  instead of a hand-rolled loop with a counter, a sleep and a try/catch.
- **`PubSub` replaced manual subscriber bookkeeping.** SSE clients are Streams
  that end with the request scope; there is no `Set` of response objects to
  clean up and no `clearInterval` to remember.
- **Two bugs surfaced.** Type-checking caught the Envoy stats parser indexing
  possibly-undefined regex groups under `noUncheckedIndexedAccess`. `TestClock`
  caught the aggregator publishing `observedSince: "1970-01-01T00:00:00.000Z"`
  for any API that had not yet changed state — a real wire-format bug that
  wall-clock time had been hiding. `Breaker.initial` now seeds from the real
  clock, and the test anchors `TestClock` to a realistic instant so epoch leakage
  stays detectable rather than becoming a legitimate value.

The state machine itself is *not* written in Effect, and that is the point.
Effect earns its place at the boundaries.

### Effect 4 RC, read from the `.d.ts` files

The published migration write-ups describe beta.5 and the API has moved since,
so these were taken from the installed types rather than from summaries:

- `ServiceMap` is `Context` again — the blog would have sent you into a wall
- `Effect.fork` is gone: `forkChild` / `forkScoped` / `forkIn` / `forkDetach`
- `Effect.zipRight` → `Effect.andThen`
- `Stream.repeatEffect` → `Stream.fromEffectSchedule`
- `HttpServerResponse.json` returns an `Effect` (encoding can fail);
  `jsonUnsafe` is the plain constructor
- `Schema.decodeUnknownEffect` does not exist; only `decodeUnknownOption` takes
  `unknown`
- `Effect.catchAll` is `Effect.catch` — and `catchCause` is *not* the
  drop-in it looks like, because it swallows defects as well as failures
- `Schema` is core now, not `@effect/schema`
- There is no `Runtime.runFork(runtime)` to bridge a callback back into the
  fiber tree the way v3 allowed; a `Queue` the callback writes to and a fiber
  that drains it is the shape that replaces it

`tsconfig.json` sets `erasableSyntaxOnly`, so the compiler enforces
strip-types compatibility rather than leaving it to discipline. It also sets
`noUnusedLocals`, `noUnusedParameters`, `noImplicitOverride` and
`noFallthroughCasesInSwitch` — with no build step and no linter in this repo,
`tsc` is the only automated thing that reads the source, so it may as well be
asked the questions a linter would. Turning them on found three dead imports
immediately, one of them left behind by the refactor in the same commit.

## Tuning

`defaultConfig` in `packages/domain/src/Model.ts`, exposed as a `Context.Reference` so
it has a default but any test can override it for one call with
`Effect.provideService(Config, ...)`. Values are set for a live demo, not
production — `openMs` in particular is 4s so recovery is watchable.

| | | |
|---|---|---|
| `quorum` | 0.6 | fraction of replicas that must agree |
| `dwellMs` | 2000 | how long a candidate state must persist before publishing |
| `minStateMs` | 3000 | floor between published transitions; debounces flapping |
| `openMs` / `maxOpenMs` | 4s / 16s | probe backoff, doubles on each failed probe |
| `probeSuccesses` | 3 | consecutive healthy observations needed to close |
| `snapshotMs` | 15000 | periodic full-state republish per API |

`dwellMs` and `minStateMs` are not cosmetic. Without them a marginal upstream
generates an event storm, and every subscriber ends up debouncing it themselves
— badly, and differently from each other.

## High availability

One aggregator publishing means one process is a single point of failure.
Two aggregators publishing independently is worse: nothing stops them from
handing out conflicting sequence numbers for the same API, which is exactly
the contract this whole design exists to protect. `Coordination.ts` closes
that gap with two primitives, both required together:

- **`LeaderElection`** — exactly one instance may publish at a time. Each
  tick calls `tryAcquireOrRenew(instanceId, ttl)`; a non-leader does not
  poll, does not step the state machine, and does not publish — it only
  keeps trying to acquire. Every genuine handoff (not a renewal) produces a
  **fencing token** that strictly increases.
- **`CheckpointStore`** — whichever instance takes over next must resume
  `sequence` and `openBackoffMs` from where the last one left off, not from
  `Breaker.initial`. A newly-leading instance rehydrates each API it hasn't
  seen yet from its last checkpoint before its first tick runs.

The subtlety worth calling out, because it is easy to get wrong: fencing has
to be checked against the *same shared counter* `LeaderElection` issues
from, not a per-API "last write wins" value. An earlier version of this
fenced each API's checkpoint independently — which is wrong, because it only
stops a stale writer *after* someone else has already written that specific
key. A stale leader mid-GC-pause can still win on any API the new leader
hasn't published for yet, which is precisely the split-brain case fencing
tokens exist to prevent. Checking against the shared lease token instead
closes the window for every API at once, the instant a handoff happens —
`Coordination.test.ts`'s "a stale token is rejected even for an API no one
has checkpointed yet" test is that exact bug, pinned down.

`main.ts` supports both backends: `InMemoryCoordinationLayer` by default
(one instance that always wins its own lease — not a special case, just
what solo mode produces), or `--ha=redis --redis=<url>` for
`RedisCoordinationLayer`, written against a deliberately minimal `RedisLike`
port (one `eval` method) so that any real client — `ioredis`, `node-redis`
— plugs in with a one-line adapter, rather than pinning a dependency the
default path does not need.

Both are run for real, not just reasoned about, at increasing levels of
integration:

1. `Coordination.test.ts` runs two independent "instances" against one
   shared in-memory coordinator in a single process — a real failover
   without a second machine, plus the harder case: an instance that led,
   *lost* the lease, and is promoted again with its own breakers still warm.
   That one must resume from the checkpoint rather than from what it
   remembers, and it is kept alive across the whole demotion precisely so a
   fresh build cannot hide the bug.
2. `test/integration/RedisCoordination.test.ts` (`pnpm run test:redis`,
   opt-in — needs Docker) spins up a real `redis:7-alpine` container via
   Testcontainers and proves the same properties, including the exact
   fencing bug above, by Redis's own Lua execution rather than by re-reading
   the script.
3. `docker compose up` runs it as an actual deployment — two real
   `aggregator` containers against one real `redis` container, with a hard
   `docker kill` of the leader used to confirm the failover live. See
   [Two real aggregator instances, one shared Redis](#two-real-aggregator-instances-one-shared-redis).

## The fork this defers

One question decides how much more there is to build: **must the aggregator's
`OPEN` be authoritative, or are the events purely observational?**

If Envoy's local enforcement is sufficient and subscribers only need to be told,
this prototype is roughly the whole system: a read-only aggregator, no xDS path,
considerably less to build and to operate.

If the open state must be enforced fleet-wide, three things follow:

1. **The xDS push path.** Push a route with `direct_response` 503 rather than
   dropping endpoints — cleaner, and it returns a stable body callers can key on.
2. **Leader-elected aggregator state** — done regardless of which side of this
   fork gets taken, since two aggregators publishing sequences for the same
   API is a contract violation, not a race, even in the purely observational
   case. See [High availability](#high-availability).
3. **A single owner for `HALF_OPEN` probing.** While an API is nominally closed,
   let Envoy handle local un-ejection (`base_ejection_time` backoff,
   `max_ejection_time`, `successful_active_health_check_uneject_host`). Once the
   control plane has declared it open, control-plane config is what is live, so
   probing becomes a weighted route sending a few percent through and watching.
   Do not let both probe at once.
