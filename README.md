# Per-API egress circuit breaker events

A working prototype of the design we discussed: Envoy enforces circuit breaking
per replica, and a control-plane aggregator turns the fleet's disagreement into
**one coherent, gapless event stream per API** for downstream subscribers.

Built on **Effect 4 (4.0.0-rc.112)**. Requires Node 22.6+ and TypeScript 5.9+.
TypeScript runs natively via Node's type stripping, so there is still no build
step — but `tsc --noEmit` is now load-bearing, because Effect's guarantees are
type-level.

```bash
pnpm install
pnpm start           # simulated 5-replica fleet
pnpm run check       # typecheck + 25 tests
pnpm run test:redis  # optional — needs Docker: HA coordination against a real Redis
```

Then open <http://localhost:8088>.

> Effect 4 is a release candidate. Versions are pinned exactly (`effect` and
> `@effect/platform-node` ship in lockstep at the same version now) because RC
> APIs still move — `ServiceMap` was renamed back to `Context`, and `Effect.fork`
> was replaced by explicit `forkChild`/`forkScoped`/`forkIn`, between beta and
> rc.112.

## The problem it solves

Envoy's outlier detection is per-replica by design — each proxy independently
samples upstream responses and ejects hosts it believes are bad. That is correct
for *protection* and wrong for *notification*: with 10 replicas and a degrading
upstream you get up to 10 `circuit_opened` events at different timestamps, then
replicas recovering out of step. Subscribers see an API flapping several times
for a single incident.

So enforcement and publication are split:

| | Enforces | Publishes | Latency |
|---|---|---|---|
| Envoy, per replica | yes, immediately | no | sub-second |
| Aggregator, one machine per API | via config push | yes | seconds |

The console makes this visible. The coloured strip on each API is one block per
replica showing that replica's local view. Drive an upstream to a partial
failure rate and you will see the blocks disagree while the published state
stays steady.

### Why publication is a separate component at all

"All discrete events delivered by webhook" at egress throughput means emission
cannot sit in the request path — no proxy holds its p99 while doing an outbound
POST per event. That single requirement is what makes this three components
rather than one:

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

## Why Envoy for the data plane

It is the only option in this class with three genuinely independent layers,
which matters because "circuit breaking" on egress is really three different
problems:

- **`circuit_breakers` thresholds** — `max_connections`,
  `max_pending_requests`, `max_requests`, `max_retries`,
  `max_connection_pools`, per priority. This is *bulkheading*: it caps resource
  consumption and detects nothing. `track_remaining: true` turns overflow into a
  gauge instead of a bare counter.
- **`outlier_detection`** — the actual breaker. Ejects hosts on
  `consecutive_5xx`, `consecutive_gateway_failure`,
  `failure_percentage_threshold`, or success-rate deviation. The `enforcing_*`
  knobs let ejection ramp in gradually rather than flipping on at 100%.
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

### Alternatives considered

- **Apache APISIX** is the batteries-included answer: the `api-breaker` plugin
  for circuit breaking, `http-logger` to push JSON to an HTTP endpoint — a
  literal webhook sink, no bridge to build — etcd for cluster config, and a
  forward-proxy plugin for egress. The trade is that `api-breaker` is per-route
  and much cruder than outlier detection (no per-host ejection, no success-rate
  statistics), it is Lua/OpenResty, and `http-logger` firing per request at high
  throughput is exactly the hot-path coupling to avoid.
- **Pingora** would give precisely the breaker and event semantics wanted, and
  Cloudflare's cache runs on it, so it is proven at scale. But it is a library,
  not a proxy: no config plane, no admin API, no clustering. River is not a
  product yet. That is a quarter of engineering to arrive where Envoy starts.

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

Run `pnpm run subscribe` in a second terminal for a consumer's view of the same
stream.

### Running it hands-free

```bash
pnpm run demo                    # payments-provider, against localhost:8088
pnpm run demo -- shipping-rates  # a different API
```

`packages/demo/src/driver.ts` drives exactly the six steps above through the same
`/api/failure` route the console's slider calls, and narrates every published
transition as `/api/events` reports it — so a demo is one command in a second
terminal, and the console (or the [Grafana dashboard](#metrics--monitoring)) is
what the audience actually watches. Nothing about the incident is scripted or
mocked: the driver only sets the failure rate and waits for the real aggregator
to publish, on the real wall clock. It ends by reading `/api/subscriber` and
failing loudly if a gap or duplicate shows up — the automated form of step 6.

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
| `egress_circuit_transitions_total` | Published `state_changed` events, by API/state/reason |
| `egress_circuit_snapshots_total` | Periodic full-state republishes, by API |
| `egress_fleet_poll_duration_ms` | Time to poll and parse every replica once per tick |
| `egress_aggregator_is_leader` | 1 if this instance currently holds the publishing lease, 0 otherwise — see [High availability](#high-availability) |
| `egress_aggregator_fencing_conflicts_total` | Checkpoint writes rejected because a newer lease holder already took over, by API |
| `egress_webhook_delivered_total` / `_failed_total` / `_dead_lettered_total` | Sink outcomes, by API |
| `egress_webhook_delivery_duration_ms` | Successful-delivery latency, including retries |
| `egress_subscriber_events_received_total` / `_gaps_total` / `_duplicates_total` | The delivery contract, read from outside the process — same numbers the console's right-hand panel shows, as counters |

### Watching it live

```bash
docker compose up prometheus grafana
```

Grafana at <http://localhost:3000> (anonymous, pre-provisioned — no login) opens
straight to the **Egress circuit breaker** dashboard: a state timeline per API,
healthy/total endpoints, transitions, webhook delivery and latency, the two
delivery-contract stat tiles that should read zero through an entire incident,
and — scraped directly from each Envoy's own `/stats/prometheus` — the raw
per-replica healthy-host count, so you can see the disagreement the console's
replica strip visualizes, in a second tool, at the same time.

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

`infra/envoy/envoy.yaml` carries the config discussed:

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

```
leader (aggregator-2) before kill:  state=OPEN  sequence=7
                                     $ docker kill workspace-aggregator-2-1
standby (aggregator) after ~8s:     is_leader=1  state=OPEN  sequence=7
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
    test/Aggregator.test.ts    8 tests under TestClock — full pipeline, zero sleeps
    test/Coordination.test.ts  5 tests — fencing primitives, plus a real two-instance failover
    test/integration/          Redis-backed HA, opt-in (`pnpm run test:redis`) — needs Docker

  subscriber/                @egress/subscriber — depends on @egress/domain
    src/subscriber.ts        standalone consumer; decodes with the producer's Schema

  demo/                      @egress/demo — no dependency on the others, speaks only HTTP
    src/driver.ts            drives the demo script over HTTP, narrates transitions

infra/
  envoy/envoy.yaml           egress config: per-API clusters, outlier detection
  traffic-generator.mjs      keeps requests flowing through Envoy so /__fail means something
  monitoring/                Prometheus scrape config + provisioned Grafana dashboard

docker-compose.yml           wires infra/ and the packages/ entrypoints together
```

Cross-package imports go through `@egress/domain`'s `package.json#exports`
(`@egress/domain/Model.ts`, `@egress/domain/Breaker.ts`) rather than relative
`../../` paths, and `workspace:*` in each consumer's `package.json` is what
`pnpm install` resolves to a symlink — so a change to the state machine is a
change in one package, felt through a real dependency edge, not a shared
folder. There is still no build step: every package runs straight off its
`src/*.ts` via `--experimental-strip-types`, and `tsc --noEmit` at the root
typechecks all four projects in one pass (`packages/*/src` and
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

Six things worth knowing, all of them found by running the thing:

- **A reason code cannot be derived from averaged endpoint counts.** With four of
  five replicas seeing zero healthy hosts, the mean rounds to 1, so "all
  endpoints gone" silently became false. The check is now
  `votes.DOWN === live.length` — unanimous, not merely quorate.
- **Ejection backoff outlives the outage.** `base_ejection_time × ejection_count`
  walks up to the `max_ejection_time` cap, so a recovered upstream stays ejected
  long after it heals. The fix is not shorter timers, it is
  `successful_active_health_check_uneject_host` — in the Envoy config, and now
  modelled in the simulator too, so recovery lands ~5s after the upstream
  returns.
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
- `Schema` is core now, not `@effect/schema`

`tsconfig.json` sets `erasableSyntaxOnly`, so the compiler enforces
strip-types compatibility rather than leaving it to discipline.

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
   without a second machine.
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
