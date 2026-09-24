# Architecture

Why the data plane is Envoy, what it emits, how those signals reach the
aggregator, and what the aggregator publishes. The reasoning that produced
this shape is in [journey.md](../history/journey.md); this is the shape itself.

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
  threshold, and there it matters less: the route retries only failures that
  never reached the third party (`connect-failure`, `refused-stream`). A 5xx is
  the daemon's to retry against its per-message budget, and the route's 1.8s
  timeout is under the daemon's 2s, so Envoy gives up before its caller does.
  Retrying 5xx here too, with a 5s timeout, made every daemon attempt up to
  three calls and kept retrying requests the daemon had already abandoned.
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
on [running against real Envoy](operations.md#running-against-real-envoy) explains what
each one is doing in the demo. The alternatives to Envoy itself are covered
in [Approaches, and where each one runs out](../README.md#approaches-and-where-each-one-runs-out).

### What the config turns on

Each of those layers is turned on deliberately in
[`infra/envoy/envoy.yaml`](../infra/envoy/envoy.yaml):

- **Several endpoints per cluster** — six for payments-provider, four for
  shipping-rates, three for tax-calc, matching the simulated fleet exactly so
  the two `FleetSource` layers can reach the same states rather than merely
  producing the same record shape. With one host per cluster a replica can
  only ever report `0/1` or `1/1`, so `healthy < total` is unreachable, no
  replica can vote `DEGRADED` from partial ejection, and
  `failure_percentage_*` never evaluates at all
  (`failure_percentage_minimum_hosts` is 3). That is also what happens to a
  real API behind a load balancer, and what the breaker does instead — flap,
  measured at 88 transitions where six endpoints produce one — is in
  [what-if.md](what-if.md#what-if-the-flaky-api-is-behind-a-load-balancer). `infra/flaky-upstream.mjs` serves
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

## Ingestion: push or poll, decided by measurement

Two layers produce the same `ReplicaReport` and nothing downstream can tell
them apart, which is the claim `FleetSource` was written to make good on:

- **`--source=envoy`** polls each replica's admin `/stats?format=json` every
  tick.
- **`--source=envoy-push`** runs the gRPC server Envoy's
  `envoy.stat_sinks.metrics_service` pushes to, on `--push-port` (9900).

`docker compose up` uses push. That is a measured decision, not a preference —
on this stack (three replicas, three APIs, 250ms tick, 250ms
`stats_flush_interval`), injecting a total upstream failure and timing from
*Envoy's own stat moving* to *the aggregator reporting it*:

| | ingestion lag (3 runs) | Envoy CPU | aggregator CPU (leader + standby) |
| --- | --- | --- | --- |
| poll | 165 / 194 / 196 ms | 4.29% | 3.11% + 0.21% |
| push | 30 / 139 / 113 ms | 3.88% | 1.37% + 0.90% |

Push roughly halves the lag, which is what you would expect: polling waits up
to a full tick and then pays an HTTP round trip and a JSON parse, while a push
is already in memory when the tick reads it. The Envoy-side difference is
inside the noise at this size — serialising every stat on each flush is not
free, and a fleet with far more stats than this one should measure again
before assuming the same answer.

Three things this cost to get right, none of them in the docs:

**A gRPC server did not need a build step.** That belief is why the polling
layer was written first. `@grpc/proto-loader` reads `.proto` at runtime, and
protobuf addresses fields by number — so
[`proto/`](../packages/aggregator/proto) holds deliberately *partial* schemas
carrying only the fields this repo reads, and Envoy's real messages decode
against them without vendoring Envoy's api tree or its dependencies.

**One sink per aggregator, not one cluster with two endpoints.** A stats sink
names one gRPC cluster. Put both instances in that cluster and Envoy
load-balances the stream, so each aggregator sees *some* replicas and computes
a quorum from a partial fleet — data that is wrong rather than absent.
`infra/envoy/envoy.yaml` declares two sinks, and both instances see all three
replicas (`reportingReplicas: 3` on each).

**The node identifier arrives once.** Envoy opens one stream per sink and
sends `identifier` only in the first message, so it is remembered per call.
Reading it off each message works perfectly in a test with one message and
loses every replica's identity in production. It comes from `--service-node`,
which each replica now sets — a polling aggregator names replicas itself from
the URL it dialled, but a replica that pushes has to say who it is.

Verified end to end on the push path: `CLOSED → OPEN
(ALL_ENDPOINTS_EJECTED) → HALF_OPEN → OPEN (PROBE_FAILED) → HALF_OPEN → CLOSED
(PROBE_SUCCEEDED)`, all three replicas reporting throughout.

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
    "reportingReplicas": 9,
    "lease": { "epoch": "5b0c…", "counter": 12 }
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
- **The vocabulary is part of the contract, not decoration.** `state`,
  `previousState` and `reason` decode against the declared literals, and
  `sequence` against `Natural` — so an event carrying a reason nobody has
  heard of, or a sequence that cannot be ordered, fails to decode rather than
  arriving half-understood. See
  [decisions/007](decisions/007-message-contracts.md).
- **`DEGRADED` is a published state, not an inference.** Binary open/closed
  throws away real information once an API has many endpoints: "3 of 10 hosts
  ejected" and "the API is gone" call for different reactions. The extra state
  costs almost nothing in the machine and saves every subscriber from deriving
  it from `healthyEndpoints` — badly, and in three different ways.

`WebhookSink` never blocks the control loop: bounded retry with backoff, a
dead-letter list, and load shedding above a concurrency ceiling.

## Fleet state stays per replica, on purpose

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

## The fork this defers

> **Answered, 2026-09-05: observational.** See
> [docs/decisions/002-enforcement-authority.md](decisions/002-enforcement-authority.md)
> — the aggregator publishes and never pushes config, because enforcement is
> already local and immediate in Envoy, and making the aggregator
> authoritative would put it in-band for every request. The reasoning below
> is what the decision rests on; it is no longer an open question.

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
   case. See [High availability](high-availability.md).
3. **A single owner for `HALF_OPEN` probing.** While an API is nominally closed,
   let Envoy handle local un-ejection (`base_ejection_time` backoff,
   `max_ejection_time`, `successful_active_health_check_uneject_host`). Once the
   control plane has declared it open, control-plane config is what is live, so
   probing becomes a weighted route sending a few percent through and watching.
   Do not let both probe at once.
