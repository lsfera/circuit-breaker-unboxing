# Architecture

## Envoy enforces

Envoy is the data plane because its layers are independent, and "circuit
breaking" on egress is several problems given one name. `infra/envoy/envoy.yaml`
turns each on deliberately:

- **One cluster per API**, several endpoints each (six for payments-provider).
  Every stat and outlier event is keyed by cluster, so cluster identity is API
  identity. With one endpoint per cluster a replica can only report `0/1` or
  `1/1`: partial ejection, and so `DEGRADED`, is unreachable, and the breaker
  flaps instead (measured: 88 transitions where six endpoints produce one).
- **`outlier_detection`** — the actual breaker, per replica: `consecutive_5xx`,
  `consecutive_gateway_failure`, and `failure_percentage_*`, which is
  ratio-based and so does not need dividing by the replica count.
- **Active health checks** against `/__health`, which is what lets a recovered
  host be un-ejected immediately (3.1 s for six hosts carrying 25 s of backoff)
  rather than serving out its ejection time.
- **`circuit_breakers`** for bulkheading, with `track_remaining: true` so
  saturation is a gauge.
- **`adaptive_concurrency`**, shedding with a `429` (not the default `503`) so a
  daemon can tell a local shed from an upstream failure.
- **Retries only for failures that never reached the third party**
  (`connect-failure`, `refused-stream`), and a 1.8 s route timeout under the
  daemon's own 2 s. A 5xx is the daemon's to retry, against its per-message
  budget.

Outlier state stays per replica on purpose: sharing it (in Redis, say) would put
a network round trip and a shared failure domain in the hot path. Agreement
belongs in the aggregator, off the request path.

## What Envoy emits, and how it gets here

| Signal | Discrete event? |
|---|---|
| Host ejected / un-ejected | yes |
| Per-request breaker rejection (`UO`) | yes |
| Threshold saturation (`*_overflow`) | **no** — a counter delta |

So the aggregator reads cluster stats, not events:

```
membership_healthy / membership_total            -> healthy / total
outlier_detection.ejections_active               -> ejectionsActive
upstream_rq_pending + cx + retry _overflow       -> overflowTotal (edge-detected)
```

Two sources produce the same `ReplicaReport`: `--source=envoy` polls each admin
`/stats`, `--source=envoy-push` receives Envoy's `metrics_service` gRPC stream.
Compose uses push, because it measured faster:

| | ingestion lag (3 runs) | Envoy CPU | aggregator CPU |
|---|---|---|---|
| poll | 165 / 194 / 196 ms | 4.29% | 3.11% |
| push | 30 / 139 / 113 ms | 3.88% | 1.37% |

Two gotchas: each aggregator needs its own stats sink (one cluster with two
endpoints load-balances the stream, and each sees a partial fleet), and Envoy
sends its node identifier only in a stream's first message.

## The aggregator decides

Per tick (250 ms) the leader folds every replica's report into one pure
`Breaker` per API. A replica votes `DOWN`, `DEGRADED` or `OK` from its own view;
a candidate state needs 60% of reporting replicas and must hold for 2 s before
it is published. Replicas silent for 5 s stop counting.

```
CLOSED ──partial ejection──▶ DEGRADED ──all ejected──▶ OPEN
   ▲                                                   │ 4 s, doubling to 16 s
   └──3 healthy observations── HALF_OPEN ◀──────────────┘
                                   │ probe failed ──▶ OPEN
```

Its `OPEN` is **observational**: it publishes and never pushes config to Envoy
([ADR 002](decisions/002-enforcement-authority.md)). Enforcement is already
local and immediate; an authoritative aggregator would be in-band for every
request.

## The event contract

```json
{
  "specversion": "1.0",
  "type": "egress.circuit.state_changed",
  "subject": "api://payments-provider",
  "data": {
    "apiId": "payments-provider",
    "sequence": 47,
    "previousState": "CLOSED",
    "state": "OPEN",
    "reason": "ALL_ENDPOINTS_EJECTED",
    "healthyEndpoints": 0,
    "totalEndpoints": 6,
    "reportingReplicas": 3,
    "lease": { "epoch": "5b0c…", "counter": 12 }
  }
}
```

- **Ordered per API**, and `sequence` is gapless across `state_changed` events.
- **Full state**, so a subscriber can sync from any one event.
- **Snapshots** (`egress.circuit.snapshot`) repeat the current state and
  sequence every 15 s, for late subscribers.
- **Strict vocabulary**: states, reasons and a `Natural` sequence are decoded
  against the schema, so an unknown value fails to decode rather than arriving
  half-understood ([ADR 007](decisions/007-message-contracts.md)).
- **`lease`** is the publishing leader's fencing token. A reader ranks events by
  it before the sequence (`supersedes`), so a paused leader that resumes is
  ignored — see [high-availability.md](high-availability.md).

Two transports: `circuit.control` on RabbitMQ for the fleet, and a webhook with
a durable outbox for other subscribers.

## The daemon fleet

Five competing consumers drain `payments-provider.work` and call the third party
through one Envoy address. Each learns the circuit from its own queue on
`circuit.control` and decides for itself:

| Circuit | The fleet |
|---|---|
| `CLOSED` | everyone consumes; after an outage, a ramp of the floor daemon alone → ¼ → ½ → all, 5 s per rung |
| `DEGRADED` | half the fleet, chosen by each daemon's position in a hash space, plus one elected "floor" daemon so a small fleet never lands on nobody |
| `OPEN` | nobody consumes; the work waits in the queue |
| `HALF_OPEN` | the daemon elected on `probe-trigger` takes exactly one message |

The target is a fraction, not a count, so no daemon needs to know the fleet's
size ([ADR 013](decisions/013-the-target-as-a-fraction.md)). Elections are
RabbitMQ's `x-single-active-consumer` on always-idle queues (`probe-trigger`,
`redrive-trigger`, `floor`): every daemon publishes the trigger, the elected one
dedupes by sequence, settles it only once the action has run, and a daemon that
dies mid-action leaves it for the next one promoted. The daemon never judges a
probe; Envoy's outlier detection and the aggregator's quorum do.

**Control events** are applied one at a time: the transition, then the
reconcile of the daemon's channels. Only after that are the triggers the event
owes published, so an `OPEN` never waits on a publish, and a trigger whose event
a newer one has since replaced is dropped. Each step is retried in place for at
most 5 s. The delivery is acked once the event is applied, and also when a step
still fails. Nothing is undone and nothing dead-lettered, because the event is
recovered elsewhere: the next snapshot re-applies the state, every daemon
publishes the same triggers, and the floor's sweep replays `work.dead` while
closed.

**Per call**, the key is the message's `message_id`, assigned once by the
producer and carried by every redelivery, retry and redrive. The outcome decides
the delivery:

| Response | Settlement |
|---|---|
| 2xx | ack |
| 429 (Envoy shed or third party full) | released uncounted after 100–400 ms; the daemon's concurrency limit ×0.7 |
| other 4xx | parked on `work.parked`, stamped `refused-<status>` |
| (no call) wrong format, not a work message, or no `message_id` | parked unread, stamped `unreadable-<reason>` |
| 408, 5xx, no response | republished with `x-egress-attempts` + 1; the third goes to `work.dead` |

The attempt count travels in a header because a broker requeue cannot add one
([ADR 016](decisions/016-the-retry-budget-travels-with-the-message.md)); the
queue's `x-delivery-limit: 3` is only the backstop for a delivery that never
reaches a republish. The concurrency limit grows back by one slot per round
trip of successes, never above `MAX_IN_FLIGHT`, which is also the consumer's
prefetch ([ADR 011](decisions/011-the-ceiling-belongs-to-the-broker.md)).

**Redrive.** On the transition to `CLOSED` the daemon elected on
`redrive-trigger`, and every 30 s while closed the floor daemon, replays
`work.dead` onto the work queue in bounded passes (5,000 per pass, publish before ack). Each replay gets a fresh
attempt budget; after five redrives a message is parked as poison. A dead letter
that did not come from the work queue — a control event or trigger that would
not decode — is parked, never replayed.

The work, control and election queues dead-letter to `work.dead`; the `floor`
queue does not, since an event nobody took there is worthless within its 30 s
TTL. The dead-letter and parked queues are terminal and have
`x-delivery-limit: -1`, because a quorum queue's default of 20 would drop a
message at its limit.
