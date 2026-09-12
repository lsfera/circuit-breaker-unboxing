# Operating it

Running the full stack, what it exposes, and what to do when something fires.
For the quickstart against a simulated fleet, see the
[README](../README.md#running-it).

## Running against real Envoy

```bash
docker compose up --build
```

Every process this repo runs — both aggregators, the producer, the five
daemons, the demo driver — runs from one image built by the [Dockerfile](../Dockerfile),
with its dependencies already inside it. What boots is an artifact rather than
a directory plus an install.

Needs `$HOST_WORKSPACE_FOLDER` set to this repo's path *on the Docker host*,
not inside whatever container you're running this from — the infrastructure
fixtures still bind-mount from the repo (`infra/envoy/envoy.yaml`,
`flaky-upstream.mjs`, the traffic generator, the Prometheus and Grafana
config), and Docker's file-sharing permission check is keyed on the host path,
not a container-internal one. In a
devcontainer this is normally already exported correctly; overriding it with
a container-internal path (e.g. `/workspace`) produces "mounts denied...not
shared from the host" even though Docker itself is working fine. If your own
shell can't reach the published ports afterward (`curl localhost:8088`
hangs or refuses), you're likely on a different Docker network than the
compose project's — every service here also joins the host devcontainer's
own external `devcontainer` network for exactly that reason, so
`curl http://aggregator:8088/metrics` from a shell on that same network
works without needing a published port at all.

Startup used to be ordered around a one-shot `deps` service, because eight
containers racing `pnpm install` against one bind-mounted `node_modules` made
the losers fail on pnpm's store lock. Building an image deletes the problem
rather than sequencing it: dependencies are installed once, at build time, in
a layer that is rebuilt only when a manifest changes. Cold start is now a
container start.

There is still no compile step, and that is deliberate — every process runs
TypeScript directly through node's type stripping, which is why `tsc --noEmit`
is load-bearing in CI rather than cosmetic. The cost of that choice is that
the runtime version matters, so the base image is pinned **by digest** rather
than by the moving `node:22-alpine` tag (currently v22.23.2), as are Envoy,
RabbitMQ, Redis, Prometheus and Grafana. An image that moves is an image
nobody can reproduce.

Every long-running service also carries `restart: unless-stopped`, which is
what makes the crash-fast stance coherent: treating a failure as fatal only
makes sense if "fatal" means "comes back", and without a policy it meant "stays
dead".

Less of the failure surface reaches that policy than it used to. The AMQP
client reconnects and rebuilds its own topology and consumers
([decisions/005](decisions/005-connection-recovery.md)), so a broker restart is
no longer a fleet restart — measured on this stack, every container stayed up
and resumed its counters rather than resetting them. The policy still covers
what recovery cannot: if the client cannot reach the broker for about five
minutes it exits deliberately, and an unhandled defect anywhere else is still
fatal on purpose.

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
the aggregator, which receives each replica's stats individually because
resolving the fleet is its actual job — nothing that merely *calls* the API
needs to know there are three of them, or what they're named. (Each replica
names itself with `--service-node`; see
[Ingestion](architecture.md#ingestion-push-or-poll-decided-by-measurement).)

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
host but Docker: `docker compose --profile demo run --rm demo` — it runs from
the same image every other service does, so it needs no install of its own.

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
| `egress_circuit_sequence` | Last published sequence number per API — the counter the delivery contract is stated in, so a flat line during an incident means nothing was published |
| `egress_circuit_transitions_total` | Published `state_changed` events, by API/state/reason |
| `egress_circuit_snapshots_total` | Periodic full-state republishes, by API |
| `egress_fleet_poll_duration_ms` | Time to poll and parse every replica once per tick |
| `egress_aggregator_ticks_total` | Control-loop iterations per instance — the liveness signal. A zero rate means the loop is gone, which no other metric distinguishes from a quiet system |
| `egress_aggregator_coordination_errors_total` | Ticks that could not reach the coordinator and stood down |
| `egress_aggregator_is_leader` | 1 if this instance currently holds the publishing lease, 0 otherwise — see [High availability](high-availability.md) |
| `egress_aggregator_fencing_conflicts_total` | Checkpoint writes rejected because a newer lease holder already took over, by API |
| `egress_webhook_delivered_total` / `_failed_total` / `_dead_lettered_total` | Sink outcomes, by API |
| `egress_webhook_delivery_duration_ms` | Successful-delivery latency, including retries |
| `egress_webhook_outbox_depth` | Events waiting in the durable outbox for a subscriber that is not taking them, by API — zero in every healthy minute, so a non-zero reading is the whole signal |
| `egress_webhook_outbox_replayed_total` / `_dropped_total` | Events delivered from the outbox after an earlier failure, and events discarded because the per-API bound was hit |
| `egress_subscriber_events_received_total` / `_gaps_total` / `_duplicates_total` | The delivery contract, read from outside the process — same numbers the console's right-hand panel shows, as counters |
| `egress_fleet_replica_lost_total` | Replica reports dropped before they could reach a quorum, by `reason` — `no-node-id`, `went-quiet`, `unreachable`, `incomplete-stats`. The denominator in `Breaker.step` shrinking is what this counts; see [ADR 009](decisions/009-what-the-quorum-is-a-quorum-of.md) and the [FleetShrunk](runbooks/FleetShrunk.md) runbook |

And from the daemon fleet — the same in-process `effect` registry, served on
`METRICS_PORT` by every daemon and by the producer:

| Metric | What it shows |
|---|---|
| `egress_daemon_circuit_state` | The state each daemon *received*, against `egress_circuit_state`, the state the aggregator *published*. They should be indistinguishable |
| `egress_daemon_target_fraction` / `_self_active` / `_floor_held` | What proportion of the fleet should be working, whether this daemon is one of them, and whether it is the one the broker elected to work regardless. The fraction should be identical on every daemon and the floor should sum to 1 — see [ADR 013](decisions/013-the-target-as-a-fraction.md) |
| `egress_daemon_calls_total` | Third-party calls through the egress listener, by outcome |
| `egress_daemon_in_flight` | Third-party calls open right now, per daemon. Bounded by the work consumer's prefetch (`MAX_IN_FLIGHT`), so it is also how close this daemon is to its ceiling — anything beyond it stays in the queue rather than in the process |
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
[High availability](high-availability.md)) and its fencing-conflict counter, and
— scraped directly from each Envoy's own `/stats/prometheus` — the raw
per-replica healthy-host count, so you can see the disagreement the console's
replica strip visualizes, in a second tool, at the same time.

[infra/monitoring/alerts.yml](../infra/monitoring/alerts.yml) evaluates alongside
it — six rules, each one there because something went wrong in a way that
looked fine from outside. `ControlLoopStalled` is the one that matters most;
see the note on transient dependency outages under
[What the build surfaced](findings.md). No Alertmanager is wired,
so they surface in Prometheus's own `/alerts`; routing them to a human is a
deployment concern.

A **RabbitMQ daemon fleet** row sits underneath it, so the reaction is on the
same screen as the cause: work-queue and dead-letter depth (from RabbitMQ's
own `rabbitmq_prometheus`, enabled by default in the management image on
15692), arrival rate against completion rate, each daemon's own view of the
circuit state, and the AMQP-side delivery-contract tiles. The panel worth
knowing is **agreed target vs actually pulling** — every daemon derives the
same target from the same events, so those two lines track each other, and
when they stop tracking, a daemon has gone deaf while still looking healthy.
Finding that by hand once is what put the panel there.

Prometheus scrapes every 2s, so an incident you trigger in the console shows
up within a point or two. It used to also scrape `host.docker.internal:8088`
so the same stack could watch a `pnpm start` sim fleet on the host; that target
is gone, because the compose aggregator publishes 8088 to the host and the
result was Prometheus scraping one process under two instance labels — see
[What the build surfaced](findings.md) for what that did to the
`SplitBrain` alert.

### Alerting, and what to do when one fires

Six rules in [infra/monitoring/alerts.yml](../infra/monitoring/alerts.yml), each
one written because something here failed in a way that looked fine from
outside, plus two SLO burn-rate alerts. They now go somewhere:
Prometheus → Alertmanager → a receiver. The receiver in this stack is
[infra/alert-sink.mjs](../infra/alert-sink.mjs), which logs what it is sent —
deliberately not Slack or PagerDuty, because this repo cannot own anyone's
credentials and the property worth demonstrating is that a firing rule leaves
Prometheus. Swap the webhook URL in
[alertmanager.yml](../infra/monitoring/alertmanager.yml) for a real integration
and nothing else changes.

Alertmanager also does the two things that make alerts readable during an
incident: grouping (by alert and API, so a fleet-wide problem is one
notification), and **inhibition** — while `ControlLoopStalled` is firing,
everything downstream of it is also true and none of it is the cause.

Every rule carries a `runbook_url` pointing into
[docs/runbooks/](runbooks/), one per alert, each starting from what was
actually observed rather than from the metric's name. The link travels with the
notification, so it arrives where the alert does.

The **SLOs** are the two things this system promises a subscriber: that
published events arrive (99.9%, multi-window burn rate at 14.4× and 6×), and
that the per-API sequence is intact. The second one is written down honestly as
what it is — a property whose error budget is zero, so `DeliveryContractBroken`
pages on the first occurrence and the ratio exists only so the *size* of a
violation is visible in the same units as the delivery one.

Verified by breaking it: `docker compose stop redis` at 08:37:53, the rule
pending at +30s, firing at +150s (its `for: 2m`), and the notification in the
sink's log with its runbook path attached. Redis back, and `RESOLVED` followed.

```
FIRING   critical/NoLeaderElected — No aggregator holds the publishing lease (runbook: docs/runbooks/NoLeaderElected.md)
RESOLVED critical/NoLeaderElected — No aggregator holds the publishing lease (runbook: docs/runbooks/NoLeaderElected.md)
```

Tracing is not wired. `@effect/opentelemetry` publishes the exact version this
repo pins, so that is a decision rather than a limitation —
[docs/decisions/003-tracing.md](decisions/003-tracing.md) records why, and
which path here would actually earn a trace.

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
| `replicaTimeoutMs` | 5000 | after this, a replica stops counting toward the quorum |
| `tickMs` | 250 | how often the control loop polls, steps, and publishes |

`dwellMs` and `minStateMs` are not cosmetic. Without them a marginal upstream
generates an event storm, and every subscriber ends up debouncing it themselves
— badly, and differently from each other.
