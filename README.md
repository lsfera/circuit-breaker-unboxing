# RabbitMQ does the coordinating

A fleet of competing consumers, each with its own
[cockatiel](https://github.com/connor4312/cockatiel) breaker. Third step of
the series, built on `article/02-in-process-breaker`.

Article 2 left three problems that need the replicas to coordinate:

- one replica's successful half-open probe can burst `maxInFlight` calls
  at a third party that has been back for milliseconds, on every replica
  at once;
- a message can be dead-lettered without the third party ever seeing it
  (1,975 in one 15s outage);
- nothing brings dead letters back.

None of them needs new infrastructure. RabbitMQ already has the pieces: a
one-message queue for a token, a settlement that doesn't count toward the
delivery limit, and a single-active-consumer election.

```mermaid
flowchart LR
  producer["Producer"] --> queue[("payments-provider.work")]
  permit[("probe-permit\n(1 token)")]
  dead[("work.dead")]
  parked[("work.parked")]
  rtrigger[["redrive-trigger\n(SAC)"]]
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
  queue -.->|"exhausts delivery limit\n(real calls only)"| dead
  b1 -.->|"onReset"| rtrigger
  b2 -.->|"onReset"| rtrigger
  b3 -.->|"onReset"| rtrigger
  rtrigger -.->|"elects exactly one"| dead
  dead -->|"redrive pass"| queue
  dead -.->|"MAX_REDRIVES exceeded"| parked
  classDef new fill:#fde68a,stroke:#b45309,stroke-width:2px,color:#1c1917
  class permit,dead,parked,rtrigger new
  linkStyle 4,5,6,10,11,12,13,14,15,16 stroke:#d97706,stroke-width:3px
```

<sub>Amber: new or changed on this branch.</sub>

Breakers share nothing. Only a half-open breaker touches the permit. Every
replica publishes to `redrive-trigger`; RabbitMQ delivers to one.

30s of `503`s then 10s of `422`s against 5 replicas, recorded 2026-09-24
with `infra/capture-incident.mjs` (5.79× real time,
[video](docs/media/dead-letter-redrive-incident.webm)).

![Grafana during a 30-second sustained outage: all five breakers open, the work-queue depth climbs continuously while the dead-letter queue stays flat at zero, then the work queue drains once the third party recovers.](docs/media/dead-letter-redrive-incident.gif)

In article 2's recording the dead-letter panel moved during an outage; here
it stays at 0 through the outage and the recovery. The price is backlog:
6,920 at peak. It moves only for the `422`s — 1,985 in 10s, each refused on
its first delivery. The permit changes nothing a dashboard shows; it was
checked separately (below).

## The permit: a one-token queue

A queue with `x-max-length: 1` and `x-overflow: reject-publish`. Every
replica seeds a token at startup; RabbitMQ keeps the first and *nacks* the
rest, which `seedPermit` treats as success. (An earlier version assumed
the rejection was silent and crash-looped four replicas on boot.)

A half-open replica does a non-blocking `get` first:

- **Token** → make the real call, then `nack` the token back whatever the
  result.
- **Empty** → fail at once with `Breaker.NoPermit`, no network call;
  cockatiel treats it as a failed probe and backs off.

**Measured:** 20 concurrent half-open probes against the live broker —
exactly one reached a real call every time, the other 19 failed instantly.

A single-active-consumer queue doesn't fit here: it hands over on
disconnect, not when a replica's backoff elapses.

## Counted attempts: release, don't requeue

`decide()` *releases* an `open` outcome (breaker open or permit lost, no
call made). RabbitMQ 4.3 doesn't count a release toward `x-delivery-limit`
(3); it does count a requeue, so three deliveries onto open breakers used
to dead-letter a message the third party never saw. `failed` still
requeues — a real call spends the budget. `master`'s `Attempts.ts` draws
the same line.

**Measured:** 1,785 dead-lettered in a 15s outage before; zero in a 40s
outage after. The work queue grows instead (6,800 at 40s, still climbing).

## The redrive: single-active-consumer

`Redrive.ts`, ported from `master`:

- **Election:** a trigger queue with `x-single-active-consumer` — the
  broker delivers to one replica and promotes another if it disconnects.
- **A pass** `get`s from `work.dead`, re-checks the gate per message,
  publishes to `work` (bumping `x-egress-redrive-count`) or, past
  `MAX_REDRIVES` (5), to `work.parked`, then acks. A crash in between
  duplicates, never loses. At most 200 per pass.
- **Gate:** the elected replica's own breaker is closed.
- **Triggers:** `onReset`, startup, and a 30s sweep. Without the sweep
  `work.dead` sat at 1,225 for 20s+ with every breaker closed; `master`'s
  ADR 016 fixes the same stall the same way.
- **Idempotency:** the republish keeps the original `message_id`.

## The breaker

As in article 2: 5 consecutive failures trip it, half-open after 1s–30s
exponential backoff. `classify` (`Breaker.ts`) gives four outcomes: `ok`
accept, `client_error` (4xx except 408/429) dead-letter at once, `failed`
requeue, `open` release after a 100–400ms hold. A 4xx that is really ours
(401, 403, 404) dead-letters every message it touches; the `status` label
on `egress_consumer_calls_total` shows it.

## What this still doesn't fix

- **The five breakers still don't agree.** The permit stops the recovery
  burst, not independent tripping.
- **Redrive waits on the elected replica's breaker**, up to 30s after the
  rest of the fleet has closed. Nothing knows what the fleet thinks.
- **A long outage grows the work queue without bound.** The delivery limit
  used to cap it by accident.
- **Losing the permit race grows backoff** like a failed probe — cockatiel
  can't tell them apart.
- **The permit is one more dependency:** if its token is lost, half-open
  probes fail until a restart reseeds it.
- **A trigger arriving mid-pass is dropped.**

## Running it

```bash
pnpm install
docker compose up -d
docker compose up -d --scale rmq-consumer=12   # resize the fleet, no restart needed
```

- RabbitMQ: <http://localhost:15672> (guest/guest).
- Grafana: <http://localhost:3000/d/in-process-breaker/in-process-breaker-e28094-five-not-one>
  (anonymous; two 401 toasts on first load are harmless). New panels:
  "Parked queue depth" and "Redrives".
- Prometheus: <http://localhost:9090>.

Inside the devcontainer use service names (`rabbitmq`, `grafana`, …); a
gitignored `.env.local` with them is read by `pnpm run incident`.

## Injecting a failure

`flaky-upstream` is the third party; each POST replaces its behaviour:

```bash
curl -X POST localhost:8080/__fail -d '{"rate":1.0}'                # 503s
curl -X POST localhost:8080/__fail -d '{"rate":1.0,"status":422}'   # 422s: refused, not down
curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"hang"}'  # never answers
curl -X POST localhost:8080/__fail -d '{"rate":1.0,"mode":"reset"}' # drops the connection
curl -X POST localhost:8080/__fail -d '{"delayMs":1500}'            # slow, still correct
curl -X POST localhost:8080/__fail -d '{}'                          # healthy again
```

## The incident script

```bash
pnpm run incident
MODE=hang node infra/incident.mjs
STATUS=422 node infra/incident.mjs
```

Injects a failure, restores, and reports peak backlog, dead-lettered,
drain time, audit counts, breaker agreement, and what redrive moved or
parked. Unless `STATUS` is set, it ends with a 422 phase.
`infra/capture-incident.mjs` records the dashboard (needs `playwright-core`).

## Load

```bash
RATE_PER_SECOND=500 docker compose up -d rmq-producer
```

The producer never backs off.

## Chaos testing

`infra/chaos-load.mjs` injects faults under load and fails if any
confirmed message is neither processed nor still queued.

```bash
node infra/chaos-load.mjs --list
node infra/chaos-load.mjs                                    # every fault
node infra/chaos-load.mjs --faults=kill-one-consumer
node infra/chaos-load.mjs --rate=500 --spike=3000 --fault-seconds=25
```

Faults: `kill-one-consumer`, `kill-all-consumers`, `kill-broker`,
`flaky-storm`. First run (2026-09-19, ~350k messages): all passed, zero
unaccounted. `work.dead` stayed at 0, so the redriver never fired under
chaos; after `flaky-storm` a breaker can stay open 90s+ at its backoff cap.

## Layout

```
packages/
  config/        settings, declared once and decoded at boot
  rmq/           Effect wrapper over amqplib (Client.ts); queue names,
                 options and wire schemas shared by every process
                 (ControlPlane.ts)
  rmq-producer/  steady load onto <apiId>.work, message_id as idempotency key
  consumer/      the fleet: Breaker.ts (breaker + permit), Redrive.ts,
                 Upstream.ts (the HTTP call), consumer.ts (wiring, and decide())
  tracing/       /metrics route; OpenTelemetry, off unless OTEL_EXPORTER_OTLP_ENDPOINT is set
infra/
  flaky-upstream.mjs   the fake third party, with an audit trail
  incident.mjs         drives one incident and reports on it
  capture-incident.mjs records the dashboard through one
  chaos-load.mjs       faults under load, judged on per-message correctness
  chaos-publisher.mjs  its load generator
  rabbitmq.conf        the broker's flow-control watermark
  monitoring/          Prometheus scrape config, Grafana dashboard
docker-compose.yml     the whole stack
```

Effect 4 (4.0.0-rc.116) — see `AGENTS.md` before writing Effect code. No
build step: packages run from `src/*.ts` through Node's type stripping.

## Verification

```bash
pnpm run check       # vendored-version check, typecheck, unit tests
pnpm run test:rmq    # needs Docker: AMQP behaviour against a real broker
```
