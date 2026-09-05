# Production readiness: everything but security

Closes every gap in the readiness audit **except security**, which is
Phase 10 and is **documentation only** — no auth, no TLS, no secret handling
is to be built by this plan. Say what is missing and what a real deployment
must do, and stop there.

Each phase is self-contained and can run in its own session. Phases 1-3 are
ordered by dependency; 4-9 are independent of each other once 3 lands.

---

## Phase 0 — Documentation discovery (facts, measured, not assumed)

Everything in this section was verified in this repo or against a live
container on 2026-09-05. Later phases must not re-derive it, and must not
contradict it without re-measuring.

### Allowed APIs — Effect 4.0.0-rc.112 (`pnpm-workspace.yaml:16`)

Verified by importing `effect` from `packages/aggregator` and inspecting the
runtime surface:

| API | Exists | Use for |
| --- | --- | --- |
| `Effect.acquireRelease` | yes | resources tied to a scope — pattern at `packages/aggregator/src/main.ts:112` and `packages/rmq/src/Client.ts:265` |
| `Effect.addFinalizer` | yes | run something on scope close without owning an acquire |
| `Effect.ensuring`, `Effect.onExit`, `Effect.onInterrupt`, `Effect.onError` | yes | shutdown hooks on a specific effect |
| `Scope.addFinalizer`, `Scope.close`, `Scope.fork` | yes | explicit scope work |
| `Layer.scope` | **does not exist** | — do not reach for it |

Other repo-local facts: `Effect.catch` is v4's failure-only catch (`catchAll`
is gone); `catchCause` also catches defects and is used deliberately in
exactly three places (sinks, HTTP routes) — see commit `ed3c042`.

### Allowed APIs — RabbitMQ quorum queues through `@egress/rmq`

Measured against `rabbitmq:4.0-management-alpine` via the repo's own client
(throwaway probe, not kept):

```
quorum dead queue (durable)            : OK
quorum work queue + DLX args           : OK
quorum + x-single-active-consumer      : OK
quorum + durable:false                 : REJECTED 400 "invalid property 'non-durable' for queue ..."
dead-letter quorum -> quorum           : OK  (3/3 landed)
SAC elects one consumer on a quorum q  : OK  (a=6 b=0)
```

Consequences that Phase 2 must respect:

- `x-queue-type: "quorum"` passes through `declareQueue`'s `args` verbatim,
  like every other non-string argument (`packages/rmq/src/Client.ts:288`).
- **A quorum queue must be durable.** The transient control queue cannot
  become quorum without also becoming durable — that is a decision, not a
  formality (see `packages/rmq/src/ControlPlane.ts:96` for why control queues
  are transient today).
- Dead-lettering and single-active-consumer both work on quorum queues, so
  neither election nor the dead-letter design has to change.
- **`x-delivery-limit` on a quorum queue is a broker-enforced redelivery
  budget**, and it works through the AMQP 1.0 client already in use. Measured
  with a handler returning `requeue` every time: four deliveries, then
  dead-lettered with `reason "delivery_limit"`. The client still reports
  `deliveryCount: 0` on every delivery and does not need to. This overturns
  the "a redelivery budget cannot be expressed" finding — see
  `docs/decisions/001-amqp-client.md`.
- On AMQP 0-9-1 (`amqplib`, measured in the same spike): channel prefetch does
  **not** affect an already-registered consumer (unacked stayed at 1 across
  `prefetch(5)` and `prefetch(10)`); a consumer registered after the change
  gets it, and cancel + re-consume applies it to an existing one. Live credit
  reduction is therefore not a capability either protocol offers.

### Redeclare is not a merge (measured, both directions)

```
409 "inequivalent arg 'durable' for queue 'mix.q' in vhost '/':
     received 'true' but current is 'false'"
```

Any queue-property change — durability, `x-queue-type`, dead-letter target —
fails against a broker that already holds the queue. Phase 2 owns the
migration procedure; no phase may assume a redeclare will silently upgrade.

### Repo-local facts the plan depends on

- `LeaderElection.release` is defined in `packages/aggregator/src/Coordination.ts`
  and **never called** — `grep -rn "\.release(" packages/aggregator/src` returns
  nothing. The lease is always surrendered by TTL expiry.
- The tick loop is `Effect.repeat(Schedule.spaced(...))` at
  `packages/aggregator/src/Aggregator.ts:356`; `AggregatorLayer` is
  `Layer.effect` at `:60`; `demote` is at `:98`.
- HTTP routes are registered in `packages/aggregator/src/Http.ts` (`/api/state`
  at `:173`); failure injection is `POST /api/failure` at `:218`.
- The webhook sink retries 3x exponential with a 2s timeout, then drops into an
  in-memory `DEAD_LETTER_BUFFER = 200` (`packages/aggregator/src/Events.ts:26`).
- No Dockerfiles exist. Every service bind-mounts the workspace and depends on
  a `deps` one-shot running `pnpm install` (`docker-compose.yml:69`).
- No `volumes:` anywhere in `docker-compose.yml` — Redis, RabbitMQ, Prometheus
  and Grafana all keep state in the container writable layer.
- `infra/monitoring/prometheus.yml` loads `alerts.yml` (6 rules) and states in
  a comment that no Alertmanager is wired.
- CI (`.github/workflows/ci.yml`) already runs typecheck + 49 unit tests, then
  `test:redis` (6) and `test:rmq` (9) against real containers.

### Anti-patterns to avoid in every phase

- Inventing Effect combinators (`Layer.scope`, `catchAll`) — check the table above.
- Assuming a queue redeclare upgrades properties.
- Adding an unbounded in-memory buffer — the repo has already been bitten
  (`DEAD_LETTER_BUFFER`, the 20-message undecodable sample, `REDRIVE_MAX`).
- Writing a claim into the README that was not run. Every phase below ends
  with something measured.

---

## Phase 1 — Two decision records that scope everything after them  ✅ 001 done, 002 open

**What to implement.** `docs/decisions/` with two records, each stating the
decision, the evidence, and what it costs. Not speculative design — both
questions already have most of their evidence in the repo.

1. `docs/decisions/001-amqp-client.md`. The fleet runs
   `rabbitmq-amqp-js-client` 1.0.0, with three silent bugs worked around in
   `packages/rmq/src/Client.ts` and a documented, unpinned hazard: dead-lettering
   becomes unreliable after the stranding bug is provoked
   (`docs/rmq-control-plane.md`, "Dead-lettering stops being reliable..."). The
   record decides between: (a) keep it, own the workarounds, gate upgrades on
   `test:rmq`; (b) move the fleet to AMQP 0-9-1 / `amqplib`, which also restores
   settable prefetch (`DEGRADED` as credit reduction) and a redelivery budget.
   **Bounded spike required before deciding**: one throwaway file that
   reproduces three properties on `amqplib` — SAC election, settle-after-call
   backpressure, and live prefetch change — and records whether the stranding
   hazard reproduces there. Delete the spike; keep the numbers.
2. `docs/decisions/002-enforcement-authority.md`. Answers the README's own
   open fork ("must the aggregator's `OPEN` be authoritative?"). If
   observational: say so, and the xDS push path is formally out of scope. If
   authoritative: this record enumerates what follows (xDS server, in-band
   failure mode, config rollback) and becomes its own plan.

**Doc references.** `README.md` "The fork this defers"; `docs/rmq-control-plane.md`
sections "Concurrent link creation is broken in this client", "Closing a
consumer with deliveries in flight kills the connection", "There is no way to
say 'this attempt failed, try again'".

**Verification.** Both records exist, each names the evidence it rests on, and
`README.md` links them from "What is a prototype, not production". The spike's
measurements are quoted in 001 — not summarised as "worked" or "didn't".

**Anti-pattern guards.** Do not start a client migration inside this phase. Do
not write an xDS server on spec.

---

## Phase 2 — Durable state: survive the process, the node, and the restart  ✅ done

**What to implement.**

1. `docker-compose.yml`: named volumes for `redis`, `rabbitmq`, `prometheus`,
   `grafana`. Redis gets `command: redis-server --appendonly yes` (AOF) —
   the checkpoint store is the one piece whose loss costs sequence continuity.
2. Promote the two queues that hold work to quorum in
   `packages/rmq/src/ControlPlane.ts`: `workQueueOptions` and
   `deadLetterQueueOptions` gain `"x-queue-type": "quorum"` alongside the
   `durable: true` they already carry.
3. Add `x-delivery-limit` to `workQueueOptions` now that Phase 1 measured it
   as a working, broker-enforced redelivery budget on this client
   (`docs/decisions/001-amqp-client.md`). Pick the limit deliberately — it is
   the number of attempts a failed third-party call gets before the work is
   parked — and rewrite the two places that currently say a budget cannot be
   expressed: the amended section in `docs/rmq-control-plane.md` and the
   amended bullet in `README.md`. The daemons need no code change; a handler
   that returns `discard` today may want to return `requeue` for retryable
   failures, which is a policy decision to make explicitly, not by default.
4. Decide and write down what happens to `sacQueueOptions`. The election
   queues are always empty, so making them quorum costs nothing but forces
   `durable: true` (Phase 0, measured). A classic transient election queue
   dies with its node, taking the election with it; that is the argument for
   quorum. Whichever way it goes, the comment at `ControlPlane.ts:96` must say
   why.
5. `docs/rmq-control-plane.md`: a "Changing queue topology on a live broker"
   procedure, built on the measured `409` — drain, delete, redeclare, in that
   order, and what to do when the queue cannot be drained first.
6. `README.md`: update "What is a prototype, not production" — Redis without
   persistence and RabbitMQ without a volume are no longer accurate once this
   lands. Replace, do not append.

**Doc references.** `packages/rmq/src/ControlPlane.ts:96-130` (the durability
split as written today); Phase 0's measured quorum results; RabbitMQ quorum
queue docs for the properties this plan does *not* cover (`x-quorum-initial-group-size`,
memory/disk behaviour) — read before choosing defaults.

**Verification.**
- `pnpm run test:rmq` stays green (9 tests).
- One new integration test: a quorum work queue keeps its messages across
  `container.restart()`, and dead-lettering still lands quorum → quorum. Pattern:
  the durability test at the end of `packages/rmq/test/integration/DeadLetter.test.ts`.
- `docker compose down && docker compose up -d`, publish, `docker compose restart
  redis rabbitmq`, and confirm the aggregator resumes from its checkpoint and the
  work queue still holds its messages. Record the numbers in the commit message.

**Anti-pattern guards.** Do not make the control queue quorum without stating
the durability consequence. Do not claim "durable" in the README while the
broker still has no volume — that pairing is the whole point of this phase.

---

## Phase 3 — Deployable artifacts  ✅ done

**What to implement.**

1. A multi-stage `Dockerfile` per runnable package (`aggregator`,
   `rmq-consumer`, `subscriber`) or one parameterised image — the workspace
   installs once with `pnpm install --frozen-lockfile --prod` and the source is
   copied in, not bind-mounted. Node pinned to the exact 22.x that
   `package.json#engines` allows.
2. `docker-compose.yml` switches those services from `image: node:22-alpine` +
   bind mount + `command: node --experimental-strip-types ...` to `build:` +
   the image's own entrypoint. The `deps` one-shot and its `depends_on`
   conditions disappear with it.
3. Decide on the type-stripping question in the same pass: either keep
   `--experimental-strip-types` and pin the runtime exactly, or add a `tsc`
   build to the image. Write the reason next to whichever you choose.
4. `.github/workflows/ci.yml`: a third job that builds every image and runs
   `docker compose config` against the result. Pin base images by digest.

**Doc references.** `docker-compose.yml:69` (the `deps` service and the race it
replaced — the same reasoning explains why an image removes the problem
entirely); `.github/workflows/ci.yml` for the existing job shape and its
comments about the Node floor.

**Verification.**
- `docker compose build && docker compose up -d` boots the full stack with no
  bind mount of `packages/`.
- The demo script from the README still runs end to end against the built
  images: `OPEN → HALF_OPEN → PROBE_SUCCEEDED → CLOSED` through `/api/state`.
- `docker image inspect` shows the pinned Node version; CI's build job is green.

**Anti-pattern guards.** No `latest` tags. No `pnpm install` at container start.
Do not delete the bind-mount dev path without checking `README.md`'s "Running
against real Envoy" instructions still work — update them if not.

---

## Phase 4 — Lifecycle: release the lease, split liveness from readiness  ✅ done

**What to implement.**

1. Release the lease on shutdown. `LeaderElection.release` exists and is never
   called (Phase 0). Add a finalizer in `AggregatorLayer`
   (`packages/aggregator/src/Aggregator.ts:60`) so an instance that is
   shutting down surrenders the lease instead of holding it to TTL expiry.
   Use `Effect.addFinalizer` or `Effect.acquireRelease` — both verified
   present. It must release **only** if this instance still holds it, which
   `release(holderId)` already enforces.
2. `/livez` and `/readyz` in `packages/aggregator/src/Http.ts` (routes are
   registered around `:173`). Liveness fails when the control loop has stalled
   — the `egress_aggregator_ticks_total` counter added in `ab73516` is exactly
   that signal. Readiness is **not** leadership: a standby serves the read-only
   API and must stay ready, or a rolling deploy will take the whole pair out.
   Write that reasoning into the code comment.
3. Daemon side: confirm a `SIGTERM` lets in-flight third-party calls settle
   rather than stranding deliveries (`packages/rmq-consumer/src/daemon.ts`,
   the `finally { release() }` at `:265` and the scoped work connection). If it
   does not, make the shutdown path wait on the permit.

**Doc references.** `packages/aggregator/src/Coordination.ts` (`release`'s
holder check); `packages/aggregator/src/main.ts:100-118` for the repo's
existing "resource dies with the scope" pattern; commit `ab73516` for why
`ticks_total` is the liveness signal.

**Verification.**
- New unit test in `packages/aggregator/test/Coordination.test.ts`: an instance
  that shuts down cleanly leaves no lease, and its peer acquires on the next
  tick rather than after the TTL.
- Measured on the running stack: time from `docker compose stop aggregator` to
  the standby's first published event, before and after. Put both numbers in
  the commit message — the claim is "a rolling deploy no longer costs up to
  `leaseTtlMs` of leaderlessness", so it needs two measurements.
- `curl /livez` and `/readyz` on a leader and on a standby: both ready, both
  live.

**Anti-pattern guards.** Readiness must not report the standby as unready. Do
not release the lease on a *demotion* (losing it is already handled at
`Aggregator.ts:98`) — only on shutdown.

---

## Phase 5 — Delivery that survives the subscriber being down  ✅ done

**What to implement.** A durable outbox behind `EventSink`, replacing the
in-memory `DEAD_LETTER_BUFFER` as the last line of defence.

1. Persist undelivered events per API, in sequence order, through the same
   `RedisLike` port `Coordination.ts` already uses — one Lua script, no new
   dependency, same "reasoned from documented semantics, then run against a
   real container" pattern.
2. Drain on recovery, in order, with a bound on both the queue length and the
   drain rate. Ordering per API is the guarantee this repo exists to protect;
   an outbox that replays out of order is worse than dropping.
3. Keep the in-memory buffer as the fast path for the current process, and say
   in the comment which one is authoritative.
4. Metrics: outbox depth per API, oldest-entry age, drops when the bound is
   hit. An alert on depth belongs in Phase 9.

**Doc references.** `packages/aggregator/src/Events.ts:120-200` (the sink, its
retry schedule, the buffer); `packages/aggregator/src/Coordination.ts`'s
`CHECKPOINT_SCRIPT` for the Lua + `evalGuarded` pattern, including why a
rejected promise must be a failure and never a defect.

**Verification.**
- Integration test alongside `test/integration/RedisCoordination.test.ts`: kill
  the subscriber, publish N events, bring it back, assert all N arrive once, in
  order, with no gap — the same properties `/api/subscriber` checks.
- The bound is exercised, not just written: overflow drops the oldest and the
  metric counts it.

**Anti-pattern guards.** No unbounded list. Do not let outbox drain failures
kill the tick loop — `catchCause` at the sink boundary is deliberate
(`ed3c042`).

---

## Phase 6 — Ingestion: polling to push

**What to implement.** Replace `EnvoyFleetLayer`'s admin `/stats` polling
(`packages/aggregator/src/FleetSource.ts:275`) with Envoy's push-based metrics
sink, behind the *same* `FleetSource` interface so nothing downstream changes —
the README already names this as the production path.

1. **Read the Envoy docs first and write down the exact names** before any
   code: the stats sink extension, its config message, and the gRPC service
   Envoy calls. The README calls it `envoy.service.metrics.v3.MetricsService`;
   confirm that against the Envoy 1.31 docs (the version pinned in
   `docker-compose.yml`) rather than trusting the README.
2. Decide the codegen story — this is the reason polling was chosen — and
   record it. A gRPC server in this workspace needs generated stubs, which is
   the first build step the repo has ever had.
3. Keep `SimFleetLayer` and the polling layer intact. Three layers, one
   interface; polling stays as the fallback and as what the unit tests drive.

**Doc references.** `packages/aggregator/src/FleetSource.ts:275` and the
`ReplicaReport` shape it produces; `infra/envoy/envoy.yaml` `stats_sinks`
placement; `README.md` "Ingestion is polling".

**Verification.**
- Both layers produce byte-identical `ReplicaReport`s for the same fleet state.
- The end-to-end demo runs on the push layer with the admin port no longer
  polled — confirmed by watching Envoy's own request count for `/stats` stay
  flat during an incident.
- Detection latency before and after, measured, in the commit message.

**Anti-pattern guards.** Do not invent Envoy config keys. Do not change
`ReplicaReport` — if the push path carries more (node ID, for instance), add a
field, do not reshape the record.

---

## Phase 7 — The recovery ramp advances on time, not on chatter

**What to implement.** Today a ramp rung advances per *control event*, so its
pace is set by `snapshotMs` — "a ramp in shape but barely one in duration"
(`docs/rmq-control-plane.md`, "What's still missing"). Gate each rung on
elapsed time or on N successful calls at the current rung, in
`packages/rmq-consumer/src/DaemonPolicy.ts`, which is pure and already has 9
tests.

**Doc references.** `packages/rmq-consumer/src/DaemonPolicy.ts` and
`test/DaemonPolicy.test.ts`; `docs/rmq-control-plane.md` "What's still missing".

**Verification.** New pure tests for the time/success gate, including a relapse
mid-ramp. Then live: the same incident, with the observed `1 → 4 → 5` timings
before and after in the commit message.

**Anti-pattern guards.** Keep `DaemonPolicy.step` pure and total — take elapsed
time or a success count as an argument, do not read a clock inside it.

---

## Phase 8 — Scale and resilience, measured

**What to implement.**

1. A load harness that answers the unmeasured questions: aggregator tick cost
   at hundreds of APIs × replicas, Redis round-trips per tick, SSE fan-out
   cost, metric cardinality. Extend `SimFleetLayer` with a synthetic
   `--apis=N`, which is cheaper and more honest than N real Envoys.
2. A soak run — hours, not minutes — watching for the failure modes this repo
   has already hit once: unbounded growth, a stalled loop, a deaf daemon.
3. Automate the two adversarial tests that were only ever run by hand: hard
   `docker kill` of the leader mid-incident, and `docker kill` of the elected
   prober. A script, run on demand, that asserts the outcome instead of asking
   a human to read logs.
4. The partition case that has never been tested: Redis reachable from one
   aggregator and not the other. Both instances must not act as leader; the
   one that cannot confirm its lease must stand down (`Aggregator.ts:113-122`).

**Doc references.** `infra/traffic-generator.mjs` and `packages/demo/src/driver.ts`
for existing drive-it-over-HTTP patterns; `README.md` "Two real aggregator
instances, one shared Redis" for the manual procedure being automated;
commit `ab73516` for the stand-down behaviour under coordination failure.

**Verification.** Numbers, in `README.md`, in a new "Measured limits" section:
ticks/second at N APIs, p99 tick duration, memory after the soak, and the
observed failover time from the automated kill test. A phase that produces no
numbers has not run.

**Anti-pattern guards.** Do not tune anything mid-measurement. Do not
substitute a benchmark of the pure state machine for a measurement of the
running system.

---

## Phase 9 — Operations

**What to implement.**

1. Alertmanager: a service in `docker-compose.yml`, an `alerting:` block in
   `infra/monitoring/prometheus.yml` (which currently says in a comment that
   routing is a deployment concern), and one receiver that visibly fires.
2. A runbook per rule in `infra/monitoring/alerts.yml` — all six exist because
   something went wrong in a way that looked fine from outside, so each runbook
   has a real incident to describe. Link them from the alert annotations.
3. SLOs for the two properties this system actually promises: event delivery
   latency, and the gapless per-API sequence. Burn-rate rules over the existing
   counters.
4. Tracing, if and only if the package that provides it is verified to exist
   for `effect@4.0.0-rc.112` — check before planning work on it.

**Doc references.** `infra/monitoring/alerts.yml` (the six rules and their
comments); `infra/monitoring/prometheus.yml`'s note about Alertmanager;
`README.md` "Metrics & monitoring".

**Verification.** Break something on purpose — stop Redis, kill the leader,
publish a malformed control event — and show the alert reaching the receiver.
Screenshot or log line in the commit; a rule that has never fired is not
verified.

**Anti-pattern guards.** No alert without a runbook. Do not add rules for
metrics that do not exist yet (Phase 5's outbox metrics land with Phase 5).

---

## Phase 10 — Security: documented, not built

**Explicitly no implementation.** No auth, no TLS, no secret store, no signing.
Write `docs/security.md` and link it from `README.md`'s "What is a prototype,
not production", stating plainly, with file references, what is absent:

- No authn/authz on any HTTP surface: `/api/state`, the SSE stream,
  `/subscriber/webhook`, and — the sharpest one — failure injection at
  `POST /api/failure` (`packages/aggregator/src/Http.ts:218`), which must not
  exist in a production build or must be behind auth and a flag.
- Envoy's admin port published on `19001-19003` (`docker-compose.yml:87`);
  admin carries `/quitquitquit` and a full config dump.
- RabbitMQ `guest/guest` with the management UI published; Redis
  unauthenticated; no TLS on any hop, including AMQP and Redis.
- Webhook events are unsigned (`packages/aggregator/src/Events.ts:127-140`):
  a subscriber cannot verify origin. Note that `idempotency-key` and
  `ce-partitionkey` are already sent, so signing is an added header rather
  than a redesign.
- No secret handling anywhere: `.env` is tracked in git and holds only
  `HOST_WORKSPACE_FOLDER` today, which is exactly how the next secret ends up
  committed.
- HTTPS egress needs TLS interception for any L7 signal to exist — already in
  the README, cross-reference rather than restate.

For each: one line on what production must do. This document is the deliverable;
it is not a to-do list this plan executes.

**Verification.** Every claim cites a file and line, and each was re-read while
writing — no inherited assertions. `README.md` links it.

**Anti-pattern guards.** Do not add a token check "while you are in there".
Do not soften the failure-injection entry — it is the one a reader must not
skim past.

---

## Phase 11 — Final verification

1. `pnpm run check` (typecheck + 49 unit tests), `pnpm run test:redis`,
   `pnpm run test:rmq` — all green, with the new tests from Phases 2, 4 and 5
   counted in the repo map at `README.md`'s Layout section.
2. `docker compose build && docker compose up -d` from clean, then the README's
   own demo script end to end against built images.
3. Restart every stateful service (`redis`, `rabbitmq`) and confirm the
   properties Phase 2 claims: checkpoints and queued work both survive.
4. Grep for the anti-patterns this plan warns about: `Layer.scope`, `catchAll`,
   `latest` image tags, `pnpm install` in a container command, any unbounded
   array push in a sink or handler.
5. Reconcile the docs with reality: every "prototype, not production" bullet in
   `README.md` is either fixed and rewritten, or still true and left alone.
   `docs/security.md` is the only place where a known gap is documented rather
   than closed.
