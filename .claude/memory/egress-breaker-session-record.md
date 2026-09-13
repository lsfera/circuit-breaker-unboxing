---
name: egress-breaker-session-record
description: Narrative record of working sessions on the egress circuit breaker demo (/workspace) that the user may turn into an article — early build-out, plus the later measurement-overturns-belief beats.
metadata: 
  node_type: memory
  type: project
  originSessionId: 431524e5-ab62-4373-9b9b-5ea98edcf3e7
  modified: 2026-09-03T10:41:39.907Z
---

The user said explicitly: "record this conversation - we may provide an
article about it." This is the record, kept as a narrative arc rather than a
changelog, since that's what an article would draw on. All work landed on
`master` in `/workspace` (the "egress circuit breaker demo" repo — see
`README.md` for the system itself: Envoy per-replica circuit breaking +
control-plane aggregator publishing one coherent event stream per API).

**Why this might become an article**: the session is a fairly complete
worked example of *using an AI pair programmer on a real distributed-systems
problem, including three separate mistakes surfaced along the way — two
self-caught by testing, one caught by the user*. The fencing bug (step 7) is
the strongest self-caught material — a genuine, well-known-hard correctness
issue (fencing tokens must check a shared counter, not a per-resource one),
introduced, then caught by writing the test that was supposed to prove the
fix worked, before ever running it. The test-skip race (step 8, in the Redis
integration suite) is a good companion because it's a *different kind* of
mistake — an ordinary "when does this hook actually run" bug, not a
distributed-systems subtlety — caught the same way: deliberately breaking
the thing being guarded against and watching what actually happened. The
third (step 9) is the odd one out and arguably the best closer: a stale
"Docker can't bind-mount here" assumption, re-tested, produced a failure
that *looked like* confirmation but was actually a self-inflicted
environment misconfiguration — and it took the user pointing at
`$HOST_WORKSPACE_FOLDER` to catch it, not another round of testing. Ends
with the most concrete payoff of the session: a real `docker kill` of a
leader mid-incident, watched failing over correctly between two actual
containers. That arc — two self-caught bugs, one user-caught assumption,
ending in a live adversarial proof — is a more interesting story than "AI
wrote code that worked the first time."

## The arc, in order

1. **Adaptive concurrency filter.** The README had explicitly called this
   Envoy filter "worth knowing about but deliberately unused." Enabled it in
   `infra/envoy/envoy.yaml`'s `http_filters` chain and updated the README's
   rationale section to match — small, but the first sign the repo was
   being treated as a living system rather than a fixed artifact.

2. **Monorepo restructuring.** The user asked to "move to a monorepo
   approach... every component well visible." Split a single `src/` tree
   into `packages/domain`, `packages/aggregator`, `packages/subscriber`,
   `packages/demo` (real `workspace:*` dependency edges, not shared-folder
   imports), moved non-package infra under `infra/`, added a pnpm catalog
   for shared dependency versions. Verified with `tsc --noEmit`, all tests,
   `docker compose config`, and a live end-to-end demo run before
   committing — this verification habit held for every later change too.

3. **Skill cleanup.** Removed installed-plugin skills irrelevant to this
   stack (React, Vercel, shadcn, Waku, OpenAPI-client, REST-endpoint,
   new-bounded-context) after the user asked to "remove all unused skills,"
   once a clarifying question pinned down what "unused" meant here
   (stack-irrelevance, not literal usage history).

4. **The RabbitMQ control-plane tangent.** The user posed a related but
   separate scenario: RabbitMQ competing consumers calling a flaky
   third-party service, needing circuit-breaker events to drive compensating
   actions (prefetch changes, consumer cancel/resume) without a thundering
   herd. This became a full design conversation — self-throttling daemons,
   an `AmqpControlPlaneSink` peer to the existing `WebhookSink`, and
   eventually `x-single-active-consumer` for `HALF_OPEN` prober election,
   with the key nuance that SAC has to be scoped to a dedicated coordination
   queue, not the primary work queue. Full detail is in
   [[rmq-control-plane-design]] — held as a design, explicitly not built
   into the repo yet, at the user's request.

5. **The topology-hiding principle, stated then implemented.** Mid-design,
   the user made an architectural point: "consumers do not need to be aware
   of Envoy topology; it's a transparent proxy from their standpoint." This
   was first captured as a design invariant for the (still hypothetical) RMQ
   work, but then made concrete in the actual repo: `infra/traffic-generator.mjs`
   had hardcoded `envoy-00/01/02` by name. Fixed by giving all three replicas
   a shared Compose network alias (`envoy`) and having the generator discover
   them via `dns.resolve4` instead of a hardcoded list — the same pattern
   flagged as reusable for the RMQ daemons' egress calls later.

6. **Comparative research: where does this design sit among real prior
   art?** Asked to analyze effectiveness against "more opinionated
   approaches." Researched Netflix Hystrix+Turbine (closest historical
   analog — aggregates per-instance breaker streams, but for a dashboard,
   not automated action, and Hystrix has been in maintenance mode since
   2018), Prometheus Alertmanager grouping/inhibition (the actual dominant
   modern answer — arrival-based batching with built-in HA clustering, but
   no gapless/ordering guarantee), Istio/Linkerd (confirmed via search that
   service mesh control planes centralize *policy* distribution only — the
   per-sidecar ejection decision stays local, nobody reconciles it back into
   one signal), and SRE multi-window burn-rate alerting (a more statistically
   principled alternative to quorum-voting, at the cost of speed/locality).
   Conclusion: this repo occupies a real, underserved niche — a formal,
   gapless, automation-consumable event contract — between "config fanout
   with no reconciliation" and "human-facing best-effort aggregation." The
   one directly transplantable idea from that research was Alertmanager's
   HA clustering model, which seeded the next step.

7. **Aggregator HA, actually built.** The README had flagged aggregator
   state as in-memory/non-replicated since the first commit. Implemented
   `packages/aggregator/src/Coordination.ts`: `LeaderElection` (lease +
   strictly-increasing fencing token on genuine handoff) and
   `CheckpointStore` (so a new leader resumes `sequence`/`openBackoffMs`
   instead of restarting at zero). Wired into `Aggregator.ts`'s tick loop —
   a non-leader polls/steps/publishes nothing. `main.ts` defaults to the
   in-memory layer (solo mode = "one instance that always wins its own
   lease," not a special case); a `RedisCoordinationLayer` is written against
   a minimal `RedisLike` port (one `eval` method) so a real client plugs in
   without pinning a new dependency, following the repo's existing
   "reasoned from documented semantics, not run against live infra here"
   honesty pattern for Envoy/Docker.

   **The catch worth highlighting for an article**: the first version of
   `CheckpointStore.save` fenced each API's checkpoint independently — reject
   a write only if a *newer* write had already landed for that exact key.
   That's not real fencing: it only stops a stale writer once someone else
   has already touched that specific key, leaving a genuine split-brain
   window open for any API the new leader hasn't published for yet — exactly
   the GC-pause race fencing tokens exist to prevent. Caught this while
   designing the test meant to prove the fix worked (a stale token should be
   rejected "even for an API no one has checkpointed yet"), before ever
   running it — writing the test surfaced the gap in the implementation it
   was testing. Fixed by fencing against the one shared lease-token counter
   instead. `Coordination.test.ts` pins this down directly, plus a test that
   runs two full `Aggregator` instances against one shared in-memory
   coordinator to exercise a real failover (sequence continuity,
   `previousState` correctness) in a single process. All 25 tests
   (20 pre-existing + 5 new) pass, confirmed stable across repeated runs.
   README gained a new "High availability" section and an entry in "What the
   build surfaced" documenting the bug plainly.

8. **Redis integration tests — a second caught-by-testing bug, same
   session.** Asked to make `RedisCoordinationLayer` "testable to
   production." First checked the assumption the README had stated as fact
   ("Docker in this sandbox cannot bind-mount the project directory") rather
   than trusting it — turned out Docker actually works fine in this sandbox
   now (pulled and ran a real `redis:7-alpine` container to confirm before
   writing anything). Added `test/integration/RedisCoordination.test.ts`
   using Testcontainers (switched to it from a hand-rolled `docker run`
   script on the user's direction — the right call, since it handles port
   allocation, readiness waiting, and cleanup properly) plus `ioredis` as a
   devDependency only, so the production-facing `RedisLike` port stays
   dependency-free. Hit a real `nodenext`/`verbatimModuleSyntax` interop
   snag with ioredis's default export (not constructable under this repo's
   TS config) — solved by importing the named export `{ Redis }` instead,
   confirmed with an isolated repro file before touching the real test.

   **The second catch**: the "skip cleanly if Docker is unavailable" logic
   was wrong in its first form — a static `{ skip }` option object passed to
   `test(...)` is evaluated at *registration* time, before node:test's
   `before()` hook has run, so it always saw the initial (wrong)
   `dockerAvailable = true` and would crash on a null client instead of
   skipping. Caught this by deliberately breaking Docker access
   (`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/nonexistent.sock`) and watching
   it fail instead of skip — the same "verify the unhappy path actually
   does what it claims" instinct as the fencing bug, applied to test
   infrastructure this time rather than application logic. Fixed by moving
   the check inside each test body via `t.skip(reason)`, which runs after
   `before()` completes. All 4 integration tests pass against a real
   container (~1.9s total), skip cleanly (0 failures, all skipped) when
   Docker is unreachable, and leave no orphaned containers behind either way.

9. **The capstone: a real two-instance failover, watched happen between
   actual containers — and a third mistake, this time mine, caught by the
   user, not by testing.** Asked to review the docs and diagram the
   problem. While reviewing, decided to verify the README's own claim that
   "Docker cannot bind-mount the project directory here" rather than take
   it as still true — ran `docker compose up` for real. It failed with
   "mounts denied... not shared from the host." Read that as confirmation
   the old claim still held. **It didn't** — the user pointed out
   `$HOST_WORKSPACE_FOLDER` should be used "to overcome bind mount limits,"
   which prompted checking the actual environment: `/etc/environment`
   already had it correctly set to the real Mac host path
   (the host's own path, the path
   Docker Desktop's file-sharing actually recognizes) — and an earlier
   `export HOST_WORKSPACE_FOLDER=/workspace` (a container-internal path,
   set while testing something unrelated) had silently shadowed it. Removing
   the override, not working around it, made the entire real stack boot:
   three genuine Envoy replicas, real outlier detection, a real aggregator.
   Confirmed end to end with real `curl -X POST .../__fail` calls and
   watching `OPEN → HALF_OPEN → OPEN → HALF_OPEN → PROBE_SUCCEEDED → CLOSED`
   actually happen through the aggregator's `/metrics`, matching the
   documented demo script exactly, against real infrastructure for the
   first time this session.

   Along the way, the user also caught that `localhost:<port>` from this
   assistant's own shell wasn't reaching the compose containers at all
   (silent empty `curl -s` output was mistaken for "no data yet" rather
   than "wrong network entirely") — pointed at the devcontainer's own
   external `devcontainer` bridge network and said to join it. That was
   exactly right: this session's own execution container turned out to
   already be attached to that network (confirmed via `docker network
   inspect`), so attaching the compose services to it too (alongside their
   existing `default` network, unchanged) made every service directly
   curlable by name, no published-port workaround needed.

   The user then asked to "add redis too" — taken as license to close the
   README's own remaining gap rather than just drop in an idle container:
   moved `ioredis` from a devDependency to a real one, added `--ha=redis`
   to `main.ts`, and changed docker-compose.yml to run **two** real
   `aggregator` containers against one `redis` service (not one aggregator
   plus an unused Redis). Verified by doing the actual adversarial thing:
   injected a real failure, let the elected leader (`aggregator-2`) publish
   into `OPEN` at `sequence=7`, then `docker kill`ed its container outright
   — no graceful shutdown, no drain. The standby (`aggregator`) took over
   within `lease_ttl_ms`, rehydrated `payments-provider` as `OPEN` (the
   actual state, not a cold `CLOSED`) from its Redis checkpoint, and
   continued publishing from `sequence=7` through recovery to `CLOSED` at
   `sequence=13`. No reset, no gap, no duplicate, across a hard kill of a
   genuinely separate OS process mid-incident — the same property every
   earlier test in this session proved in increasingly smaller sandboxes,
   now proven at the top: a real deployment, not a test process.

   **Why this belongs at the center of the article, more than the two
   testing catches**: those were self-caught by a good habit (run the
   unhappy path, don't trust the code). This one was a mistake the
   assistant made, misread as confirming a stale premise, and the user
   corrected — a good complement to the other two, because it shows the
   discipline holds even when the fresh evidence points the wrong way and
   someone else has to say "check that again."

10. **Back to the RabbitMQ design — diagrammed, and this time actually
    verified against real infrastructure, in Effect.** The user returned to
    the RMQ scenario from step 4, asking for a diagram plus verification of
    the "missing parts." Wrote up the held design as `docs/rmq-control
    -plane.md` (architecture + state→action mapping + the HALF_OPEN
    election, as Mermaid). Then, mid-verification, the user named a specific
    library — `rabbitmq-amqp-js-client` — which turned out to speak AMQP 1.0
    against RabbitMQ 4.x natively, not the AMQP 0-9-1 an initial `amqplib`
    -based script had already used; that first script was scrapped
    ("remove the obsolete ones") in favor of redoing the verification with
    the library actually named, against a RabbitMQ 4.x container instead of
    the 3.13 one already in use. The user then asked for it "within effect,"
    so the verification became a small `Rmq` `Context.Service` mirroring
    this repo's own `FleetSource.ts`/`Coordination.ts` shape
    (`Layer.effect` + `Data.TaggedError` + `Effect.acquireRelease`), not
    bare `async`/`await`.

    Both of the design's load-bearing RabbitMQ claims held up against a real
    broker: `x-single-active-consumer` really does deliver to exactly one of
    several registered consumers and promote a different one automatically
    when that one closes, and closing a consumer really does stop delivery
    without touching the connection. But the same pass surfaced a genuine,
    previously unknown gap: this specific client's public `Consumer` API is
    only `start()`/`close()`/`id`/`replyTo` — no way to adjust credit/prefetch
    after creation, confirmed by reading its actual type surface rather than
    assumed from familiarity with older RabbitMQ clients that do support
    this. That breaks `DEGRADED` (live prefetch reduction) and the
    ramp-back-on-recovery step as designed; only `CLOSED`/`OPEN`/`HALF_OPEN`
    are buildable on this client today. A fourth concrete instance of the
    session's throughline — checking a specific, nameable claim against
    reality turned up something no amount of re-reading the design would
    have caught, this time in a library someone else wrote rather than in
    this session's own code.

## Threads still open (useful if the article continues past this point)

- The RabbitMQ control-plane design ([[rmq-control-plane-design]]) is fully
  worked out but not built.
- The Redis backing this session's `docker compose up` demo is one
  `redis:7-alpine` container with no persistence/replication configured —
  a second single point of failure, just moved down a level. The README
  says as much (see the updated "prototype, not production" bullet); a real
  deployment wants Redis with AOF/replication or a different backing store
  entirely behind the same `LeaderElection`/`CheckpointStore` interfaces.
- The "fork this defers" question (must the aggregator's OPEN be
  authoritative, requiring an xDS push path?) is still open and orthogonal
  to everything done this session.

## Later sessions (2026-09-12/13), the beats worth an article

The user asked for an article about this work and suggested publishing it as
incremental branches. The repository's commit bodies hold the detail; these are
the moments where a measurement overturned a reasonable belief, which is the
throughline:

- **A hash that didn't hash.** FNV-1a placed `daemon-0`..`daemon-5` in a
  straight line covering 2% of `[0,1)`, so a published fraction selected the
  wrong share of the fleet every time, not at random. SHA-256 fixed it (ADR 013).
- **Limits changed the numbers they were meant to hold still.** The same demo
  run with and without container limits: every garbage-collected process used
  28–52% less memory once it could see a ceiling, while Envoy and Redis didn't
  move. Every earlier published RSS figure described the laptop, not the
  process (ADR 014).
- **RabbitMQ doesn't read its own cgroup limit**, so flow control would never
  have kicked in before the OOM killer.
- **"Nothing in the control path notices" was false.** A hundred open consoles
  took up to a fifth of the breaker loop's cadence. The "unexplained" 1.2 GiB
  of memory turned out to be the load test itself, which grew the process it
  was reading (ADR 015).
- **The design said `SubscriptionRef`; its buffer is unbounded.** Reading the
  library's source caught it before a slow browser could leak memory.
- **Vendoring the library's source at `main` would have taught APIs two
  release candidates newer** than the code compiles against. It was pinned to
  the installed tag, and the first thing it caught was one of the repo's own
  skills teaching four Effect 3 APIs.

Related: [[egress-breaker-open-threads]].
