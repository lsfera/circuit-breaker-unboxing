---
name: chaos-reliability-work-2026-09-13
description: "State of the load-test / chaos-under-spikes / reliability-fix work started 2026-09-13 on /workspace — what is built, deployed, uncommitted, and still open, updated 2026-09-14."
metadata: 
  node_type: memory
  type: project
  originSessionId: d23c6dbd-5c0e-471a-a8fc-5ed6afc8c7c7
  modified: 2026-09-14T10:57:07.732Z
---

Started 2026-09-13, continued 2026-09-14. **Still nothing from this work is
committed** — a large uncommitted changeset spans packages/, infra/, docs/.
Verify with `git status` before acting; the user's convention is one commit
per request with a narrative body ([[rmq-control-plane-design]]), so this
needs splitting/writing up whenever it does get committed.

**Built (infra):** `infra/load-sweep.mjs` (rate sweep; `RATE_PER_SECOND`,
`ENVOY_CPUS`, `PRODUCER_CPUS` compose overrides), `infra/chaos-load.mjs` (34 faults:
process, network via netshoot tc/iptables in target netns, flaky-service journeys
covering all 8 breaker edges; always under traffic spikes; per-message oracle),
`infra/chaos-publisher.mjs` (forked publisher, confirmed-bitmap),
`infra/flaky-upstream.mjs` (modes error/hang/reset/delay + `/__audit`).
Dashboard regrouped into component rows + RabbitMQ panels; `WorkQueueStalled`
alert + runbook; Prometheus scrapes queue_metrics/delivery/exchange families.

**Measured limits:** default envelope ceiling ~13k msg/s (Envoy 0.5 CPU); with
Envoy at 2 CPU held 19.2k, broke at 25.6k on broker CPU (flow control cut
publishing). Adaptive concurrency sheds from ~800/s (min_rtt reads 0).

**Fixes deployed and verified (low profile: 34/34 CORRECT, no loss, DLQ empty):**
Envoy sheds 429 + daemon `release` (uncounted nack); consumer-stamped
idempotency key, retry by republish with `x-egress-attempts`; floor sweeps DLQ
every 30s while CLOSED + park after 5 redrives (`<api>.work.parked`); DLQ/parked
`x-delivery-limit: -1` (the real loss cause); work queue at-least-once DLX; probe
releases untaken deliveries; health-check timeout 2s; app queue-flow metrics
removed in favour of RabbitMQ; leader steps down when control plane not ready
(+ streak expiry, publish-confirm timeout).

**No longer half-finished, confirmed 2026-09-14:** the two items that were
half-finished when two Sonnet subagents hit the session limit on 2026-09-13 —
Aggregator/Client's `demoteAndFence`/`resetConnection` plus per-attempt
readiness counting, and `Breaker.ts`'s DEGRADED close-hold hysteresis — turned
out to already be fully wired through when this was checked 2026-09-14; only
the typecheck/test verification was missing. `pnpm run check` now passes
clean (137/137 unit tests). One deadlock *was* found and fixed along the way,
in a subagent-written test (`AmqpControlPlaneSink.test.ts`): it called
`sink.deliver()` directly under `TestClock` instead of forking it first, so
nothing ever advanced the virtual clock the delivery fiber was blocked on —
see `TestClock`'s own docstring ("fork the effect being tested, then adjust")
and `agent-patterns/effect-pubsub-and-streams.md`'s TestClock section, both of
which state this pattern and neither of which the subagent had read.

**Fixed 2026-09-14** (all three "open/to decide" reliability bugs below,
independently verified live against the running stack plus `pnpm run check`
— see the diffs for how, once committed, rather than trusting this summary):
- Consumers stuck after broker closes their channels, seen after host
  suspend: real bug, but only for a one-sided *network* partition, not a
  process freeze (`docker pause` recovered in ~260ms — no bug there).
  `amqp.connect()` had no socket `timeout`, so a partition fell back to
  Linux's own ~135s TCP SYN-retry per attempt instead of amqplib's backoff,
  turning the documented "~5 minute" recovery budget into 2+ hours. Fixed in
  `packages/rmq/src/Client.ts`.
- Daemons hang silently on queue-arg mismatch (no crash): confirmed live (21
  reconnect attempts, 84s, zero log output). `Client.ts` never listened for
  amqplib's `connect-failed` event. Fixed: logs every failed attempt, and
  abandons the retry budget immediately (instead of the full ~5 minutes) when
  the error carries a numeric AMQP reply code — a deterministic broker
  rejection that retrying cannot fix.
- Aggregators drop API state after telemetry blackout+outage: confirmed live
  — a leadership change *during* a total telemetry blackout left the new
  leader's registry empty for every API it leads, not just the one the
  blackout/outage coincided with. Rehydration was keyed off that tick's poll
  reports instead of the full configured API list. Fixed in
  `packages/aggregator/src/Aggregator.ts`, with a new regression test.

**Fixed and live-verified 2026-09-14 (this session):** the "hangs forever" gap
found 2026-09-14 — `packages/rmq-consumer/src/main.ts` ran `runDaemon` inside
`Layer.effectDiscard(Effect.forkScoped(Effect.orDie(...)))`, a defect in a
forked, unobserved fiber that per [[egress-breaker-open-threads]]'s ADR 005
note does not end `Layer.launch`. Fixed by extending `launchWithRmq` in
`packages/rmq/src/Client.ts` to take an optional second effect to race
against `Rmq.lost` (`Effect.raceFirst`, defaults to `Effect.never`), and
having each `main.ts` (`rmq-consumer` and, same shape, `rmq-producer`) catch
its forked loop's defect, fail a local `Fatal` deferred from the handler, and
pass `Deferred.await` of it as that second effect — mirroring the
aggregator's `Fatal`-deferred pattern, which has no `Rmq` to race against and
so hand-rolls the whole thing itself.

First draft of the fix had a real bug: it dropped the `Effect.orDie` around
`runDaemon`/`runProducer` and caught only `Effect.catchDefect`, but their
failures (e.g. `declareQueue`'s `PRECONDITION_FAILED`) are the typed
`RmqError`, not a defect — `catchDefect` would never have fired for the exact
scenario this exists to fix. Caught before landing by checking
`runDaemon`/`runProducer`'s actual error channel in `Client.ts` rather than
trusting the pattern by analogy; restored `Effect.orDie(...).pipe(Effect.catchDefect(...))`.

Live-verified against the running stack (not just `pnpm run check`, which
also passes clean at 137/137): declared `verify-fix.work` by hand via the
management API with a mismatched `x-dead-letter-exchange`, ran the daemon
binary directly against it (`node packages/rmq-consumer/src/main.ts --rmq
rabbitmq:5672 --api-id verify-fix` — devcontainer shares the `rabbitmq`
hostname with the compose network), and confirmed it now logs `FATAL: daemon
died` and exits with code 1 in ~0.4s, where before it would hang forever.
Scratch queue deleted afterward; the running demo stack (aggregators,
daemons, producer) was left untouched — this ran as a one-off process, not a
rebuild of `egress-breaker:dev`.

**`payments-provider.work.parked`'s ~130,000 messages, investigated
2026-09-14:** genuinely poison, not a bug artifact. Sampled 2000 of the
129,805 via the management API's non-destructive `get` (`ack_requeue_true`):
every message has a unique `n` and idempotency key (no duplication/republish
loop — ruled out the class of bug `Redrive.ts`'s `ORIGIN_PASS_PROPERTY`
comment describes, 17,703 republishes of two messages), and each carries only
`traceparent` + `x-idempotency-key`, no `REDRIVE_COUNT_HEADER` — exactly the
shape `Redrive.ts:196-206` produces for a message that hit `MAX_REDRIVES`
(one WORK_DELIVERY_LIMIT=3 exhaustion, then don't-carry-forward on the
"parked" branch). The sampled `n` values span ~2,434 across 2,000 messages,
so extrapolated the full backlog spans on the order of 150k `n` values — a
sustained failure window, not a small poison set replayed many times.

It predates the currently-running producer: that container restarted at
2026-09-14T10:18:41Z (`docker inspect ...StartedAt`) and its own log counter
was only at ~348,000 sent by 10:47, climbing at the default 200/s — nowhere
near the parked messages' `n` range (~1,225,180–1,227,614+), so those were
produced by an earlier producer process before a restart reset the counter.
None of the recorded `history/runs/chaos-load-low-*.json` show
`parkedFromThisRun` above 0, and `load-sweep-*.json` doesn't track parking at
all — so the incident that created this isn't in any file `history/runs/`
kept, most likely an earlier ad-hoc high-rate/near-ceiling exploration.
Quorum queue args are immutable, so this is not able to have carried over
from before the `x-delivery-limit: -1` fix — the live queue already shows
`-1`, meaning it was created (or last recreated) after that fix shipped.

Not a bug, not actively growing, not urgent — the queue is exactly what
"parked" is for (a human decides what to do with poison messages) and it
costs ~23 MB. Left alone; nobody has decided whether to drain, archive, or
inspect these 129,805 payloads by hand.

**Docs pass, done 2026-09-14 — narrower than the original note claimed.**
Checked ADR 016, `docs/rmq-control-plane.md`, `docs/approaches.md`,
`docs/operations.md` and the runbooks against the current code: all already
correctly describe the republish/retry-budget/parking design — ADR 016 does
**not** blame at-most-once DLX (it explicitly rules that theory out and
attributes the loss to the DLQ's own `x-delivery-limit`), so that part of the
original note was itself stale, describing an earlier draft that had already
been fixed by the time this was checked. The actual gap was
[ADR 005](../../docs/decisions/005-connection-recovery.md): three fixes from
today (socket timeout, `connect-failed`/reply-code handling, and this
session's `launchWithRmq`/`Fatal` fix — all above) were in code comments but
never folded back into the ADR. Added a dated addendum there
("A silent, deterministic reconnect, and a fiber `launchWithRmq` still
couldn't see") covering all three, in the ADR's own style. Nothing else
needed changing.

**Still open / to decide, unchanged since 2026-09-13:** shedding-as-overflow
blocked (Envoy `rq_blocked` is listener-wide, needs a listener per API); high
& near-ceiling chaos profiles not run; webhook availability trade-off of
gating leadership on AMQP readiness was flagged, not decided.
