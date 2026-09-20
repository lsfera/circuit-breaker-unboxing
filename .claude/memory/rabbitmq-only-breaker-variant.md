---
name: rabbitmq-only-breaker-variant
description: Branch article/02-rabbitmq-only-breaker — the in-process breaker rebuilt so its open state lives in RabbitMQ (delay chain), what was measured, what's uncommitted.
metadata:
  type: project
---

Built 2026-09-20 on branch `article/02-rabbitmq-only-breaker` (off `article/02-in-process-breaker`), **committed 2026-09-20 with the article (docs/rabbitmq-held-breaker.md), chaos harness (infra/chaos-breaker.mjs) and Grafana capture (infra/capture-incident.mjs). Idea came from Particular's NServiceBus RabbitMQ delayed delivery; the user wants holds of up to ~24h.

Design: `packages/rmq/src/DelayedDelivery.ts` (17 quorum levels, TTL 2^n s, e2e bindings, max 131071s), `Client.ts` gained `bindExchange` + `drainConsumer`, `packages/consumer/src/Breaker.ts` is a pure-ish supervise() machine (closed = consumer, open = no consumer + wake token, half-open = prefetch-1 probe), cockatiel removed. Failed probe settles `release`.

**Measured** (live, 20s outage): dead-lettered 0 vs 2,745 for cockatiel; recovery 9s vs 23s (single runs). Known cost: hold of h seconds = up to h late recovery; a lost token leaves a replica open until restart (no rescue timer, deliberately skipped).

**Why:** the user's series compares breaker designs per branch. **How to apply:** reach the stack from the devcontainer with `BROKER=amqp://guest:guest@rabbitmq:5672 FLAKY_UPSTREAM=http://flaky-upstream:8080 PROMETHEUS=http://prometheus:9090`; rebuild with `docker compose build rmq-consumer`. See [[rmq-control-plane-design]].

**Chaos found a real bug (2026-09-20):** first run parked 2–3 healthy messages per outage in the trip window (5 failures to trip + a round trip to cancel; requeue spends the 3-attempt budget). Fixed: a failure that follows another on the same replica, and any failed probe, is `release`d (decide(outcome, role, streak)); after the fix 0 dead letters in 4 outage scenarios (~45k msgs each). `partial` (60% failure) still dead-letters ~11 and flaps (83 openings) — the case for a failure-rate breaker. Screen capture needs playwright-core installed outside the repo (chromium is cached in ~/.cache/ms-playwright); Grafana needed `docker compose restart grafana` to pick up the dashboard JSON change. history/runs/*.json is gitignored.
