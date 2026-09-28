---
name: project-state
description: The article series on /workspace — branches, what article/04 holds, the user's settled positions, and open items as of 2026-09-28.
metadata:
  type: project
---

**Branches** (no remote yet; deleted branches live only in the reflog):
01-base-scenario, 02-in-process-breaker, 03-rabbitmq-coordination, 04-rabbitmq-only-breaker (the breaker
held by RabbitMQ as a delay-chain token; the consumer SDK, a PostgreSQL ledger app, JSON + protobuf),
05-platform-control-plane (old master: Envoy + aggregators, "scaling at platform level"). `main` holds only `.gitignore` and `renovate.json`, which updates every
`article/*` branch (effect/@effect/* excluded: they move with the subtree). They share no
ancestry but the repos/effect subtree, so changes move by cherry-pick along 01→03; 05 is structurally different.
`.claude/memory` is tracked, so this note exists only where committed.

**Settled by the user:** fewer, denser articles; a bounded queue is an anti-pattern (use per-message deadlines
and parking, never `x-max-length` for load); the failure-rate breaker was dropped. SDK decisions: `For/bind/run`,
explicit negotiation with no default media type, no HTTP in the SDK, one breaker per dependency tuned in code,
no idempotency key in the SDK.

**Open:**
- Effect rc.118 and amqplib 2.1.0 were held back on 2026-09-28 by pnpm's release-age cooldown; upgrade once
  they are a day old (subtree pull per [[devcontainer]]).
- Three Effect skills were deleted on 2026-09-13 for teaching Effect 3; don't recreate them.
- ~120 MiB of unreferenced Effect history in `.git` from subtree pulls; `git gc` prunes it.

Article source material the user asked to keep: [[egress-breaker-session-record]].
