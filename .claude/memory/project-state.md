---
name: project-state
description: "The article series on /workspace — branches, what article/02-in-process-breaker holds, the user's settled positions, and open items as of 2026-09-28."
metadata:
  node_type: memory
  type: project
  originSessionId: 5ae73c69-84bf-4950-b335-c824169d0fe2
  modified: 2026-09-28T07:33:13.878Z
---

**Branches** (no remote yet; deleted branches live only in the reflog): 01-base-scenario, 02-in-process-breaker,
03-rabbitmq-coordination, 04-rabbitmq-only-breaker (breaker held by RabbitMQ; consumer SDK),
05-platform-control-plane (old master: Envoy + two aggregators with a Redis lease, published per-API events;
"scaling at platform level", with entry criteria for when it pays). `main` holds only `.gitignore` and
`renovate.json`, which updates every `article/*` branch (effect/@effect/* excluded: they move with the
subtree). This branch is a cockatiel breaker in each consumer replica, nothing shared.
The branches share no ancestry but the repos/effect subtree: changes move by cherry-pick along 01→03;
04 and 05 are structurally different. `.claude/memory` is tracked, so each branch has its own copy.

**Settled by the user:** fewer, denser articles; a bounded queue is an anti-pattern (per-message deadlines and
parking, never `x-max-length` for load); the failure-rate breaker was dropped; 05 costs about three times the
code of 03 and only pays at platform level. Terminal quorum queues keep `x-delivery-limit: -1` (see
[[rabbitmq-facts]]).

**Open:**
- This branch is on Effect rc.116 (04/05 are on rc.117); rc.118 and amqplib 2.1.0 were held back on 2026-09-28
  by pnpm's release-age cooldown. Upgrade with a subtree pull per [[devcontainer]].
- Three Effect skills were deleted on 2026-09-13 for teaching Effect 3; don't recreate them.
- ~120 MiB of unreferenced Effect history in `.git` from subtree pulls; `git gc` prunes it.

Article source material the user asked to keep: [[egress-breaker-session-record]].
