---
name: reliability-testing-preferences
description: "How the user wants reliability / chaos work on /workspace judged, run and reported — correctness bar, stop-fix-relaunch, subagents, broker-sourced metrics, plain explanations."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d23c6dbd-5c0e-471a-a8fc-5ed6afc8c7c7
  modified: 2026-09-14T10:16:58.537Z
---

Stated by the user during the 2026-09-13 chaos work:

- **Correctness = no message lost AND dead-letter queue empty at the end.** Breaker
  and recovery checks matter but are secondary. Prove loss per message, not from
  broker counters (management publish/ack totals drift with channel churn).
- **"If you find bugs, stop to assess, fix and relaunch."** Don't let a matrix run on
  over a known correctness bug or residue that poisons later scenarios.
- **Use lower-model subagents for the hard/long work** (runs, triage,
  implementation from a precise brief). Review their diffs before deploying —
  two needed real fixes (poison count reset, readiness deadlock). Subagents can hit
  session limits mid-edit; check for half-applied changes. Confirmed again
  2026-09-14 on 3 bugs handed to 2 parallel subagents (split by disjoint
  files): doing the root-cause investigation myself first — reading the
  actual source (amqplib internals, not just this repo) until I had a
  concrete, falsifiable hypothesis with file:line evidence — then handing
  each agent a brief built on that (hypothesis + exact reproduction steps
  against the live stack + what to verify), produced diffs that held up
  clean under review, unlike 09-13's briefs which were thinner. A subagent
  can also stop mid-turn without a final report ("waiting on a background
  check...") even though its task shows as completed — resume it by name and
  ask explicitly for the full report before trusting anything it said.
- **Every fault under traffic load spikes**, and exercise every breaker transition.
- **Queue-flow numbers come from RabbitMQ metrics, not application counters**;
  offload app metrics wherever the broker already knows the fact. App metrics stay
  only for what the broker cannot see (third-party outcomes, decisions, sequences).
- **The idempotency key is a consumer concern, not the producer's** — stamped on
  first call, carried by republish.
- **Dashboard panels grouped by component/context** (rows).
- When the user asks "what does it mean?", lead with a **plain-language
  explanation** (an analogy is fine); they asked twice when the first answer was
  code-level.
- "go" after a list of proposals = proceed; still surface blockers found on the way.

Related: [[chaos-reliability-work-2026-09-13]], [[rmq-control-plane-design]].
