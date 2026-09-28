---
name: working-style
description: How the user runs work on /workspace — one commit per request, measure before claiming, the verification standard, the chaos bar, how to report.
metadata:
  type: feedback
---

- **One commit per request**, with a narrative body: the finding, the measurement, what was left alone, what
  was and was not verified. Titles are a sentence naming the change. Leave the user's own uncommitted edits out.
- **Measure before claiming.** Run it against the live stack or a test, and say plainly what was not measured.
  Measure a component through its real caller: a unit test of the permit alone once "proved" a permit that
  never gated anything.
- **Check before building.** Read the installed/vendored source before hand-building plumbing (heartbeats,
  retries, backoff); a misconfigured default often looks like a missing feature. Verify broker behaviour on the
  wire, not from an argument's name (`reject-publish` nacks the loser loudly).
- **Verification standard** (article/04): `pnpm run check`, `pnpm run test:rmq`, rebuild
  (`docker compose build rmq-consumer && docker compose up -d`), then `node infra/chaos-app.mjs`
  (`--format=mixed` for JSON + protobuf). A VOID scenario (host suspended) is rerun alone. Save the run to
  `docs/runs/` and cite it in the commit.
- **Chaos bar:** nothing lost and dead-letter/parked queues back where they started, proven per message, not from
  broker counters. Every new component gets fault injection under load, not just `pnpm run incident`.
  Found a correctness bug mid-run: stop, fix, relaunch.
- **Subagents:** do the root-cause reading first and hand them a falsifiable hypothesis with repro steps; review
  their diffs; a "completed" subagent may not have finished its report.
- Dependency upgrades respect pnpm's `minimumReleaseAge`; never keep a `minimumReleaseAgeExclude` entry.
- "What does it mean?" gets a plain-language answer first. "go" after proposals means proceed.

Related: [[code-style]], [[project-state]], [[devcontainer]].
