---
name: working-style
description: "How the user runs work on /workspace — one commit per request, measure before claiming, the verification standard for this branch, the chaos bar, how to report."
metadata:
  node_type: memory
  type: feedback
  originSessionId: 5ae73c69-84bf-4950-b335-c824169d0fe2
  modified: 2026-09-28T07:33:03.509Z
---

- **One commit per request**, with a narrative body: the finding, the measurement, what was left alone, what
  was and was not verified. Titles are a sentence naming the change. Leave the user's own uncommitted edits out.
  Check `git branch --show-current` first: the user switches branches between requests.
- **Measure before claiming.** Run it against a test or the live stack,
  and say plainly what was not measured. Measure a
  component through its real caller. When the user doubts a claim, measure it on the wire and show the table.
- **Check before building.** Read the installed/vendored source before hand-building plumbing (heartbeats,
  retries, backoff); a misconfigured default often looks like a missing feature.
- **Verification standard** (article/01-base-scenario): `pnpm run check`, `pnpm run test:rmq`; source changes need
  `docker compose up -d --build`; then `pnpm run incident` (`MODE=hang`, `RATE`, `WINDOW_MS`).
  A VOID run (host suspended) is rerun alone.
- **Chaos bar:** nothing lost and the dead-letter queue empty at the end, proven per message (the fake third
  party's `<run>:<n>` audit), not from broker counters. Every fault under a traffic spike. Found a correctness
  bug mid-run: stop, fix, relaunch.
- **Subagents** only when asked: do the root-cause reading first and hand them a falsifiable hypothesis with
  repro steps; review their diffs.
- Dependency upgrades respect pnpm's `minimumReleaseAge`; never keep a `minimumReleaseAgeExclude` entry.
- Prefer less public surface: export only what another module reads, but an SDK entry point's public types stay.
- "What does it mean?" gets a plain-language answer first. "go"/"yes" after proposals means proceed.

Related: [[code-style]], [[project-state]], [[devcontainer]].
