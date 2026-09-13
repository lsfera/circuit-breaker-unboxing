---
name: egress-breaker-open-threads
description: Work on /workspace that was proposed, offered or deferred but not done, as of 2026-09-13 — check before starting anything new.
metadata:
  type: project
---

As of 2026-09-13. Verify against `git log` and the ADRs before acting.

- **ADR 015, steps 3–5 (console at 1,000 APIs).** Steps 1–2 are built:
  `/api/events/stream` and the shared frame in `ConsoleFrames.ts`. Still
  proposed: an attention view (counts plus the 50 worst APIs), snapshot/patch
  with `Last-Event-ID` resume, and gzip last. Browsers still receive
  2.77 MB/s each. The ADR has measured targets; the browser rendering cost has
  never been measured (no browser in this devcontainer).
- **`.claude/skills/domain-modeling` taught Effect 3 APIs** (`Schema.annotations`,
  `Schema.Data`, `Schema.DateTimeUtcFromSelf`, `DateTime.unsafeNow`). After that
  was reported, the file was deleted from the working tree on 2026-09-13, but
  the deletion is not committed. `AGENTS.md` and
  `agent-patterns/effect-3-to-4.md` still name it as the example of stale
  material; update them once the deletion is committed.
  `.claude/skills/effect-testing` assumes `@effect/vitest`, which the repo does
  not use.
- **Offered, not taken up:**
  - `review/NN-*` branches for publishing the work incrementally, from the
    article request (recommended: annotated branches at pass boundaries, no
    history rewrite).
  - buildx/GHA layer caching for the Pages workflow.
  - `infra/instrument.mjs`'s throttle budget is a percentage of runnable
    periods, so a nearly idle container fails on one throttle (alertmanager,
    1 of 57). It needs a minimum-periods floor.
- **Dependencies not updated on 2026-09-13.** The npm packages are at latest
  (Effect rc.115, jsdom 30). Container images pinned by digest in
  docker-compose.yml, the GitHub Actions and the docs Gemfile were not touched.
  Several would be major bumps, Prometheus 2 → 3 among them. `@types/node`
  stays on 22.x on purpose, because Node 22 is the runtime floor.
  `history/runs/baseline-demo-2026-09-12.json` was recorded on rc.113 and has
  not been re-recorded.
- **Unexplained:** one `test:rmq` failure on the first of three runs
  (2026-09-13), not captured. The next two runs passed 16/16.
- **Local disk:** about 120 MiB of unreferenced upstream Effect history sits in
  `.git` from `git subtree add`. `git gc` prunes it eventually; never pushed.

Related: [[rmq-control-plane-design]], [[egress-breaker-session-record]].
