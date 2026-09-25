---
name: egress-breaker-open-threads
description: "Work on /workspace that was proposed, offered or deferred but not done, as of 2026-09-13 — check before starting anything new."
metadata: 
  node_type: memory
  type: project
  originSessionId: d23c6dbd-5c0e-471a-a8fc-5ed6afc8c7c7
  modified: 2026-09-25T14:32:33.431Z
---

As of 2026-09-13. Verify against `git log` and the ADRs before acting.

- **ADR 015, steps 3–5 (console at 1,000 APIs).** Steps 1–2 are built:
  `/api/events/stream` and the shared frame in `ConsoleFrames.ts`. Still
  proposed: an attention view (counts plus the 50 worst APIs), snapshot/patch
  with `Last-Event-ID` resume, and gzip last. Browsers still receive
  2.77 MB/s each. The ADR has measured targets; the browser rendering cost has
  never been measured (no browser in this devcontainer).
- **Three Effect skills were deleted on 2026-09-13** at the user's request,
  because they clashed with `agent-patterns/`, which wins:
  - `domain-modeling` taught four Effect 3 APIs.
  - `effect-testing` assumed `@effect/vitest` rather than this repo's
    `node:test` + `TestClock`.
  - `effect-ts` (lockfile-managed) installs a floating `effect@rc` and points at
    `node_modules/effect`, which doesn't exist at the root under pnpm.

  Don't reinstall or recreate them. Any replacement must agree with
  `agent-patterns/` and `repos/effect`.
- **Offered, not taken up:**
  - `review/NN-*` branches for publishing the work incrementally, from the
    article request (recommended: annotated branches at pass boundaries, no
    history rewrite).
  - buildx/GHA layer caching for the Pages workflow.
  - `infra/instrument.mjs`'s throttle budget is a percentage of runnable
    periods, so a nearly idle container fails on one throttle (alertmanager,
    1 of 57). It needs a minimum-periods floor.
- **Dependencies, as of 2026-09-13.** Everything is at latest: npm packages,
  container images including Prometheus 3.14, GitHub Actions, and the docs
  Gemfile on Ruby 4.0. Node is 26 everywhere (images, CI, `engines` >=26,
  `@types/node` 26) since 2026-09-13, and the `--experimental-strip-types` flag
  is gone. Node 26 is Current until LTS on 2026-10-28. ADR 008's quoted transcript and
  `history/` still show it, as records. `history/runs/baseline-demo-2026-09-12.json` was
  recorded before all of this and has not been re-recorded.
- **Left from the RabbitMQ 4.3 upgrade:** amqplib crashes the daemon (unhandled `error` on the
  inner connection) when the broker closes the connection during topology
  replay — recorded in ADR 005, not fixed.
- **Found 2026-09-14, fixed 2026-09-14 (same day, later session):** a related
  but distinct gap in the same family — `packages/rmq-consumer/src/main.ts`
  ran `runDaemon` as a forked, unobserved fiber
  (`Layer.effectDiscard(Effect.forkScoped(Effect.orDie(...)))`), so a startup
  failure there (e.g. the same queue-arg mismatch that used to make a daemon
  merely hang for ~5 minutes, now fixed) was a defect that never ended
  `Layer.launch` — the daemon hung *forever*, looking healthy. Fixed by
  extending `launchWithRmq` and applying the same pattern to
  `rmq-producer/src/main.ts`, which had the identical shape. Live-verified
  against the running stack (daemon binary run standalone against a
  hand-mismatched queue, exits FATAL in ~0.4s) — see
  [[chaos-reliability-work-2026-09-13]] for the one bug this fix went through
  first (dropped `orDie` meant `catchDefect` couldn't have caught the real
  failure) and how it was caught before landing.
- **Grafana `le!="Infinity"` filter** can be removed once Prometheus 2-scraped
  data is past retention (after 2026-09-28).
- **Unexplained:** one `test:rmq` failure on the first of three runs
  (2026-09-13), not captured. The next two runs passed 16/16.
- **Local disk:** about 120 MiB of unreferenced upstream Effect history sits in
  `.git` from `git subtree add`. `git gc` prunes it eventually; never pushed.

- **The 2026-09-13 load/chaos/reliability work is now committed** — see
  [[chaos-reliability-work-2026-09-13]] for what it covered, and
  [[chaos-reliability-work-2026-09-17]] for the 2026-09-17/18 follow-up (first
  live chaos-load run against the real stack, a consumer-rebuild bug, and the
  full ADR 017 split-brain arc — all resolved and committed).
- **Branch 04, as of 2026-09-25:** the 11-finding Effect review is fixed
  (one commit each, up to 8061161dbf), plus a redrive bug the review
  surfaced (1bce156431: capped passes closed their channel before the
  confirms arrived and replayed work several times). 02/03 redrive with
  `get`, so they don't have it. Http.ts `record` made lease-aware too
  (c0b27027a2). `sendBatch` and the Trace.ts tests were backported to
  01, 02-in-process, 02-rabbitmq-only and 03 (local commits, not pushed).
  Still open: the user's
  "native prometheus rabbitmq monitor" request is waiting on which
  difference from 03 they meant.

Related: [[rmq-control-plane-design]], [[egress-breaker-session-record]], [[chaos-reliability-work-2026-09-13]], [[chaos-reliability-work-2026-09-17]].
