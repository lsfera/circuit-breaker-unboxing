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
- **Two project skills were deleted on 2026-09-13** at the user's request:
  `domain-modeling`, which taught four Effect 3 APIs, and `effect-testing`,
  which assumed `@effect/vitest` rather than this repo's `node:test` +
  `TestClock`. Don't recreate them from memory; any replacement should be
  written against `repos/effect`.
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
  Gemfile on Ruby 4.0. `@types/node` and the `node:22-alpine` image stay on 22
  on purpose (runtime floor). `history/runs/baseline-demo-2026-09-12.json` was
  recorded before all of this and has not been re-recorded.
- **Left from the RabbitMQ 4.3 upgrade:** the `rabbitmq-data` volume still holds
  node directories for five container ids (`rabbit@<id>*`) from before
  `hostname: rabbitmq` was pinned. They are unreachable garbage, not deleted
  without asking. Also: amqplib crashes the daemon (unhandled `error` on the
  inner connection) when the broker closes the connection during topology
  replay — recorded in ADR 005, not fixed.
- **Grafana `le!="Infinity"` filter** can be removed once Prometheus 2-scraped
  data is past retention (after 2026-09-28).
- **Unexplained:** one `test:rmq` failure on the first of three runs
  (2026-09-13), not captured. The next two runs passed 16/16.
- **Local disk:** about 120 MiB of unreferenced upstream Effect history sits in
  `.git` from `git subtree add`. `git gc` prunes it eventually; never pushed.

Related: [[rmq-control-plane-design]], [[egress-breaker-session-record]].
