---
name: chaos-test-new-components
description: "Standing rule: every new component introduced in the circuit-breaker article series must be exercised under chaos testing (fault injection + sustained load), not just the single scripted incident.mjs run, before considering that article's work verified."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5ee1dde4-09c2-4ea3-8453-6ae6c140ccbc
  modified: 2026-09-19T09:13:39.079Z
---

Whenever a new component is introduced (the cockatiel breaker, the
probe-permit queue, the aggregator, the SAC redriver, the release-vs-requeue
fix, and anything after), it must be validated under chaos testing —
fault injection combined with sustained traffic load — to assess overall
reliability, not just driven through the single scripted outage
`infra/incident.mjs` runs.

**Why:** stated explicitly by the user 2026-09-19, generalizing from how
rigorously the pre-pruning master system was chaos-tested (see
[[chaos-reliability-work-2026-09-13]], [[chaos-reliability-work-2026-09-17]]):
process kills, network partitions and flaky-service faults injected under
load, judged first on per-message correctness (no message lost, dead-letter
queue empty at the end — see [[reliability-testing-preferences]]) and only
then on whether breaker/component behavior matched expectations. A single
clean incident script run proves the happy-path story a report tells; it
does not prove the component survives a replica dying mid-probe, the
aggregator restarting mid-incident, or a network partition arriving while
the breaker is already half-open.

**How to apply:** `article/01`'s base branch pruned out the full chaos
harness (`infra/chaos-load.mjs`, on `master`) along with `packages/domain`
and the rest of the original system — only the load generator
(`infra/chaos-publisher.mjs`) survived, and it already speaks this repo's
current wire format (`x-idempotency-key`, `{apiId, n}`). Before or shortly
after each article's own incident-report step, build or extend a
right-sized chaos driver for what actually exists on that branch (not a
wholesale port of master's harness, which assumes packages/domain's
DEGRADED-state reducer and a multi-aggregator topology this series doesn't
have), inject faults relevant to the component just added, and report
findings plainly alongside the incident report — stop and fix before
continuing if a fault surfaces a real correctness bug, per
[[reliability-testing-preferences]]'s stop-fix-relaunch rule.

**First run, 2026-09-19** (`infra/chaos-load.mjs`, built this session): four
faults (`kill-one-consumer`, `kill-all-consumers`, `kill-aggregator`,
`flaky-storm`) against the fleet as it stood after article 6, at both a
light (100/s→500/s) and heavy (500/s→3000/s) load profile. All four passed
the no-loss bar every time (zero unaccounted messages across ~350k
confirmed total). Two real findings, neither a bug:

- **A mixed-mode outage (`flaky-storm`'s error→hang→reset cycle) can leave
  a breaker open for well over 90 seconds after the upstream is fully
  healthy again** — cycling through three failure modes back-to-back lets a
  replica's `ExponentialBackoff` climb close to its 30s ceiling before the
  storm even ends, so total settle time is bounded by that plus the storm's
  own duration, not by a single fault's usual few-second recovery. Confirmed
  it does eventually close (checked breaker state directly after the
  harness's own 90s timeout elapsed) — genuinely just slower, not stuck.
- **The redriver (article 5) never fired in any of these runs — `work.dead`
  stayed at exactly 0 every single time.** Traced why rather than assuming
  it's untested: article 6's fix means a local breaker-open/permit-lost
  rejection no longer counts toward `x-delivery-limit`, so only a message
  unlucky enough to be one of the handful in flight in the narrow window
  *before* a replica's breaker trips — and then redelivered and fail for
  real two more times before that closes — can still dead-letter. Chaos
  faults built for pre-article-6 behavior don't reliably produce that
  anymore. Real dead-lettering has gotten genuinely rare, which is exactly
  what article 6 set out to do — but it means exercising the redriver under
  chaos now needs a fault purpose-built to force it (not yet built).

Also found and fixed two harness bugs before trusting any result:
`chaos-publisher.mjs`'s own idempotency-key scheme (`RUN_ID:n`) is never
what reaches `flaky-upstream`'s audit — `consumer.ts` mints its own key as
`apiId:n` on the HTTP call, ignoring whatever the AMQP message carried — so
the audit lookup and its bucket must be keyed (and cleared) by `apiId`, not
by a per-run id. And watching a queue drain to exactly zero while the
publisher keeps producing at a rate near the fleet's own throughput ceiling
never terminates — cut inflow to zero before measuring settle time, don't
just drop back to the pre-fault baseline rate.

Related: [[reliability-testing-preferences]], [[check-before-building]],
[[egress-breaker-open-threads]], [[devcontainer-environment-gotchas]].
