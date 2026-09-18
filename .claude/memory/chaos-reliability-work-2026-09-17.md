---
name: chaos-reliability-work-2026-09-17
description: "First live chaos-load run against the real stack: a consumer-rebuild bug and a full split-brain investigation (ADR 017), resolved end to end and committed."
metadata:
  node_type: memory
  type: project
  modified: 2026-09-18T00:00:00.000Z
---

2026-09-17/18. First-ever live run of `infra/chaos-load.mjs --profiles=high`
against the real stack found real bugs, worked one by one, each verified live
(not just in unit tests) before moving on — see git log for exact commits.

- **Consumer-rebuild explosion** in `packages/rmq/src/Client.ts`: rebuilding a
  consumer didn't close the channel it was replacing, and `consume()` raced
  `live.add(entry)` against `attach()`. Fixed; verified live.
- **ADR 017, full arc** (`docs/decisions/017-a-heartbeat-off-the-delivery-channel.md`):
  `net-control-partition+outage` showed the demoted leader's
  `egress_aggregator_is_leader` gauge staying at 1 for seconds after the
  standby had already taken over — two leaders reporting at once.
  1. `CONSECUTIVE_FAILURE_THRESHOLD`'s own comment did math that didn't match
     `Coordination.ts`'s lease TTL (claimed 6.3s inside a 5s TTL). Fixed
     (3→2).
  2. Added a heartbeat (liveness probe independent of real delivery) to
     detect a dead connection without waiting for a real event. Its first
     version shared the default confirm channel with real deliveries and
     produced a genuine duplicate control-plane delivery under load —
     `Client.ts` grew a general `channelKey` capability so a heartbeat (or
     any future caller) gets its own isolated confirm channel.
  3. Split-brain window was still 3-4s even with both fixes. Diagnosed by
     instrumenting the heartbeat with per-attempt timestamps: it fired
     exactly on schedule (ruling out the "Effect fiber scheduler starved
     under load" hypothesis floated at the time) — the real bug was that a
     real event's own `DELIVERY_RETRY` chain, once started, never consulted
     the heartbeat's concurrently-updated `consecutiveFailures` state, so it
     ran to its own ~8.7s worst case regardless of what the heartbeat had
     already established. Fixed by making the real-delivery path check that
     shared state on every attempt (not just at tick start) and fail fast
     once already known bad.
  4. Live re-verified: `never two leaders at once — 0s` (was unbounded, then
     4s, then 3-4s across the fixes above), `0 gaps, 0 duplicates` on
     `circuit.control` held throughout. `pnpm run check`: 147/147.

Pattern worth repeating: at every stage a new run surfaced a *new* problem
(the duplicate; then "heartbeat isn't the bottleneck"; then the timing
contradiction) — each was reported plainly, with the live measurement, before
touching code again, rather than patching silently. See
[[reliability-testing-preferences]] for the standing stop-fix-relaunch
expectation this followed.

Related: [[egress-breaker-open-threads]], [[reliability-testing-preferences]],
[[devcontainer-environment-gotchas]].
