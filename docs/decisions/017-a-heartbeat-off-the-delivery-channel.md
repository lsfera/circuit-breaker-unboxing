# 017 — A heartbeat off the delivery channel

**Status**: decided — `AmqpControlPlaneSink` heartbeats the broker on its own
channel and its own clock; `CONSECUTIVE_FAILURE_THRESHOLD` corrected to match
`Coordination.ts`'s lease TTL. A separate, deeper gap in the same fault is
identified and left open — see Consequences.
**Date**: 2026-09-17.
**Context**: the first live run of `infra/chaos-load.mjs --profiles=high`
against the real stack (never run before today) found `packages/rmq/src/Client.ts`'s
consumer-rebuild bug — fixed separately, see `git log` — and, once that was
fixed, a second, unrelated finding on `net-control-partition+outage`: the
demoted leader's own `egress_aggregator_is_leader` gauge stayed at 1 for
several seconds after the standby had already, correctly, taken over.

## What was measured

**The watchdog's own math was wrong.** `AmqpControlPlaneSink.ts`'s
`CONSECUTIVE_FAILURE_THRESHOLD` was 3, reasoned in its own comment to land at
"≈6.3s of consecutive confirm timeouts... inside the lease TTL a standby
would otherwise wait out." `Coordination.ts`'s lease TTL is 5000ms. 6.3s is
not inside 5s. Live: `net-control-partition+outage` first showed 4s of
`egress_aggregator_is_leader=1` on both instances at once (confirmed against
Prometheus's own scrape of both, not just the chaos harness's poll).
Lowering the threshold to 2 (≈4.1s) shrank it to 3s on the next run — real,
but not closed, and still a race against the fault's own variance.

**A heartbeat sharing the delivery channel produced a genuine duplicate.**
Adding a periodic liveness probe — a publish-and-confirm with nothing behind
it, independent of whether the breaker has an event to send — closed the
*detection* side of the race in principle (worst case
`2×PROBE_INTERVAL + PUBLISH_CONFIRM_TIMEOUT` ≈ 4s, comfortably inside the
5000ms TTL). But it shared `Client.ts`'s one confirm channel with real event
delivery, and under `high`'s 9000/s spike plus an active partition, that
produced exactly the failure mode the sharing risked: `no daemon saw a gap or
duplicate on circuit.control` — a check that had passed clean on every run,
before and after every other fix — failed with 5 duplicates. A real event's
confirm, queued behind heartbeat traffic on the same channel, came back just
past `PUBLISH_CONFIRM_TIMEOUT`; `DELIVERY_RETRY` retried it as a failure, and
the original publish — never actually cancelled, only abandoned — still
landed. Giving the heartbeat its own confirm channel
(`Client.ts`'s `send(..., channelKey)`, new) removed it: the same fault,
same load, `0 gaps, 0 duplicates` again. `pnpm run test:rmq`: 21/21 against a
real broker, including a new test that kills one `channelKey`'s channel and
asserts the other is untouched.

**The heartbeat itself was then found not to be the bottleneck.** Even with
both fixes in place and the duplicate gone, `net-control-partition+outage`
still showed 4s of two-leaders. Checked directly: the heartbeat's own
dedicated channel is confirmed live and publishing at its designed ~1/s
cadence (RabbitMQ management API, idle stack). But the full, unfiltered
aggregator log across the actual fault shows exactly one step-down event,
and it is driven by the *real* transition event's own failed delivery
(`sink.deliver`, called directly from `Aggregator.ts`'s tick loop), not by
the heartbeat — meaning readiness had not yet flipped by the time the
breaker's own transition needed publishing, ~9.5s after the fault (Envoy's
`outlier_detection`: 1s interval, 5 consecutive failures, per host). The
aggregator's own HTTP response times stayed at 0-1ms throughout the same
window, ruling out the obvious explanation (a globally starved event loop).
**Not yet measured**: whether this is Effect's own fiber scheduler falling
behind under the spike's fiber volume specifically, as opposed to Node's I/O
layer — the leading hypothesis, not a confirmed one.

## What was chosen

`AmqpControlPlaneSink.ts` forks a heartbeat loop (`probe`, exposed on
`AmqpSinkImpl` rather than self-started, so constructing a sink in a test
stays inert by default) that publishes an empty, unroutable message
(`HEARTBEAT_ROUTING_KEY = "__heartbeat__"` — no `circuit.<apiId>` prefix, so
it can never collide with a real binding) on its own confirm channel every
`PROBE_INTERVAL` (1s), counted into the same `consecutiveFailures` a real
delivery is. `Client.ts`'s `send` grew an optional `channelKey`: omitted, it
behaves exactly as every existing caller already expected (the one shared
default channel); given, it opens and maintains an independent confirm
channel, reopened on reconnect the same way the default one always was.
`CONSECUTIVE_FAILURE_THRESHOLD` dropped from 3 to 2, matching the arithmetic
its own comment already claimed.

### What it was chosen over

- **Leaving the watchdog at 3 attempts.** Rejected outright — the comment's
  own math didn't hold, independent of anything else measured here.
- **A heartbeat on the shared channel.** The first version. Produced a real
  duplicate under load; not viable regardless of detection speed.
- **Chasing the fiber-scheduling gap further** (a separate worker thread or
  process for the heartbeat, profiling Effect's runtime under load, or
  restructuring `attemptTick`'s delivery path to consult `sinkReady` before a
  real event retries from scratch). Not chosen today — the gap remains
  provably harmless on every run so far (sequence always monotonic, zero
  duplicate control events, zero message loss), and closing it needs real
  profiling, not another constant. Left as a separately scoped follow-up
  rather than guessed at.

## Consequences

- **`never two leaders at once` still fails on `net-control-partition+outage`
  in the chaos suite**, at roughly 3-4s. This is a known, open gap, not a
  regression introduced here — it predates every fix in this record and is
  smaller after them (was unbounded by the breaker's own ~9.5s reaction time
  before any of this; is now bounded by whatever the heartbeat's real
  detection latency turns out to be under load).
- **No evidence of an actual split-brain write** in any run: `the published
  sequence never went backwards`, `no control event delivered twice`, and
  `no message lost` all pass every time this fault has been run. The gap is
  a stale self-reported gauge on the demoted side, not a fencing failure —
  `resetConnection`'s fencing (Client.ts) is what actually prevents a
  double-publish, and it is untouched by any of this.
- **A second confirm channel is now a supported, general capability**, not a
  one-off: any future caller with the same "test the connection independent
  of real traffic" need can ask for its own `channelKey` rather than
  re-deriving this fix.
