---
name: check-before-building
description: "Before building a custom mechanism (heartbeat, retry, backoff, dedup...), check whether the library/protocol already provides it — verify against the actual installed source, not assumption."
metadata:
  node_type: memory
  type: feedback
---

Before hand-building a mechanism that sounds like infrastructure plumbing
(a heartbeat, a retry policy, connection liveness, backoff, dedup), check
whether the library or protocol already provides it, and whether it's just
misconfigured or unused — don't assume a gap exists without checking the
actual installed source.

**Why:** on `/workspace` (2026-09-18), a whole ADR's worth of code — a
reserved routing key, an isolated confirm channel, a forked probe loop —
was built to detect a dead RabbitMQ connection fast. The user pushed back:
RabbitMQ already heartbeats a connection. Checking `node_modules`'s actual
amqplib source confirmed it: the client library already independently
tracks broker activity and closes a dead connection on its own, feeding
the exact same `isConnected` flag the codebase already read — the app
never configured the heartbeat interval, so it silently used the
library's 60s default instead of something useful. One connect option
(tuned to 1s) plus consulting the existing flag per-attempt matched the
custom heartbeat's detection speed, live-verified, with ~100 fewer lines.
See [[chaos-reliability-work-2026-09-17]] for the full arc.

**How to apply:** when a task is "add liveness/health detection for X," or
any other mechanism a mature library/protocol commonly ships (heartbeats,
reconnection, retries, idempotency, backoff), read the actual vendored or
installed source for that capability before designing something bespoke.
A misconfigured default masquerading as a missing feature is a real,
recurring failure mode — cheaper to rule out first than to build around.
