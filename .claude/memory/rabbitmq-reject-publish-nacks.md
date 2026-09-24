---
name: rabbitmq-reject-publish-nacks
description: "RabbitMQ's x-overflow reject-publish nacks the loser's confirm (not a silent drop), and x-max-length counts only READY messages — a held token lets a duplicate in; verify broker-argument wire behavior"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5ee1dde4-09c2-4ea3-8453-6ae6c140ccbc
  modified: 2026-09-18T22:16:26.177Z
---

A queue declared with `x-max-length: 1` + `x-overflow: reject-publish` does
**not** silently drop a publish once the queue is full. It rejects the
message and reports that rejection back to the publisher as a **nack on the
publisher's own confirm** (when publishing in confirm mode). A client that
treats any nack as a real failure — reasonably, since a nack usually means
something — will see this fire for every "loser" of a multi-publisher race,
not just genuine errors.

**Why:** on `/workspace` (2026-09-18), designing article/03's fleet-wide
probe-permit queue, every replica publishes one seed token at startup,
relying on `x-overflow: reject-publish` to keep exactly one and discard the
rest. The plan assumed the discard was silent — "RabbitMQ keeps the first
publish and drops the rest, no election code of our own." Live: four of five
replicas immediately crash-looped on boot with `RmqError: message nacked`,
because `@egress/rmq`'s `send` (reasonably) treats a nacked confirm as
fatal. The fix was to expect and swallow that specific nack in the seeding
code, not to change the queue's overflow mode — the mechanism was right,
the assumption about its wire behavior was wrong.

**How to apply:** when relying on a RabbitMQ queue argument (or any broker
feature) for a specific behavior — especially "silently" doing something —
verify what actually happens on the wire (docs, or a real broker) before
building error handling around it, rather than trusting the plausible
reading of the argument's name. `reject-publish` rejecting *loudly* (a nack)
rather than *quietly* (a drop) is the whole reason that overflow mode exists
as distinct from the default `drop-head` — the name says what happens to
the message, not whether the publisher is told. See
[[check-before-building]] for the related, more general lesson about
verifying real behavior instead of assuming it.


**Second finding, 2026-09-24:** `x-max-length` counts only *ready* messages.
Measured on RabbitMQ 4.3: with the one permit token held unacked (a `get` in
progress), a new seed publish is *accepted*, and the queue then has two tokens.
A quorum queue's limit was looser still (a second seed got in with the first
ready). So "one-token queue" permits need the token returned by
publish-then-ack (the publish is refused while a duplicate is ready, which
collapses it), never by a requeuing nack. Fixed on all three
branches that have a permit (2026-09-24): `article/02-rabbitmq-only-breaker`
(Permit.ts), `article/03-rabbitmq-coordination` (ec207a41c) and
`article/04-429-backpressure` (39cd1ec7c; then named 04-proportional-shedding).

**Same day, a worse one on 03/04:** the permit had never gated a probe at all.
cockatiel moves Open→HalfOpen *inside* `execute()`, just before running the
probe, and `consumer.ts` read `breaker.state` before calling it, so probes
always read Open. Found only by polling the permit queue live (616k polls,
token never taken). A unit test of `withPermit` alone had "proved" it worked.
Lesson: measure the component through its real caller, not in isolation.
