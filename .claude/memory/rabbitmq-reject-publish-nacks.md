---
name: rabbitmq-reject-publish-nacks
description: "RabbitMQ's x-overflow reject-publish nacks the losing publisher's confirm rather than silently dropping the message — verify broker-argument semantics against real behavior, not the plausible-sounding reading of the name"
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
