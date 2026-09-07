# 011 — The concurrency ceiling belongs to the broker

**Status**: decided — the in-process gate is gone.
**Date**: 2026-09-07.
**Context**: raised in review — "I don't like the idea of a concurrency gate".
The objection was right, and measuring it made the case stronger than the
argument was.

## What was there

Each daemon capped concurrent third-party calls with a hand-rolled gate: a
counter, an array of pending resolvers, and `acquire`/`release` around every
call. Its comment claimed the arrangement was

> real backpressure all the way to the queue, not an in-process buffer
> pretending to be one.

It was exactly the second thing. The client's `DEFAULT_PREFETCH` was 100 and
`maxInFlight` was 32 — and the two were chosen that way *deliberately*, with
`Client.ts` explaining that prefetch should sit above the gate so the gate
could do the limiting. So a saturated daemon held a hundred deliveries unacked
while working thirty-two of them, and the other sixty-eight sat in a JavaScript
array.

## What that cost, measured

A backlog was built by pausing the fleet, then the queue was sampled every 20ms
through the drain.

| | peak unacked | working | parked in-process |
| --- | --- | --- | --- |
| Before (prefetch 100, gate 32) | **500** | 160 | **340** |
| After (prefetch 32, no gate) | **160** | 160 | **0** |

500 is exactly five daemons times a prefetch of 100, and 160 is exactly five
times 32. The middle column is the same in both rows, because the number of
calls the fleet can have open never changed. What changed is where the other
340 messages live.

Three things were wrong with them living here:

- **The backlog lied.** Those messages are not in `ready`, so the queue depth —
  the number this repo points at to show a backlog forming — read 340 short.
- **They were hostage.** A paused or wedged daemon still held them. They cannot
  be redelivered to a daemon that is idle until the holder's channel dies,
  which is the opposite of what a competing-consumer fleet is for. The trace
  makes this vivid: with the fleet paused and 3,640 messages waiting, the
  daemons held 160 they were not processing. Under the old shape they would
  have held 500.
- **The lever was already there.** `prefetch` is the broker's own word for "how
  many deliveries may this consumer hold", and the client has taken it as a
  first-class argument since the move to AMQP 0-9-1. The gate reimplemented it
  one layer up, in a place where nothing can see the result.

## The rule

**One limit, at the broker.** The work consumer asks for
`prefetch: maxInFlight`, so at most that many deliveries exist in a daemon at
once and every one of them is a call actually in progress. Everything else
stays in the queue, where the depth is honest and another daemon can take it.

`DEFAULT_PREFETCH` survives as a default for consumers whose work is bounded by
something other than how many messages they hold — the control queue, the two
election queues, a redrive pass. Any consumer whose prefetch *is* its
concurrency limit now passes its own: the work consumer asks for `maxInFlight`,
the HALF_OPEN probe asks for exactly one.

`egress_daemon_queued` is deleted rather than left at zero. It measured the
depth of the in-process buffer, and a gauge whose only honest value is zero is
worse than no gauge — it invites someone to build a panel on it.

## What did not change

Throughput. The drain cleared 3,640 messages in 0.27s either way, because the
number of calls in flight was never what the gate controlled — the two limits
had the same value in the only place it mattered.

And the deeper design is untouched: backpressure is still settlement timing.
`@egress/rmq` settles a delivery only when the handler's promise resolves, so a
daemon that is slow stops acking and the broker stops pushing. That was always
the mechanism. The gate was a second, weaker copy of it inside the process.
