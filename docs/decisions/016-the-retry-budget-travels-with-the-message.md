# 016 — The retry budget travels with the message

**Status**: decided — failed calls are retried by republish, carrying an
idempotency key and an attempts header; the floor sweeps `<api>.work.dead`
while `CLOSED`; a message redriven `MAX_REDRIVES` times is parked.
**Date**: 2026-09-13.
**Context**: the consumer taking over the idempotency key (ADR 010's amendment
and ADR 011 both assumed a shed request was the only thing that needed to
survive a retry unchanged; giving every call attempt its own key raised the
question of how a retry carries anything at all).

## What was measured

Chaos harness `infra/chaos-load.mjs`, fault `kill-broker`, against the
previous design — a broker-counted requeue and a key assigned once by the
producer: **2** messages dead-lettered, both `reason: delivery_limit`,
`x-delivery-count: 4`, while the circuit never left `CLOSED`. A per-message
audit alongside it found **0 lost** and **18 duplicate calls** to the
upstream. Nothing was lost, but with one key per message rather than per
attempt, a message redelivered after a connection loss made two calls that
looked, from the third party's side, identical and unrelated — the second
carried the same key as the first only because both happened to be the same
message, not because anything told the daemon they were.

After deploying republish-based retries: the sweep's first pass replayed
**21,646** of a **22,246**-message dead-letter backlog left over from the
Envoy-shedding incident in [ADR 010's amendment](010-a-proxy-that-fails-on-its-own-behalf.md#amendment--2026-09-13),
taking the queue to 0 with nothing parked. `pnpm run check`: 118 unit tests
pass. `pnpm run test:rmq`: 18 broker tests pass.

The first chaos matrix with this change in place then **lost 1,570 messages**
under an upstream outage. The obvious suspect, the work queue's default
at-most-once dead-lettering, was wrong: a probe showed dead letters reaching
`<api>.work.dead` intact. The loss was in the dead-letter queue itself. A
quorum queue has `x-delivery-limit: 20` unless told otherwise, and every
redrive pass hands back what it does not move (its channel closing counts as a
return). With no dead-letter target of its own, the queue *drops* a message
at that limit (`dead_letter_strategy="disabled"`). On a temporary queue, 0 of
50 messages survived 22 channel closes; with `-1`, all 50 survived 25. The dead-letter
and parked queues now declare `x-delivery-limit: -1`, and
`DeadLetter.test.ts` fails without it. The work queue got at-least-once
dead-lettering as well, which is still correct but was not the fix. The low-profile matrix
afterwards: 34 of 34 scenarios lost nothing and left the dead-letter queue empty.

**Not yet measured**: the right jitter range for the shed backoff, and the
throughput cost of paying a publish on every failed call rather than a bare
requeue.

## What was chosen

A failed call (anything other than a 2xx or a 429) is retried by
**republishing** the body — never by `requeue` — carrying
`x-idempotency-key` (`Attempts.ts`, `ControlPlane.ts`'s `IDEMPOTENCY_KEY_HEADER`)
and `x-egress-attempts` forward, incremented, plus `x-egress-redrive-count`
when the delivery already had one. The original is acked only after the
republish succeeds — publish first, ack second, so a crash between the two
produces a duplicate rather than a loss. Attempts 1 and 2 go back to
`<api>.work`; the third goes straight to `<api>.work.dead` with
`x-egress-origin-queue` / `x-egress-origin-reason: attempts-exhausted`, since
a direct publish carries none of the broker's own `x-first-death-*`
annotations and would otherwise look unattributable to the redrive. If the
republish itself fails, the daemon falls back to a plain `requeue` — the
broker's `x-delivery-limit: 3` is what backstops a delivery that cannot even
be republished.

The floor daemon (the one elected on `<api>.floor`) now dispatches a
`SweepTick` (`DaemonState.ts`) every 30 seconds. While `CLOSED` and with
`REDRIVE_ON_CLOSE` on, it runs the same bounded redrive pass that used to run
only on a transition into `CLOSED`. A pass that moves nothing logs at debug.
Both the sweep-triggered and election-triggered paths go through one atomic
claim (`redriveRunning`, a `Ref<boolean>` in `daemon.ts`) so they can never
both hold a consumer on `<api>.work.dead` at once — the previous guard read
the consumer `Ref` without holding it across the whole consume, which left a
window where two passes could both start.

Each redrive stamps `x-egress-redrive-count` (`Redrive.ts`'s `nextRedrive`).
Past `MAX_REDRIVES` (5) a message goes to `<api>.work.parked` — durable,
quorum, no dead-letter target of its own — with a warning log, instead of
back to work. A redrive still resets `x-egress-attempts`: a fresh outage
deserves a fresh call budget, the same stance the header already took before
parking existed.

### What it was chosen over

- **Requeue, with the budget held by the broker.** This is what the fleet did
  before. A broker requeue hands back the *original* message, and there is no
  way to add a header to a message that already exists on the wire — so the
  key could travel with the message only if every retry of an attempt reused
  the same delivery, which a redelivery after a connection loss does not
  guarantee. Republishing is the only way to attach a header to a retry at
  all; the broker-held budget was a side effect of `requeue`, not something
  chosen for its own sake, and it is given up here as the price of the key
  surviving.
- **A producer-assigned key.** Rejected earlier, for a reason unrelated to
  retries: key ownership was decided to not be a producer concern (the
  producer never sees the daemon's call outcome, only whether a message was
  published). This decision does not revisit that; it is the consequence of
  it. Once the daemon owns the key, it is also the only thing that can put it
  on a retry.
- **A key derived from message content.** Would need no header at all if the
  body were stable across every republish — which it is, so the option was
  real. Not chosen because it ties the key to *what* the message says rather
  than *which attempt* is being made: a legitimately republished retry and an
  unrelated message that happens to carry the same body would be
  indistinguishable to the third party, where a randomly minted key
  distinguishes them by construction. Not measured against the chosen
  approach.
- **Redrive only on the transition into `CLOSED`.** The design until now.
  Messages dead-letter while the circuit stays `CLOSED` too — a broker
  restart advancing `x-delivery-count` on outstanding deliveries, an Envoy
  503 counted as a failure before ADR 010's amendment, a rolling redeploy
  churning consumers mid-delivery — and none of that is a recovery, so
  nothing would ever replay them until the breaker happened to open and close
  again, which might not happen for a long time on a fleet that never trips.
  The sweep is the same bounded pass, run on a clock instead of a transition.

## Consequences

- **The retry budget is now split, not unified.** `x-egress-attempts` — a
  header the daemon reads and writes — is what actually bounds a failing
  message's calls per outage; `x-delivery-limit: 3` on the queue stays as a
  backstop for deliveries that keep coming back *unsettled* (a consumer
  killed mid-call, a connection lost before the republish), not as the
  primary counter. This is a reversal of the position written down in
  [ADR 001](001-amqp-client.md) and [ADR 004](004-downgrade-to-amqp-0-9-1.md),
  and repeated in `docs/rmq-control-plane.md`, that the budget belongs to the
  queue *because* an in-process or header-carried counter is lost the moment
  a message moves — the reasoning was right for a requeue and does not
  survive the move to republishing. Those two decision records are not
  amended here; a reader relying on them for how retries work today will be
  wrong, and `docs/rmq-control-plane.md` and `docs/approaches.md` are updated
  to describe the current behaviour, but the ADRs themselves are left as the
  record of what was true when they were written.
- **A known gap.** A daemon killed after the call returns but before the
  republish or the ack completes gets the *original* delivery back from the
  broker, unsettled — with no key, since the key only ever lived in the
  daemon's local variable and the call headers, never on the message. That
  one retry mints a new key rather than reusing the one the failed call used,
  which is the one case this design does not close.
- **The cost is paid only on failure.** A successful call — the common case,
  running thousands of times a second — carries no extra publish; the header
  materialization and the republish itself happen exclusively on the
  `republish` branch of `Attempts.nextAttempt`, not on every call.
- **Parking is a consequence of the key, not an independent decision.** A
  message that can never succeed used to cycle through dead-letter and
  redrive forever, each time looking like a fresh attempt to the third party.
  Giving every message a stable per-attempt key makes that upstream-visible,
  which is what makes bounding it with `MAX_REDRIVES` worth doing now rather
  than earlier.
