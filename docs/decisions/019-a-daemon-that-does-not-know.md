# 019 — What a daemon does when it does not know the circuit

**Status**: decided 2026-09-29.

## Problem

A daemon learned the circuit only from events, and nothing recorded how old
that knowledge was.

- **At start it assumed CLOSED at full strength.** Its control queue is new, so
  nothing earlier is waiting in it; until the next snapshot (up to 15 s) it
  consumed at full prefetch. Started mid-outage — a deploy, a rescheduled
  container, `--scale` — it spent every message's three attempts against the
  upstream the breaker had stopped, and a fleet restarted together sent a burst
  to `work.dead`.
- **Silent, it kept the last state for ever.** With both aggregators or Redis
  gone during an `OPEN`, the fleet stopped consuming until someone intervened.
  ADR 002 accepts losing the fleet's compensating actions, not a fleet stalled
  for the length of a control-plane outage.

## Decision

`DaemonState.control` says what the circuit is worth: `unheard`, `heard` or
`silent`.

| | Circuit believed | Who works | Probe, redrive, sweep |
|---|---|---|---|
| `unheard` (from start to the first event) | no | nobody, the floor included | no sweep |
| `heard` | yes | the fraction and the floor, as before | as before |
| `silent` (no applied event for `SILENCE_MS`, 60 s) | no | `SILENT_FRACTION` (¼) by position, no floor | none |

- **Silence is four snapshot intervals**, the same length as the floor lease: one
  lost snapshot is noise, four is a control plane that has stopped. A daemon that
  never hears one falls back after the same minute.
- **A stale event is not a sign of life.** Only an applied event resets the clock.
- **Coming back ramps.** The next event after `unheard` or `silent` starts the
  ramp from the floor alone, as after an outage; a fleet restarted while healthy
  reaches full strength in the ramp's 15 s.
- **The fallback has no floor** because the floor is elected by the events that
  stopped. With five daemons a quarter by position can be nobody; that is the
  price of not guessing.

## Why a quarter, not zero or all

Zero is the stall this replaces. All is the burst the breaker exists to prevent,
against an upstream the last event may have said was down. Envoy still ejects and
sheds per replica while the aggregator is gone (ADR 002), so a quarter of the
fleet is load a failing upstream refuses quickly and a healthy one absorbs.

## Consequences

- `egress_daemon_control_knowledge` (0 heard, 1 unheard, 2 silent) and the
  `DaemonsOnFallback` alert. `FloorUnheld` is silenced while any daemon is
  silent, and `FleetDisagreesWithTarget` compares only daemons that have heard.
- The floor lease moved into `DaemonState` with the rest of the decision's inputs,
  and a floor election reconciles at once: before, a newly elected floor waited
  for the next event to start work, and a lapsed one kept working.
- `docs/high-availability.md` has the failure table this fills a row of.

## What would change it

- A fleet large enough that a quarter by position is reliably several daemons
  could fall back higher; one small enough to often land on nobody could elect a
  fallback floor on a queue the control plane does not feed.
- A requirement that nothing call an upstream without a fleet-wide verdict
  (ADR 002's "what would flip it") makes the fallback zero.
