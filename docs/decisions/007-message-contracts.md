# 007 — One declaration per message, read at both ends

**Status**: decided — applied across the codebase.
**Date**: 2026-09-07.
**Context**: the pass after
[006](006-representing-absence.md) and the lease token, and the same question
one level out. 006 settled how a value says "there isn't one" and how a
computation says "that failed". The lease token settled what a *value* crossing
a boundary is allowed to be. This settles the one above both: what counts as a
valid **message**, and where that is written down.

## The rule

**Every message that crosses a process boundary decodes through one
declaration, and the code that publishes it encodes through the same one.**
Not a schema at the reader and a `JSON.stringify` at the writer; not two
readers that have to agree.

**A value the receiver will *order* or *branch on* must be declared precisely
enough that an unusable one does not decode.** A sequence that cannot be
compared and a reason that is not in the vocabulary are not "unusual input" —
they are inputs the code has no meaning for, and the boundary is where that is
cheapest to say.

**What could not be read is preserved, never dropped in silence.** That was
already this fleet's discipline — the canonical dead-letter queue exists for
exactly this — and the exception was the one message kind with no schema.

## What the pass found

Three things, one of which mattered on its own.

**The election trigger had no contract at all.** It is the body on both SAC
queues, `probe-trigger` and `redrive-trigger`, and it was
`JSON.stringify({ sequence })` at the publisher and
`Number(JSON.parse(body).sequence ?? -1)` at each of *two* consumers — one
encoder and two parsers, kept in agreement by hand, for the message that
decides how many times a recovering third party gets called.

`Number(...)` returns `NaN` for `{"sequence":"7"}`, `{"sequence":{}}` and
`{"sequence":[7]}`, and the elected daemon dedupes with
`sequence <= probedSequence`, where every comparison against `NaN` is false. So
a trigger that could not be ordered read as a *new* transition, and left
`probedSequence` in a state where the next real trigger read as new as well.
Measured against the pure reducer, with one malformed trigger dropped into the
five copies of one sequence a HALF_OPEN transition produces: **three probes
where the contract says exactly one**. That contract — exactly one call into an
upstream that has just started answering — is the entire reason the election
exists.

This is the same shape as the hole the previous pass closed in the lease token,
found the same way, in the code that fix did not reach.

The `?? -1` had a quieter failure of its own: a trigger with no usable
`sequence` became a valid-looking number that every daemon silently ignores.
Neither acted on nor preserved — the one outcome this fleet refuses everywhere
else.

**`reason` on the published event was `Schema.String`,** six lines below the
`ReasonSchema` that lists the vocabulary. The tell was in `Aggregator.ts`: the
one place that consumes a decoded event's reason as a `Reason` had to write
`as Reason` to do it — the "type assertion wearing a schema's clothes" that
`Model.ts`'s own comment condemns, left behind when the checkpoint validator
that comment is about was fixed. It also left the checkpoint's *write* side
looser than its read side, and those two disagreeing has a consequence: a
checkpoint whose reason is outside the vocabulary saves fine and then fails to
decode on rehydration, which reads as "no checkpoint" and cold-starts a new
leader at CLOSED.

**`sequence` on the published event was `Schema.Number`** — the value the whole
delivery contract orders by. `Contract.observe` counts a gap or a duplicate by
comparing it against the last one seen, and the same orderability argument
applies as on the trigger.

## What changed

| | Was | Now |
| --- | --- | --- |
| Election trigger | `JSON.stringify` + two hand-written parsers | `ElectionTrigger` schema; `encodeElectionTrigger` / `decodeElectionTrigger` |
| Trigger `sequence` | `Number(x ?? -1)`, `NaN` on anything odd | `Schema.Natural` — an unorderable sequence does not decode |
| `CircuitEventData.reason` | `Schema.String`, cast to `Reason` at the one consumer | `ReasonSchema`, no cast |
| `CircuitEventData.sequence` | `Schema.Number` | `Schema.Natural` |
| Reading a message | one schema reader, two hand parsers | one `readerFor`, used by both |
| The daemon's two trigger handlers | duplicated twelve lines each | one `onTrigger`, given what to build |

The two failure reasons stay distinct, as 006 set out: `malformed-json` means
the publisher is not who we think it is, `schema-mismatch` means a version skew
between it and this fleet. Both now reach the trigger queues, which means a
malformed trigger is dead-lettered and counted in
`egress_daemon_undecodable_total` like every other unreadable message, instead
of being turned into a `-1`.

## What was deliberately left alone

**The work message body.** `@egress/rmq-producer` publishes `{ apiId, n }` and
nothing decodes it — the daemon's handler takes `_body`, and a redrive
republishes the bytes verbatim. A schema for a value nobody reads is ceremony,
and it would have to be maintained against a payload whose whole point is that
this repo does not care what real work looks like.

**The endpoint counts on the published event.** `healthyEndpoints`,
`totalEndpoints` and `reportingReplicas` stay `Schema.Number`. They are
reported, not ordered and not branched on; tightening them would be tidiness,
not a closed hole.

## Consequences

Tightening a decoder means messages that used to be accepted are now rejected.
Nothing in this repo publishes one — the aggregator's reasons come from
`Breaker`, its sequences from a counter that starts at 0 and increments — so
the rejections this can produce are exactly the ones worth having: a publisher
that is not this aggregator, or a version of it that disagrees with this fleet
about the vocabulary. That is the failure the dead-letter path was built to
make visible, and it now covers the last message kind that was bypassing it.

The reverse cost is real and worth stating: a future aggregator that adds a
`Reason` publishes events an older subscriber will now reject rather than
display. That is the same trade `state` and `previousState` have always made
through `StateSchema`, so this makes the contract consistent with itself rather
than introducing a new constraint. A new reason is a contract change; this makes
it one that shows up at the boundary instead of in a log line.

## What would change this

A subscriber outside this repo that needs to survive vocabulary additions it
has not deployed for. The fix then is a schema that decodes a known reason and
keeps an unknown one as an explicit "unrecognised" case — not a return to
`Schema.String`, which does not distinguish the two.

## One rule, two observers

Added 2026-09-10. The guarantee this record is about — per-API sequences are
gapless and never repeat — was *checked* in two places and *stated* in two
places, which are not the same thing.

`Http.record` watches the webhook subscriber over HTTP from inside the
publishing process; `Contract.observe` watches over AMQP from five daemons the
publisher does not control. Both had their own `<=`-not-`===` comparison and
their own paragraph explaining why it is `<=`. The vantage points are what make
the two independent evidence; the rule is not, and two observers of one
guarantee disagreeing about what a violation *is* would make both readings
worthless rather than corroborating.

`classifySequence` in `@egress/domain` is the rule, returning `first` /
`duplicate` / `gap` / `next` against the highest sequence already seen. What
stays local is the shape each observer needs: one highest-sequence-per-API map
in the aggregator, which watches every API at once, and a single
`lastSequence` in a daemon fleet that serves one. Both were checked against
their previous implementations over 240,000 random transitions each before the
old code was deleted.

`SEQUENCED_EVENT` moved with it. It was a named constant on the daemon side and
a repeated string literal on the aggregator's.

## The published `time` came from a different clock

Added 2026-09-10. Every instant this system reasons about comes from the Effect
clock, which is what lets `TestClock` drive a simulated minute in microseconds
— and `Events.build` read `new Date()` for the envelope's `time` field. A tick
therefore carried two clocks: `observedSince` in simulated time two lines below
`time` in wall time.

Nothing was wrong in production, where the two agree. What it cost was the
test: `assert.ok(Date.parse(e.time) > 0)` was as much as could be said about a
value that had nothing to do with the run. `build` takes `now` from the caller,
which already had it, and the assertion is now that every published `time`
falls inside the window the test clock was advanced through. Confirmed to fail
when `new Date()` is put back.
