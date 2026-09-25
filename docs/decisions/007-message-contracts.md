# 007 — One declaration per message, read at both ends

**Status**: decided 2026-09-07.

## Rule

- Every message crossing a process boundary decodes through one `Schema`, and
  the publisher encodes through the same one.
- A value the receiver **orders or branches on** is declared tightly enough
  that an unusable one does not decode: sequences are `Schema.Natural`, reasons
  are `ReasonSchema`.
- What cannot be read is dead-lettered and counted, never dropped in silence.

## Why

The election trigger was `JSON.stringify({ sequence })` at one end and
`Number(JSON.parse(body).sequence ?? -1)` at two others. `NaN` compares false,
so a trigger that could not be ordered read as new and left the dedupe state
broken for the next one. Against the pure reducer, one malformed trigger among
the five copies of a `HALF_OPEN` transition produced **three probes** where
the contract is one.

`reason` was `Schema.String` and cast at its one consumer, so a checkpoint could
save a reason it would fail to decode on rehydration — a silent cold start at
`CLOSED`.

## Also

- **One rule for sequence gaps**: `classifySequence` in `@egress/domain`, used
  by both the aggregator's webhook observer and the daemons' AMQP observer, so
  two independent vantage points cannot disagree about what a gap is.
- **One clock**: the envelope's `time` is the Effect clock's, not `new Date()`,
  so tests can assert it.
- The work message has its own schema too ([018](018-control-flow-as-expressions.md)).

## Trade

A new `Reason` is a contract change: older subscribers reject it rather than
display it. If an external subscriber needs to survive additions, decode an
unknown reason to an explicit "unrecognised" case, not back to `String`.
