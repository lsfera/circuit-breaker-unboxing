# 006 — How absence, failure and identity are represented

**Status**: decided 2026-09-07; the `undefined` exceptions narrowed by
[018](018-control-flow-as-expressions.md).

## Absence

- **`Option`** wherever code branches on "is there one?" — return types, `Ref`
  contents, record fields. Imported as `O`.
- **`undefined`** only as it arrives from outside (`Map.get`, an options bag),
  converted with `O.fromUndefinedOr` / `O.fromNullishOr` at first use. This
  Effect has no `fromNullable`.
- **`null`** only where a contract insists: `CircuitEvent.previousState` (JSON,
  and the first event has no predecessor), ioredis and gRPC signatures.

**Corollary: an absence inside a positional sequence must keep its position.**
`Outbox.peek` once filtered undecodable entries, and the caller committed by
count, so each filtered entry cost a delivered event its slot and it was sent
twice (`[2, 3, 3]`). `peek` returns `Option` entries, and commits are by
absolute position.

## Failure

- **In `Effect`, the error channel** (`Data.TaggedError`, `catchTag`). A
  `Result` inside an `Effect` is a second error channel.
- **In pure code, `Result` when the caller branches on why, `Option` when it
  does not.** `decodeCircuitEvent` returns
  `Result<CircuitEvent, "malformed-json" | "schema-mismatch">`: the first means
  the publisher is not the aggregator, the second a version skew.
- A Redis checkpoint that is absent, truncated or the wrong shape reads as
  `None`, and says which in a log. Failing would take the instance out of
  leadership over one key; restarting one API's sequence is detectable
  downstream.

## Identity

`LeaseToken` was a string split on `:`, and a malformed counter parsed to
`NaN`, which compares false — so `"abc123:abc"` against a current
`"abc123:5"` was *not* fenced. It is `{ epoch, counter }`, decoded by
`Schema.TemplateLiteralParser([NonEmptyString, ":", Natural])`, ordered by one
total function `isFenced`. Compare tokens with `sameToken`, not `===`: on a
record `===` is identity.
