# 006 — How absence, failure and identity are represented

**Status**: decided — applied across the codebase.
**Date**: 2026-09-07.
**Context**: the codebase had `Option`, `null` and `undefined` all meaning
"there isn't one", chosen per-site by whoever wrote the line. That is three
spellings of one idea, and the cost is not aesthetic — a reader has to work out
which convention a given function follows before they can use it. The same
question one level up — how a computation reports *failure* — had the same
problem, and is answered in the second half.

## The rule

**`Option` when absence is a result the code branches on.** Return types,
values held in a `Ref`, fields of a domain record. If a caller has to ask "is
there one?", the type should make them.

**`undefined` when absence is structural rather than semantic.** Three cases,
all of them boundaries rather than decisions:

- *Lookups.* `Map.get` and index access return `undefined`, and wrapping every
  one costs a line and buys nothing.
- *Memoisation sentinels.* `let cached: T | undefined` means "not computed
  yet", which is a different question from "computed, and empty". `Client.ts`
  caches three lazily-derived delivery fields this way; making them `Option`
  would need `Option<Option<T>>` to keep the distinction.
- *Optional inputs.* An options bag takes `readonly prefetch?: number`, not
  `Option<number>`. The caller omits it; they do not construct an absence.

**`null` only where something outside this repo insists.** It is converted at
the boundary and never travels further.

## Where `null` legitimately stays

| Where | Why |
| --- | --- |
| `CircuitEvent.previousState` | `Schema.NullOr` — the published wire contract. JSON has `null`, not `None`, and the first event for an API genuinely has no predecessor. Changing this would break every subscriber. |
| `RedisLike.eval` → `Promise<string \| number \| null>` | ioredis's own signature. `Outbox.stringList` consumes it and returns arrays from there on. |
| `grpc.ServiceError \| null` in `EnvoyPushSource` | The gRPC callback signature. |
| `packages/demo/src/driver.ts` | A standalone script parsing HTTP JSON where `null` is the payload's own shape. It depends on nothing and nothing depends on it. |

## What changed

| | Was | Now |
| --- | --- | --- |
| `Aggregator.stateOf` | `Effect<State \| null>` | `Effect<O.Option<State>>` |
| Aggregator leadership token | `LeaseToken \| null` | `O.Option<LeaseToken>` |
| In-memory coordination lock | `LockState = {...} \| null` | `Ref<O.Option<Lock>>` |
| `Outbox.parseEvent` | `CircuitEvent \| null` | `O.Option<CircuitEvent>` |
| `DeliveryInfo.deadLetter` | `{...} \| null` | `O.Option<{...}>` |
| `DeliveryInfo.parent` | `ExternalSpan \| null` | `O.Option<ExternalSpan>` |
| `Trace.traceparent` | `Effect<string \| undefined>` | `Effect<O.Option<string>>` |
| `Trace.parentFrom` | `→ ExternalSpan \| null` | `→ O.Option<ExternalSpan>` |
| `Tracing.endpoint` | `→ string \| undefined` | `→ O.Option<string>` |
| `decodeCircuitEvent` | `→ O.Option<CircuitEvent>` | `→ Result<CircuitEvent, DecodeFailure>` |

`Option` is imported as `O` throughout. The codebase had both `Option.` and
`O.`; one spelling is worth more than whichever spelling wins.

Two idioms worth naming, because they are what made the conversions read better
rather than worse:

- `O.toArray` as the filter-map. `stringList(result).flatMap((raw) =>
  O.toArray(Result.getSuccess(decodeCircuitEvent(raw))))` drops what did not
  decode without a `.filter` and a type predicate to keep in step with it.
- `O.fromUndefinedOr` at a lookup boundary — `O.fromUndefinedOr(map.get(k)?.x)`
  converts once, at the edge, rather than letting `undefined` travel.

Note for anyone reaching for the usual names: this Effect version has
`fromNullishOr` / `fromUndefinedOr` / `fromNullOr`, and no `fromNullable`.

## Failure: `Result`, or the error channel

The same question one level up — how does a computation say it *failed* — has
the same shape of answer, and the dividing line is whether the code is
effectful.

**In `Effect`, use the error channel.** `Effect<A, E>` is already the
either-with-effects, and this repo uses it well: `RmqError`,
`CoordinationUnavailable`, `CheckpointFenced` are `Data.TaggedError`s in the
error channel, recovered with `Effect.catchTag`. Reaching for a `Result`
*inside* an `Effect` would be a second error channel next to the one that
already exists, and combinators would stop composing.

**In pure code, `Result` when the caller branches on why; `Option` when
absence is the whole story.** That is the whole rule, and it is why most of
this repo's pure fallible functions correctly return `Option`:
`Trace.parentFrom` fails only one way (the header did not parse) and the
caller's response is the same either way — skip the trace.

One function did not fit. `decodeCircuitEvent` returned an `Option`, and its
own comment admitted the collapse: *"malformed JSON decodes to None, same as a
schema mismatch"*. Those mean different things to the fleet. A schema mismatch
is a version skew between the aggregator and the daemons — the exact failure
that dead-lettering undecodable messages exists to catch, and the one
`egress_daemon_undecodable_total` counts. Something that is not JSON at all
means the publisher is not the aggregator. It is now
`Result<CircuitEvent, "malformed-json" | "schema-mismatch">`, and the daemon
logs the two separately.

### The case that looked like `Result` and is not

The Redis `CheckpointStore.load` reads a checkpoint that may be absent,
truncated, or valid JSON of the wrong shape, and returns `Option` for all
three. That reads like a `Result` waiting to happen, and it should stay as it
is:

- `load` is effectful, so the candidate was never `Result` — it was the error
  channel, `Effect<Option<Checkpoint>, CoordinationUnavailable | Unreadable>`.
- Failing there takes the instance out of leadership over one corrupt key.
  Resuming that one API from nothing costs it its sequence continuity, and
  *that is detectable downstream* — a sequence starting over is precisely what
  the delivery-contract check watches for. Trading a detectable anomaly for an
  outage is the wrong way round.
- `RedisCoordination.test.ts` already asserts all three read as absent, which
  makes it a tested decision rather than an accident.

What was wrong with it was not the type: it was **silent**. Both branches
returned `None` with nothing written down. It logs which one now, and the
`Option` stays.

## Identity: primitive obsession, and the one that mattered

A separate pass looked for domain concepts carried as raw primitives. Most of
what turned up is fine — `apiId` is a string because it *is* a string, a key
with no invariant to protect — and the pure-function-over-record shape of
`@egress/domain` is FP working as intended, not an anemic model. Adding methods
to those records would be the wrong direction.

One was not fine, and it was the most safety-critical value in the system.

`LeaseToken` was `type LeaseToken = string`, holding `"<epoch>:<counter>"` and
picked apart with `slice(indexOf(":"))` wherever it was compared. Two problems,
one of them a live hole:

- Any string was a token. Nothing stopped `save(apiId, "hello", checkpoint)`.
- `tokenCounter` returned `NaN` for a counter that did not parse, and
  `NaN < current` is `false` — so **a malformed token carrying the right epoch
  was not fenced**. Reproduced against the old helpers before changing them:
  `"abc123:abc"` against a current of `"abc123:5"` came back accepted. That is
  precisely the write fencing exists to reject.

It is a record now — `{ epoch, counter }` — with the ordering rule in one total
function, `isFenced`. The wire form is a `Schema`:

```ts
Schema.TemplateLiteralParser([Schema.NonEmptyString, ":", Schema.Natural])
```

`Schema` rather than a hand-written parser for the reason `CheckpointFromJson`
is a schema: the value crosses a boundary, and what counts as a valid one
belongs in one declaration instead of a parser and a formatter that have to be
kept in agreement. `Natural` is what closes the hole — `"abc:abc"` and
`"abc:-1"` do not decode at all, so a token whose counter cannot be ordered
cannot be built, and nothing downstream has to defend against one.

The change also forced a bug into the open that a type alias had been hiding:
`justAcquired` compared tokens with `!==`. On strings that was value equality;
on records it is identity, so every tick would have looked like a fresh
acquisition and rehydrated the registry. `sameToken` says what was always
meant. Nothing in the compiler would have caught that — it is the kind of thing
a primitive lets you get away with until the day it doesn't.

(`justAcquired` itself is gone: rehydration turned out to need only "no breaker
for this API yet", and coupling it to the acquisition tick as well is what made
the checkpoint unreadable under a push-based source. See `docs/findings.md`.
The equality lesson above is why this paragraph stays.)

## Consequences

- Three call sites in `Aggregator.test.ts` compare with `assert.deepEqual(x,
  O.some(...))` rather than `assert.equal`. That is the type change surfacing
  in the tests, which is the point of making it.
- The in-memory lock is slightly wordier: `O.getOrUndefined(current)` where it
  used to read `current?.`. Accepted — the `Ref` now says what it holds, and
  `tryAcquireOrRenew` already returned `Option`, so the two halves finally
  agree.

## What would change this

A hot path where an `Option` allocation per item is measurable. Nothing here
qualifies: the highest-rate `Option` in the codebase is `DeliveryInfo.parent`,
computed lazily and read once per delivery.
