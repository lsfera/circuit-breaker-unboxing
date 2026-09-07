# 006 — How absence is represented

**Status**: decided — applied across the codebase.
**Date**: 2026-09-07.
**Context**: the codebase had `Option`, `null` and `undefined` all meaning
"there isn't one", chosen per-site by whoever wrote the line. That is three
spellings of one idea, and the cost is not aesthetic — a reader has to work out
which convention a given function follows before they can use it.

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
| `RedisLike.eval` → `Promise<string \| number \| null>` | ioredis's own signature. `Outbox.parseList` consumes it and returns `Option`/arrays from there on. |
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

`Option` is imported as `O` throughout. The codebase had both `Option.` and
`O.`; one spelling is worth more than whichever spelling wins.

Two idioms worth naming, because they are what made the conversions read better
rather than worse:

- `O.toArray` as the filter-map. `parseList(result).flatMap((raw) =>
  O.toArray(parseEvent(raw)))` drops what did not parse without a `.filter`
  and a type predicate to keep in step with it.
- `O.fromUndefinedOr` at a lookup boundary — `O.fromUndefinedOr(map.get(k)?.x)`
  converts once, at the edge, rather than letting `undefined` travel.

Note for anyone reaching for the usual names: this Effect version has
`fromNullishOr` / `fromUndefinedOr` / `fromNullOr`, and no `fromNullable`.

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
