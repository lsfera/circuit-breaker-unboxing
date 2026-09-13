# Effect 3 idioms, and what they are in Effect 4

This repository runs `effect@4.0.0-rc.115`. Most Effect material in
circulation — blog posts, model memory, and some of this repository's own
agent skills — is Effect 3, and Effect 4 renamed or removed a good deal of it.
The code usually fails to compile, which is the good outcome. The bad one is
an agent "fixing" the type error by reaching for a different v3 API.

The authority is the generated rename map,
[`repos/effect/migration/v3-to-v4.md`](../repos/effect/migration/v3-to-v4.md)
(line numbers below), and the source it describes. This file is only the
subset that has come up here.

## Renames that have come up in this repository

| Effect 3 | Effect 4 | Source |
| --- | --- | --- |
| `Either`, `Effect.either` | `Result`, `Effect.result` | v3-to-v4.md:22, :9861 |
| `Option.fromNullable` | `Option.fromNullishOr` — or `fromUndefinedOr` / `fromNullOr` when only one of them is possible, which is what this codebase uses | v3-to-v4.md:12744; `Option.ts:773,807,841` |
| `Effect.catchAll` | `Effect.catch` | v3-to-v4.md:9813 |
| `Effect.fork` | `Effect.forkChild` | v3-to-v4.md:9897 |
| `Effect.forkDaemon` | `Effect.forkDetach` | v3-to-v4.md:9901 |
| `Effect.zipRight` | `Effect.andThen` | v3-to-v4.md:10145 |
| `Context.Tag` | `Context.Service` | v3-to-v4.md:9453 |
| `FiberRef` | `Context.Reference` / `References` | v3-to-v4.md:277 |
| `Schema.annotations` | `Schema.annotate` | v3-to-v4.md:14654 |
| `Schema.decodeUnknown` | `Schema.decodeUnknownEffect` | v3-to-v4.md:14694 |
| `Schema.DateTimeUtcFromSelf` | `Schema.DateTimeUtc` | v3-to-v4.md:14146 |
| `Schema.Data` | nothing — delete the wrapper; decoded objects already have structural equality | v3-to-v4.md:14126 |
| `DateTime.unsafeNow` | `DateTime.nowUnsafe` — `unsafe` moved to the end of every name | v3-to-v4.md:9569 |

## Changed contracts, not just names

**`filterMap` keeps a `Result`, not an `Option`.** `Array.filterMap` and
`Stream.filterMapEffect` take a function returning `Result.succeed(value)` to
keep and `Result.fail(anything)` to skip
([`Array.ts:3803`](../repos/effect/packages/effect/src/Array.ts),
[`Stream.ts:4145`](../repos/effect/packages/effect/src/Stream.ts)). Returning
an `Option` does not compile; converting it back to an `Option`-shaped helper
is the wrong fix. See `packages/subscriber/src/subscriber.ts` and
[ADR 006](../docs/decisions/006-representing-absence.md).

## Where this repository's own material disagrees

`.claude/skills/domain-modeling/SKILL.md` teaches `Schema.annotations`,
`Schema.Data`, `Schema.DateTimeUtcFromSelf` and `DateTime.unsafeNow`. None of
the four exists in the installed version — checked by name against
`repos/effect/packages/effect/src/Schema.ts` and `DateTime.ts`. Its structure
(tagged unions, constructors, guards, `Match.typeTags`) still applies; its API
names do not. `.claude/skills/effect-testing` is written for `@effect/vitest`,
which this repository does not use: tests here are `node:test` running Effect
programs, with `TestClock` from `effect/testing`.

When a skill, a post or a remembered example disagrees with `repos/effect`,
the vendored source is right.
