# Effect 3 idioms, and what they are in Effect 4

This repository runs `effect@4.0.0`. Most Effect material in
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
| `Either`, `Effect.either` | `Result`, `Effect.result` | v3-to-v4.md:22, :9879 |
| `Option.fromNullable` | `Option.fromNullishOr` — or `fromUndefinedOr` / `fromNullOr` when only one of them is possible, which is what this codebase uses | v3-to-v4.md:12762; `Option.ts:773,807,841` |
| `Effect.catchAll` | `Effect.catch` | v3-to-v4.md:9831 |
| `Effect.fork` | `Effect.forkChild` | v3-to-v4.md:9915 |
| `Effect.forkDaemon` | `Effect.forkDetach` | v3-to-v4.md:9919 |
| `Effect.zipRight` | `Effect.andThen` | v3-to-v4.md:10163 |
| `Context.Tag` | `Context.Service` | v3-to-v4.md:9471 |
| `FiberRef` | `Context.Reference` / `References` | v3-to-v4.md:277 |
| `Schema.annotations` | `Schema.annotate` | v3-to-v4.md:14672 |
| `Schema.decodeUnknown` | `Schema.decodeUnknownEffect` | v3-to-v4.md:14712 |
| `Schema.DateTimeUtcFromSelf` | `Schema.DateTimeUtc` | v3-to-v4.md:14164 |
| `Schema.Data` | nothing — delete the wrapper; decoded objects already have structural equality | v3-to-v4.md:14144 |
| `DateTime.unsafeNow` | `DateTime.nowUnsafe` — `unsafe` moved to the end of every name | v3-to-v4.md:9587 |

## Changed contracts, not just names

**`filterMap` keeps a `Result`, not an `Option`.** `Array.filterMap` and
`Stream.filterMapEffect` take a function returning `Result.succeed(value)` to
keep and `Result.fail(anything)` to skip
([`Array.ts:3803`](../repos/effect/packages/effect/src/Array.ts),
[`Stream.ts:4195`](../repos/effect/packages/effect/src/Stream.ts)). Returning
an `Option` does not compile; converting it back to an `Option`-shaped helper
is the wrong fix. See `packages/subscriber/src/subscriber.ts` and
[ADR 006](../docs/decisions/006-representing-absence.md).

## Where this repository's own material disagrees

A `domain-modeling` skill in `.claude/skills/` taught `Schema.annotations`,
`Schema.Data`, `Schema.DateTimeUtcFromSelf` and `DateTime.unsafeNow`. None of
the four exists in the installed version — checked by name against
`repos/effect/packages/effect/src/Schema.ts` and `DateTime.ts` — and the skill
was deleted. Its structure (tagged unions, constructors, guards,
`Match.typeTags`) is still sound, so if domain models need guidance again, write
it against the table above.

Two more skills were deleted alongside it, for reasons that aren't v3-vs-v4
idioms, so they aren't repeated here: `effect-testing` assumed `@effect/vitest`
— see this repository's actual choice in
[`effect-guide-in-this-repo.md`](effect-guide-in-this-repo.md)'s testing row —
and `effect-ts` (Effect-TS/skills) pinned a floating `effect@rc` and pointed at
`node_modules/effect`, which doesn't exist at the root under pnpm (`git log`,
commit `1a213315fe`).

When a skill, a post or a remembered example disagrees with `repos/effect`,
the vendored source is right.
