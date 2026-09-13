---
name: effect-functional-style
description: How the user wants Effect code written in /workspace — composition over if/else and loops, Option/Result combinators, and simple over clever.
metadata:
  type: feedback
---

The user asked, across several requests on this repo: *"verify the idiomatic use
of Option. do not use as if the else but use more functional constructs and
composition"*, then *"review whole solution. Use effect constructs. Remove if
clauses and loops (for/while)"*. When a helper I wrote for that combined
`O.firstSomeOf` with a higher-order function, the reply was *"it's convoluted"* —
it was collapsed to one `O.getOrElse` over a pair.

**How to apply**:
- Branch with `O.match` / `O.map` / `O.getOrElse` / `Result.match`, a ternary, or
  `Effect.when` — not `if` statements. Iterate with `Arr.*`, `Effect.forEach`,
  `Stream`, or recursion — not `for`/`while`. Test files use the same style.
- Data-first calls (`O.map(opt, f)`) are the settled style for Option; the
  surrounding `.pipe` usage in files like `Http.ts` is fine to match.
- `Arr.filterMap` and `Stream.filterMapEffect` take `Result` (succeed = keep,
  fail = skip), not `Option`.
- The functional version must also be the *simpler* one. If removing an `if`
  needs a combinator the reader has to look up plus a helper, it has gone too
  far — pick the most direct composition.
- This repo is Effect 4 (rc.113). Check APIs against `repos/effect`, not memory;
  see `agent-patterns/effect-3-to-4.md` in the repo.

Related: [[essential-comments-not-narrative]], [[rmq-control-plane-design]].
