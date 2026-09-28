---
name: code-style
description: How the user wants code, comments and docs written on /workspace — simple Effect composition, essential comments, lean docs.
metadata:
  type: feedback
---

- **Effect composition over `if`/loops, but never convoluted.** Branch with `O.match`/`Result.match`/ternaries/
  `Effect.when`; iterate with `Arr.*`/`Effect.forEach`/recursion. If the functional version needs a helper the
  reader must look up, it went too far ("it's convoluted"). Plain code (`??`, optional chaining) beats Option
  wrapped around nullable interop state.
- Effect 4 (rc.117 on 2026-09-28): check every API against `repos/effect`, not memory.
- **Comments are essential only:** invariants, gotchas, why the obvious alternative is wrong, a pointer to an
  ADR. No history, measurements or narrative (that's git log). Past ~6 lines, cut to what a reader must know.
- **Docs stay lean:** a correction replaces the wrong sentence; reruns go to `docs/runs/` and are cited, not
  tabulated; point to the README rather than summarising it.
- Incident-report HTML: each `ol.limits li` needs exactly one child `<div>`; badges go inside it.

Related: [[working-style]].
