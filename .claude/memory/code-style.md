---
name: code-style
description: "How the user wants code, comments and docs written on /workspace — simple Effect composition, essential comments, lean docs."
metadata:
  node_type: memory
  type: feedback
  originSessionId: 5ae73c69-84bf-4950-b335-c824169d0fe2
  modified: 2026-09-28T07:33:07.560Z
---

- **Effect composition over `if`/loops, but never convoluted.** Branch with `O.match`/`Result.match`/ternaries/
  `Effect.when`; iterate with `Arr.*`/`Effect.forEach`/recursion. If the functional version needs a helper the
  reader must look up, it went too far ("it's convoluted"). Plain code (`??`, optional chaining) beats Option
  wrapped around nullable interop state.
- Effect 4 (rc.116 on this branch, rc.117 on 04/05, 2026-09-28): check every API against `repos/effect`, not memory.
- **Comments are essential only:** invariants, gotchas, why the obvious alternative is wrong, a pointer to an
  ADR. No history, measurements or narrative (that's git log). Past ~6 lines, cut to what a reader must know.
- **Docs stay lean and true:** a correction replaces the wrong sentence; check every number and name in a doc
  against the code; counts that drift (files, tests) are approximate or dated; point to the README rather than
  summarising it. READMEs follow article/04's style (dense opening, amber "new" mermaid class, tables,
  compact "Running it", footer with the Effect version and check commands).

Related: [[working-style]].
