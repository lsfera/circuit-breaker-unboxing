---
name: essential-comments-not-narrative
description: "The user wants code comments to be essential only — no history, no measurements, no war stories. That material belongs in git log and docs/decisions."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: de259c96-c20a-4b26-ab25-0b5d99a60c8d
  modified: 2026-09-07T21:37:08.548Z
---

Stated directly on 2026-09-07, reviewing /workspace: *"time to review all bloated
code comments. Be essential, if I want the story I read git log."*

**Keep in a comment**: an invariant, a constraint that would cause a bug if
violated, a gotcha the code cannot express, why an obvious alternative is wrong,
and a pointer to the ADR that holds the reasoning. Prefer one or two sentences.

**Do not keep**: what the code used to be, what a past bug was, measured numbers
from an investigation, a restatement of the code, or essay-length rationale.
Those go in the commit body and `docs/decisions/`, both of which this repo
already uses heavily — the comment can link to them instead.

**Why**: comments are read while changing the code, and narrative crowds out the
one line that would have prevented the change from being wrong. It also goes
stale, which git log cannot.

**How to apply**: when a doc block runs past ~6 lines, ask what a reader *must*
know to change this safely and cut to that. A stale hand-maintained count in
prose ("Three things worth knowing" above four) is the same defect as any other
pair of things that must agree — either delete the count or delete the drift.

This trimmed /workspace from 31% comment lines to 24% (1,049 lines) without
touching behaviour. Applies to my writing in this repo by default, and is worth
assuming elsewhere unless the user says otherwise.

Related: [[rmq-control-plane-design]].
