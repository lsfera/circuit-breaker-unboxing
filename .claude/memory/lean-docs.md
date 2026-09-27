---
name: lean-docs
description: README and docs/ write-ups stay lean — fixes replace text rather than add to it; reruns and history go to docs/runs and git log.
metadata:
  type: feedback
---

"keep it lean" (2026-09-27), right after a doc review whose fixes added a paragraph per finding to
docs/rabbitmq-held-breaker.md. The trimmed version cut ~100 lines: a rerun table that only said "passed
again", self-narration ("I reasoned that out…"), panel inventories, and pointers that restated the README.

**Why:** the user prefers fewer, denser articles and lean READMEs; a doc that grows with every correction
turns into a changelog. **How to apply:** when correcting a doc, replace the wrong sentence with a shorter
right one; cite a run file instead of tabulating a rerun; one-line pointers to the README, not summaries of
it. Same spirit as [[essential-comments-not-narrative]]; see [[article-series-branches]].
