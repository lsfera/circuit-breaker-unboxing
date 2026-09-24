---
name: incident-report-limits-grid-gotcha
description: The ol.limits li CSS grid in incident-report artifacts breaks if a list item has more than one direct child div
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5ee1dde4-09c2-4ea3-8453-6ae6c140ccbc
  modified: 2026-09-18T20:20:10.543Z
---

In the incident-report HTML template (reused across article branches' "what
this design cannot overcome" sections), `ol.limits li` is
`display:grid; grid-template-columns:36px 1fr` with the counter number as an
`::before` pseudo-element. Each `<li>` must have **exactly one** direct
child `<div>` wrapping everything else (`<b>`, `<span class="why">`, any
badge/tag) — a second sibling div (e.g. a "CARRIED FROM ARTICLE N" tag)
overflows the grid's column definition and produces broken layout: text
wrapping into the narrow 36px counter column instead of the wide content
column.

**Why:** Bit twice — once in the article-1 incident report (a tag/description
split across two children) and again in article-2's report (a "carried
forward" badge added as its own sibling div). Both times the fix was
identical: put every extra element (badges included) *inside* the single
content div as an inline element, never as a second `<li>` child.

**How to apply:** Before adding any new element to an `ol.limits li` in this
template — a badge, an icon, a second line — nest it inside the existing
content `<div>`, and check rendered output once before publishing if the
`<li>` structure changed at all.
