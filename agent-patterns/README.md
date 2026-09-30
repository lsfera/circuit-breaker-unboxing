# Agent patterns

Notes on the parts of Effect this codebase actually uses, written by reading
the vendored source in [`repos/effect`](../repos/effect) — implementation,
tests and migration guides — rather than from memory or the web.

They are a project-local digest, not a copy of the documentation. Each one
covers what this repository leans on, cites the files and lines it was
derived from, and records the mistakes that have been made here, so the next
reader starts from the correction.

| | For |
| --- | --- |
| [effect-guide-in-this-repo.md](effect-guide-in-this-repo.md) | Effect's own agent guide (`repos/effect/LLMS.md`) applied to this code: what it follows, and where an ADR overrides the guide |
| [effect-pubsub-and-streams.md](effect-pubsub-and-streams.md) | Fan-out, back-pressure and buffering: which PubSub to use, the unbounded buffer inside `SubscriptionRef`, and testing streams under `TestClock` |

**They go stale with the pin.** The line numbers are for
`effect@4.0.0-rc.117`, the tag `repos/effect` was fetched from. When the
pin moves — `pnpm run check:vendored` fails until it does — regenerate a
note from the new source rather than trusting it:

> Review the implementation, tests and migration notes for `<module>` in
> `repos/effect`, and rewrite `agent-patterns/<file>.md` for this version,
> keeping the repository-specific findings that still hold.
