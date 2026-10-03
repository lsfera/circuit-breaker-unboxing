# Agent instructions

## Effect

This repository uses **Effect 4** — `effect@4.0.0`, pinned in
`pnpm-workspace.yaml`'s catalog. Most Effect material in circulation, and
most of what a model remembers, is Effect 3, which Effect 4 renamed and
reshaped substantially. Before writing Effect code:

1. Read [`repos/effect/LLMS.md`](repos/effect/LLMS.md) completely — Effect's
   own guide for agents, for exactly this version.
2. Read [`agent-patterns/effect-3-to-4.md`](agent-patterns/effect-3-to-4.md)
   for the v3 idioms that have already come up here, and what they are now, and
   [`agent-patterns/effect-guide-in-this-repo.md`](agent-patterns/effect-guide-in-this-repo.md)
   for where this repository departs from the guide in step 1 — where the two
   disagree, the repository's decision wins.
3. For any API you are not certain of, read its source and its tests in
   `repos/effect` rather than guessing. The tests show behaviour the
   docstrings do not.

## Vendored repositories

This project keeps external repositories under @repos/ as reference. They are
not committed: run `pnpm run fetch:effect` after cloning (`pnpm run check`
runs it too).

- Use vendored repositories as read-only reference material when working with
  related libraries.
- Prefer examples and patterns from the vendored source code over generated
  guesses or web search results.
- Do not edit files under @repos/ unless explicitly asked.
- Do not import from @repos/ — application code keeps importing from normal
  package dependencies.

`repos/effect` is `Effect-TS/effect` at the tag `effect@4.0.0`,
fetched by `scripts/fetch-effect.mjs` as a shallow clone of that tag. It is
**the installed version, not `main`**: `main` runs ahead of the releases (it
was two release candidates ahead when this was first vendored), and reference
material for a newer version describes APIs this code cannot use.

| Installed package | Source | Tests |
| --- | --- | --- |
| `effect` | `repos/effect/packages/effect/src` | `repos/effect/packages/effect/test` |
| `@effect/platform-node` | `repos/effect/packages/platform/node/src` | `…/platform/node/test` |
| `@effect/opentelemetry` | `repos/effect/packages/opentelemetry/src` | `…/opentelemetry/test` |
| v3 → v4 renames | `repos/effect/migration/v3-to-v4.md`, `repos/effect/MIGRATION.md` | |
| Worked examples | `repos/effect/ai-docs/src` | |

**When sources disagree, the vendored code wins** — over a skill, a blog
post, or memory. This is not hypothetical: a project skill taught four Schema
and DateTime APIs that do not exist in this version, and was deleted for it
(see `agent-patterns/effect-3-to-4.md`).

**Searching.** `repos/` is gitignored, so searches that honour `.gitignore`
(ripgrep, `git grep`) skip it. Search `repos/effect` deliberately, by naming
the path.

**Updating.** The fetched copy follows the catalog's `effect`. `pnpm run check`
fetches the tag for it, then fails if any `effect` or `@effect/*` dependency
differs from the fetched package of the same name. To move:

```bash
# 1. bump every Effect version in pnpm-workspace.yaml's catalog (and any direct
#    pins), then `pnpm install`
# 2. `pnpm run check` — it replaces repos/effect with the new tag
# 3. regenerate the agent-patterns notes the release touched
```

## File names

Effect's own convention (`Effect.ts`, `Schema.ts`): a module is named after
its subject, in PascalCase — `Breaker.ts`, `Verdict.ts`, `Client.ts`,
`ControlPlane.ts`. Lowercase is for a process's wiring only: `main.ts`, the
module named after its package holding the run function `main.ts` launches
(`consumer.ts`, `producer.ts`, `aggregator.ts`), and test helpers
(`harness.ts`). A file that other modules import from is PascalCase.

## Agent patterns

[`agent-patterns/`](agent-patterns/) holds notes derived from the vendored
source for the parts of Effect this codebase leans on. Each cites the files
and lines it came from, and line numbers are for the pinned tag. When the pin
moves, regenerate a note from the new source rather than trusting it.
