---
name: use-rtk-for-command-output
description: "Wrap shell commands with the rtk CLI (git, grep, pnpm, etc.) for condensed output, per CLAUDE.md's rtk-instructions block and the user's explicit ask on 2026-09-14."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 3db85e00-992d-4ea8-aca3-a5dac7e90039
  modified: 2026-09-14T10:33:04.595Z
---

Prefer `rtk <subcommand> ...` over the raw command when a matching one exists
(`rtk git`, `rtk grep`/`rtk rg`, `rtk pnpm`, `rtk find`, `rtk diff`, `rtk
test`/`rtk vitest`, `rtk gh`, `rtk log`, etc. — see `rtk --help` for the full
list). It's an installed CLI proxy (`/home/node/.local/bin/rtk`, config at
`.rtk/filters.toml`) that filters/summarizes output before it reaches context.

**Why:** CLAUDE.md carries an `rtk-instructions` block saying command output
is already condensed and to re-run as `rtk proxy <cmd>` only when a result is
unusable. The user separately said "use rtk" outright on 2026-09-14, i.e. reach
for the dedicated `rtk <subcommand>` wrapper proactively rather than waiting
for a bad result to trigger the fallback.

**How to apply:** For git/pnpm/grep/test/find/diff/gh commands, check `rtk
--help` for a matching subcommand and use it instead of the plain command.
Dedicated tools (Read, Edit, Grep) still win over both plain and `rtk`-wrapped
Bash where they fit — this is about which shell invocation to use when Bash is
the right tool at all.
