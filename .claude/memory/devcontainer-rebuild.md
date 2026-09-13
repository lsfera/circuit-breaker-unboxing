---
name: devcontainer-rebuild
description: What survives a rebuild of the /workspace devcontainer and what does not — checked 2026-09-13 before the move from a Debian 11 / Node 24 image to Debian trixie / Node 26.
metadata:
  type: reference
---

The devcontainer is defined outside this repo: a host checkout of
`devcontainer-vue`, which also supplies `/scripts/post-create.sh`. That script
chowns the volumes, runs `pnpm self-update && pnpm install`, and apt-installs
ripgrep and git. `/etc/environment` (HOST_WORKSPACE_FOLDER, TESTCONTAINERS_*,
NODE_VERSION) is written at creation from that config.

**Survives:**
- `/workspace` (bind mount), which includes `.claude/memory` — this memory
  directory, via `autoMemoryDirectory` in `.claude/settings.local.json`
- the volumes `devcontainer-vue_devcontainer_devcontainer-vue-node_modules`
  (`/workspace/node_modules`), `devcontainer-vue_devcontainer_pnpm-store`, and
  `vscode`
- `~/.ssh` keys (bind), and git identity, gpg and docker credential helpers,
  which Dev Containers re-injects from the host
- everything on Docker Desktop: the compose stack's data volumes, the
  `devcontainer` network, and images

**Lost** (home directory, not mounted):
- `~/.claude/projects/-workspace/*.jsonl` — session transcripts (53 MB on
  2026-09-13), which `--resume` and any article draw on
- `~/.claude/.credentials.json` and `~/.claude.json` — login; re-authenticate
- `~/.claude/settings.json` — `model: opus`, `effortLevel: high`,
  `agentPushNotifEnabled: true`
- `~/.agents/skills/find-skills` and its `~/.claude/skills` symlink — a
  user-level skill from vercel-labs/skills
- `~/.claude-mem` — a symlink to `/workspace/.claude-mem`, which does not exist,
  so the claude-mem plugin enabled in `.claude/settings.json` was already
  storing nothing
- the plugin cache, file-history, shell snapshots and bash history (disposable)

**After a rebuild:**
- `/workspace/node_modules` holds native builds for the old Node ABI and glibc,
  so run `pnpm install --force` if post-create's install leaves anything
  failing to load.
- `pnpm self-update` in post-create moves the host's pnpm past
  `package.json#packageManager` (11.23.0), which the image and CI pin.
- Recheck `git subtree` (see [[devcontainer-environment-gotchas]]).

Related: [[rmq-control-plane-design]].
