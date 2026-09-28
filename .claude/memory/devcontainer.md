---
name: devcontainer
description: "Traps in the /workspace devcontainer (host suspends, git subtree, docker exec, networking, restarts) and what a rebuild keeps or loses."
metadata:
  node_type: memory
  type: reference
  originSessionId: 5ae73c69-84bf-4950-b335-c824169d0fe2
  modified: 2026-09-28T07:33:24.110Z
---

- **The host suspends.** Timing runs spanning a sleep are corrupt; chaos scripts mark them VOID. Rerun.
- **`git subtree`**: not on PATH. Stash, `/usr/bin/git update-index -q --refresh`, then
  `/usr/bin/git subtree pull --prefix=repos/effect https://github.com/Effect-TS/effect.git effect@<ver> --squash`, pop.
- **Reach services by compose name** (`rabbitmq`, `prometheus:9090`, `grafana:3000`, `flaky-upstream:8080`);
  `localhost` ports don't reach them. The consumers are one scaled service (`workspace-rmq-consumer-1..5`).
- **`HOST_WORKSPACE_FOLDER`** comes from `/etc/environment` (the host path); exporting it as `/workspace` breaks
  the bind mounts. The Dockerfile lists package manifests by hand: a new package missing there fails at runtime.
- **`docker exec` truncates output after ~0.5 s** (process keeps running): write to a file and `cat` it.
- **`restart: unless-stopped` never fires after `docker kill` here**; restart explicitly.
- Prometheus doesn't hot-reload `prometheus.yml`; Grafana needs a restart for new panels.
- `pkill -f "<path>"` kills the Bash tool's shell (use `[s]ubscriber` patterns); never add `&` with
  `run_in_background`; the Bash tool caps at 600 s; no `bc`. npm has no network in the sandbox.
- A fresh RabbitMQ probed by `docker exec` as root dies on `.erlang.cookie`: use `-u rabbitmq`.
- Switching branches leaves ignored `node_modules` of packages the other branch has (e.g. `packages/consumer`).
- **Rebuilding the devcontainer** keeps `/workspace` (with this memory), the node_modules/pnpm volumes and
  everything on Docker Desktop; it loses `~/.claude` (transcripts, login, user settings). Afterwards
  `pnpm install --force` if native modules fail to load.

Related: [[working-style]].
