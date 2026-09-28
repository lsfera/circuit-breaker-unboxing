---
name: devcontainer
description: Traps in the /workspace devcontainer (host suspends, git subtree, docker exec, networking, restarts) and what a rebuild keeps or loses.
metadata:
  type: reference
---

- **The host suspends.** Timing runs spanning a sleep are corrupt; chaos scripts mark them VOID. Rerun.
- **`git subtree`**: not on PATH. Stash, `/usr/bin/git update-index -q --refresh`, then
  `/usr/bin/git subtree pull --prefix=repos/effect https://github.com/Effect-TS/effect.git effect@<ver> --squash`, pop.
- **`docker exec` truncates output after ~0.5 s** (process keeps running): keep exec'd commands fast or write to
  a file and `cat` it.
- **Reach services by compose name** (`rabbitmq`, `prometheus:9090`, `postgres`); `localhost` ports don't reach
  them. Ledger: `docker exec workspace-postgres-1 psql -U postgres -d ledger`.
- **`restart: unless-stopped` never fires after `docker kill` here**; restart explicitly.
- Prometheus doesn't hot-reload `prometheus.yml`; Grafana needs a restart for new panels.
- `pkill -f "<path>"` kills the Bash tool's shell (use `[s]ubscriber` patterns); never add `&` with
  `run_in_background`; the Bash tool caps at 600 s; no `bc`.
- A fresh RabbitMQ probed by `docker exec` as root dies on `.erlang.cookie`: use `-u rabbitmq`.
- **Rebuilding the devcontainer** keeps `/workspace` (with this memory), the node_modules/pnpm volumes and
  everything on Docker Desktop; it loses `~/.claude` (transcripts, login, user settings). Afterwards
  `pnpm install --force` if native modules fail to load.

Related: [[working-style]].
