---
name: devcontainer-environment-gotchas
description: "Traps in the /workspace devcontainer that have silently cost time — host suspends corrupting timing runs, git subtree missing, pkill killing its own shell, RabbitMQ and cgroups."
metadata: 
  node_type: memory
  type: reference
  originSessionId: d23c6dbd-5c0e-471a-a8fc-5ed6afc8c7c7
  modified: 2026-09-13T23:28:17.256Z
---

Facts about this environment, each learned by losing time to it (2026-09-12/13):

- **The host suspends.** Long runs sometimes span a laptop sleep: an
  `infra/instrument.mjs` run recorded 2,169 s of wall clock inside a 300 s
  `timeout` (which uses a monotonic clock). Any timing or CPU window can be
  corrupted this way. Check a record's `seconds` against the expected duration
  before quoting it, and discard the run if they disagree.
- **Node 25+ ships no corepack.** The Dockerfile installs pnpm with
  `npm install --global "$(node -p 'require("./package.json").packageManager')"`.
- **An attached `docker run` can drop a short-lived container's output.** A test
  run can exit 0 having printed nothing. Run it detached, `docker wait`, then
  `docker logs`.
- **`git subtree` is not on PATH.** The PATH git is 2.55 in `/usr/local`, without
  contrib. The system copy works: `/usr/lib/git-core/git-subtree pull
  --prefix=repos/effect https://github.com/Effect-TS/effect.git effect@<ver> --squash`.
- **`pkill -f "<path>"` kills the Bash tool's own shell**, because the shell's
  command line contains the pattern (exit 144). Use a bracket pattern like
  `pkill -f "[s]ubscriber/src/subscriber.ts"`.
- **Finding a node process by pid:** under Node 24 its `/proc/<pid>/comm` is
  `MainThread`, not `node`, and `pgrep -f` also matches the wrapping `bash`.
  Filter with `comm != bash`.
- **Bash tool timeout caps at 600 s.** Longer requests are clamped and the
  command moves to the background. Split long measurements, e.g.
  `scripts/measure-console-stream.mjs --parts=2`.
- **RabbitMQ (4.0.9 and 4.3.5) ignores the container memory limit** under
  cgroup v2 and sizes its watermark from host RAM (48 GiB). That is why `infra/rabbitmq.conf`
  declares it. `RABBITMQ_VM_MEMORY_HIGH_WATERMARK` makes the image refuse to
  start.
- **`rabbitmqctl` prints a banner before its table header.** Counting its output
  lines miscounted connections twice.
- **Garbage-collected processes size their heaps from visible memory.**
  Unlimited containers used 28–52% more RSS than limited ones for the same work,
  so memory readings from unlimited processes are upper bounds (ADR 014).

- **Probing a fresh RabbitMQ container with `docker exec` as root** writes a
  root-owned `.erlang.cookie` before the server does, and the broker then dies
  with `eacces`. Use `docker exec -u rabbitmq`.
- **RabbitMQ 4.3 behaviour changes that bit this repo:** transient non-exclusive
  queues are refused by closing the connection, and a requeuing `basic.nack`
  no longer counts toward `x-delivery-limit` (a `basic.reject` does).
- **Prometheus 3 normalizes `le`** (`1048576` → `1.048576e+06`, `Infinity` →
  `+Inf`), so queries spanning v2 and v3 data briefly see duplicate buckets.

- **Quorum queues (RabbitMQ 4) default to `x-delivery-limit` 20, and a queue with no
  dead-letter target silently DROPS at the limit** (`dead_letter_strategy="disabled"`
  counter). A consumer channel closing with the message unacked counts as a return.
  Terminal queues need `x-delivery-limit: -1`. Measured with temp queues, 2026-09-13.
- **Changing a queue's arguments** needs the queue deleted (`DELETE` without
  `if-empty` — quorum queues reject that with 400); daemons meanwhile hang at
  declare silently rather than crash.
- **`consumer_capacity` reads 0 for every quorum queue** — useless as a stall signal.
- **Host suspend shows as clock skew:** `Date.now() - performance.timeOrigin -
  performance.now()` grows only across a sleep; chaos-load.mjs voids such runs.
  After a wake RabbitMQ may close consumer channels ("ack timed out, -1 ms").
- **No `bc` in the devcontainer.** Docker bind mounts need host paths — pass Envoy
  configs to `--mode validate` via `--config-yaml "$(cat …)"`, not a /tmp mount.
- **netshoot (`nicolaka/netshoot:v0.14`) with `--net container:X --cap-add NET_ADMIN`**
  works for tc netem / iptables; every container has two IPs (default + devcontainer).

Related: [[rmq-control-plane-design]].
