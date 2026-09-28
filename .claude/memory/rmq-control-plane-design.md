---
name: rmq-control-plane-design
description: "The egress breaker repo in /workspace (RabbitMQ fleet on amqplib) — how the user runs the work, the verification standard, and how to reach the stack. Points at the repo's docs rather than restating them."
metadata:
  node_type: memory
  type: project
  originSessionId: 431524e5-ab62-4373-9b9b-5ea98edcf3e7
  modified: 2026-09-13T00:00:00.000Z
---

**Built, running, verified live.** `packages/rmq`, `packages/rmq-consumer`,
`packages/rmq-producer`, and `packages/aggregator/src/AmqpControlPlaneSink.ts`.
`docker compose up` runs the whole scenario; the stack is usually already up in
this devcontainer.

**Read the repo, not this note.** `docs/rmq-control-plane.md` is the full
writeup and `docs/decisions/00{1..8}-*.md` are the decision records. Everything
this memory used to restate — the SAC elections, the two-connection workaround,
the four AMQP 1.0 client bugs — is either documented there or **no longer
true**: the client was replaced (`docs/decisions/004-downgrade-to-amqp-0-9-1.md`),
which deleted three workarounds, moved the redelivery budget into the queue
(`x-delivery-limit` on a quorum queue), and reduced each daemon to one
connection with channels that churn. Do not re-derive from an older description.

**How the user works on this repo**, which is the part not written down:

- The work runs as a series of requests, each landing as **exactly one commit**
  with a long narrative body: the finding, the measurement behind it, what was
  deliberately left alone, what was verified and what was not. Commit titles
  are a sentence naming the failure or the change ("A chaos check that could
  not hold, and an overview that restated the README").
- **Findings are measured, not asserted.** Before claiming a defect or an
  improvement, run it — against the pure reducer, a sim aggregator
  (`--source=sim --apis=1000 --replicas=10`), or the live stack — and say
  plainly what was not measured. Several commit bodies exist because a first
  reading was wrong and the measurement corrected it.
- **The verification standard** (article/04, 2026-09-28): `pnpm run check`
  (vendored-version check, typecheck, unit tests), `pnpm run test:rmq` (broker
  tests, Testcontainers), then rebuild the image and run
  `node infra/chaos-app.mjs` (add `--format=mixed` for JSON+protobuf); a
  scenario marked VOID (host suspended) is rerun alone. The site and diagram
  checks were removed with the prune (62732c02f), and mermaid/jsdom with them.
  `HOST_WORKSPACE_FOLDER` must stay as `/etc/environment` sets it — exporting
  it to `/workspace` breaks bind mounts.
- Containers run a **built image** (`egress-breaker:dev`), so source changes
  need `docker compose build aggregator` then `up -d`. The Dockerfile lists
  workspace manifests **one line per package, by hand**; a new package missing
  from that list fails as `ERR_MODULE_NOT_FOUND` at runtime, not at build.
- The compose stack is reachable from this session by service name
  (`http://aggregator:8088`, `http://prometheus:9090`, `rabbitmq`), because
  the services join the `devcontainer` network. The daemon fleet is one scaled
  service (`workspace-rmq-daemon-1..5`), not numbered services.

**State as of 2026-09-13**: ADRs 001–015 in `docs/decisions/`. The fleet targets
a published fraction (ADR 013), every compose service has resource limits
(ADR 014), and the console's frame is shared across browsers (ADR 015 steps 1–2).
Open work is in [[egress-breaker-open-threads]]. Related:
[[egress-breaker-session-record]], [[devcontainer-environment-gotchas]].
