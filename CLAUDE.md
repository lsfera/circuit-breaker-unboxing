# Repository guide for coding agents

This repository is a pnpm TypeScript monorepo demonstrating a circuit breaker
whose open state and delay are held by RabbitMQ. This branch is the
RabbitMQ-only breaker version; other `article/*` branches are separate
snapshots, so do not assume their package layout or implementation matches this
checkout.

## Structure

| Package | Responsibility |
| --- | --- |
| `packages/rmq` | Effect wrapper around `@cloudamqp/amqp-client`, queue conventions, delayed delivery, message contracts and negotiation |
| `packages/rmq-consumer` | Consumer SDK: per-dependency breaker, probe permit, redrive, parking and metrics |
| `packages/rmq-producer` | Publisher SDK: publishes a contract's messages to its exchange, encoded, with ids, in confirmed batches; knows no queue, never reads breaker state |
| `packages/contracts` | The example message contracts, declared once for producer and consumer |
| `packages/producer` | Example load generator using the publisher SDK |
| `packages/consumer` | Example application using the consumer SDK |
| `packages/config` | Shared settings decoded at process startup |
| `packages/tracing` | OpenTelemetry tracing and Prometheus metrics |

The main flow is producer → contract exchange → RabbitMQ work queue → competing consumer fleet →
upstream service. In this branch a closed breaker consumes work; an open
breaker stops its consumer and leaves a wake token in the broker's TTL
dead-letter delay chain; the returning token permits a single-message probe.

## Development

- Requirements: Node.js 26+, pnpm 12.8.1, and Docker for container-backed
  integration tests.
- `pnpm run check` fetches the pinned Effect source, checks vendored package
  versions, typechecks, and runs unit tests.
- `pnpm run test:rmq` runs RabbitMQ integration tests and requires Docker.
- Tests use Vitest (`vitest.config.ts`). `test` and `test:rmq` have `:bun`
  and `:deno` variants; `test:runtimes` runs the unit suites on all three.
- `pnpm run format` formats the workspace with dprint.
- `pnpm run incident` runs the local outage scenario; see the root
  `README.md` for the Docker Compose environment.

## Working conventions

- Read [`AGENTS.md`](AGENTS.md) before changing Effect code. The Effect catalog
  in `pnpm-workspace.yaml` is the version source of truth; consult the fetched
  `repos/effect` guide, source, and tests rather than relying on Effect 3
  examples or APIs from another release.
- Effect 4.0.1 exposes modules through entrypoints such as `effect/cli`,
  `effect/http`, and `effect/observability`. Follow the existing imports and
  verify any uncertain API against the pinned source.
- `repos/effect/` is fetched reference material, ignored by Git: read it, but
  do not edit it or import application code from it.
- Preserve the queue and breaker semantics described in the root `README.md`
  and `docs/rabbitmq-held-breaker.md`; update tests when behavior changes.
- Do not add `Co-authored-by` trailers to commits in this repository.
