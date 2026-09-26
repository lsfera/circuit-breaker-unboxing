---
name: consumer-sdk-design
description: "How the user shaped the consumer SDK (2026-09-25/26, branch article/02-rabbitmq-only-breaker) — the API decisions they made one by one, and what the exercise exposed."
metadata:
  node_type: memory
  type: project
  originSessionId: 1ac9cdcc-2897-4209-9932-2908741af021
  modified: 2026-09-26T11:45:11.379Z
---

`@egress/rmq-consumer` became an SDK; `packages/consumer/src/main.ts` is the application, importing only the SDK
(+ `effect`, `@effect/sql-pg`). The user drove the API decision by decision during planning:

- `Consumer.For(schema, negotiate).bind(action, [deps])` → a registration; `Consumer.run({ consumers, flags, layer })`.
  They asked for `For` (A inferred from the schema), `.bind(...).run()`, the action as an Effect over
  `(payload, metadata)` with **no idempotency key in the SDK**, generics, convention over configuration.
- **Explicit, not implied**: content negotiation has no default (`accept(parsers, { undeclared, type })`; they
  kept `undeclared` as opt-in leniency). "It must be explicit" was their words on a default media type.
- **No HTTP in the SDK**: they rejected exposing an `Http` helper ("confusing"); classifiers live in the app.
- **One breaker per dependency** (`Consumer.Dependency(name, { classify, timeout })`); several (contract,
  consumer) pairs per app; the app plugs services via `layer`; SDK `flags`/`command`/`launch` exported so the
  app can add its own flags (they asked for this mid-implementation).
- **Breaker settings per dependency, in code** (2026-09-26): `Dependency(name, { classify, breaker: {...} })`,
  `BREAKER_*` flags are the defaults. They asked "help me decide" between code / runtime-by-name / both; code
  was recommended (what differs is the dependency's kind; a runtime override can be layered on later).
- Postgres ledger in compose, assumed schema (they struck `CREATE TABLE` from the app), and chaos for the app
  (`infra/chaos-app.mjs`, 8 scenarios) as well as the regression suite.

**Why:** "a great way to verify how good is the design". **How to apply:** keep the SDK surface lean and
explicit; surface design friction found by writing the app (repeat charges when step 2 fails after step 1,
probes running whole actions, contract declared twice, shared MAX_IN_FLIGHT). Any SDK change touching
probes: test with a dependency two consumers share, over an outage long enough for holds to grow.

Related: [[rabbitmq-only-breaker-variant]], [[chaos-test-new-components]], [[effect-functional-style]].
