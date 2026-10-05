# Effect's guide, and where this repository departs from it

[`repos/effect/LLMS.md`](../repos/effect/LLMS.md) is Effect's own guide for
agents, and `AGENTS.md` says to read it first. It describes defaults for a
typical Effect application. Where it disagrees with a decision recorded in this
repository — an ADR, or a reason written next to the code — **the repository
wins**. This note lists both halves so that nobody "fixes" the code back toward
the guide.

Checked against `effect@4.0.0` on 2026-10-03.

## Guidance this repository follows

| The guide says | Here | Where |
| --- | --- | --- |
| Import CLI, HTTP, SQL, and observability modules from their package entrypoints | `effect/cli`, `effect/http`, `effect/sql`, and `effect/observability`; the stable release removed the corresponding `effect/unstable/*` entrypoints | `packages/config/src/Settings.ts`, `packages/consumer/src/main.ts`, `packages/tracing/src/Metrics.ts` |
| A reusable function returning an Effect is `Effect.fn` / `Effect.fnUntraced`, not a function that wraps `Effect.gen` | `Effect.fnUntraced` — see below for why not the traced form | `makeRmq`, `makeWebhookSink`, `runDaemon`, `runProducer`, `ConsoleFrames.make`, `pollOne` |
| A pipe on such a function goes in as extra arguments, not `.pipe` | Each pipeable receives the effect and then the function's arguments | `pollOne` in `FleetSource.ts`: `(effect, replica) => Effect.catch(effect, …)` |
| A service with one implementation carries it as `static readonly layer`, and builds it with `Service.of` | `Aggregator.layer`, `EventBus.layer`, `Rmq.layer(opts)` | `Aggregator.ts`, `Events.ts`, `rmq/Client.ts` |
| Service identifiers name the package and file | `"@egress/aggregator/Events/EventBus"`, `"@egress/domain/Model/Config"` | every `Context.Service` and `Context.Reference` |
| Runtime type checks come from `Predicate`, not `typeof` | `Predicate.isString` | `rmq/Client.ts`, `Outbox.ts` |
| Parse untrusted text with `Schema`, not by hand | `Schema.fromJsonString(Schema.Unknown)`, then the message schema | `readerFor` in `domain/Model.ts` |
| Format instants with `DateTime` | `DateTime.formatIso(DateTime.makeUnsafe(ms))`, byte-identical to `toISOString()` | the published event in `Events.ts` |
| Output from an Effect program goes through `Console` | `Console.log`, not `Effect.sync(() => console.log(…))` | `demo/driver.ts`, `subscriber.ts` |
| Catch failures with `Effect.catch`; a defect is a bug | `Effect.catch` where only an expected failure should be absorbed — a request body that does not decode is a 400, but a defect in the handler is a 500, not a 400 blaming the caller | `/api/failure` and `/subscriber/webhook` in `Http.ts`, the demo driver's requests, `pollOne` |
| Absence of an optional value is `Effect.option`, not a catch-all | `Effect.option(Effect.currentSpan)` | `traceparent` in `rmq/Trace.ts` |
| Completing a `Deferred` is an effect | `Deferred.fail(fatal, new Fatal(…))`, not `Effect.sync(() => Deferred.doneUnsafe(…))` | `aggregator/main.ts` |

Two traps met while applying these:

- **A static layer is evaluated when the class is.** `Aggregator`'s implementation
  is defined below the class, so its layer is
  `Layer.effect(Aggregator, Effect.suspend(() => make))`. Referencing `make`
  directly throws at module load. `Rmq.layer` has no such problem, because it is
  a function that runs only when called.
- **`Schema.UnknownFromJsonString` is `@internal`** in this version
  (`Schema.ts:9496`). Use the public `Schema.fromJsonString(Schema.Unknown)`.
  Decoding in two steps (`read` in `packages/rmq-consumer/src/Negotiation.ts`)
  keeps a body that does not parse an answer, not an exception.

## Where the repository wins

| The guide says | This repository does | Because |
| --- | --- | --- |
| Define errors with `Schema.TaggedError` | `Data.TaggedError` — `RmqError`, `Halted`, `Rejected`, `Fatal` | Failures stay in the error channel as `Data.TaggedError`s, matched by class (`Settle.ts` reads `Halted` and `Rejected`). None of them crosses a process boundary, so a schema for their encoding would buy nothing. `RmqError` also overrides `message`, which `Data.TaggedError` prints. |
| Tracing-relevant functions use `Effect.fn("name")`, which opens a span | `Effect.fnUntraced`; spans are opened explicitly with `Effect.withSpan` at chosen boundaries | Spans are kept to chosen boundaries (`work.publish` in the producer), with sampling left to a collector at the tail. A span for every service function would be volume the collector discards. |
| Test with `@effect/vitest` and `it.effect` | `node:test` running Effect programs, with `TestClock` from `effect/testing` | The repository's test runner. `packages/aggregator/test/ConsoleFrames.test.ts` is a worked example. |
| `Effect.catch` rather than `Effect.catchCause` | `catchCause` in six places that deliberately absorb defects too | Each one guards the control loop or the delivery path from something outside it, and a comment says so: a sink must never stall a tick (`Events.ts`, `AmqpControlPlaneSink.ts`), an unreachable outbox must not fail a tick, a drain pass stops at its first failure of any kind, and a daemon's control handler logs and carries on (`daemon.ts`). Where a defect is caught, the cause is logged rather than dropped. |
| Prefer combinators and folds to loops | Four loops in `FleetSource.ts`'s simulator, and sequential `await` loops in `rmq/Client.ts` | The simulator mutates per-host ejection state across N simulated requests, and commit `df95b85` left it on purpose: a fold would move the mutation into a closure rather than remove it. The client's loops are inside amqplib's Promise callbacks, where order is the point. |
| Use `Clock` / `DateTime` for the current time | `Date.now()` in six calls | Each runs in an AMQP or gRPC callback with no fiber to read a `Clock` from, and says so in a comment: `EnvoyPushSource.ts`, `Redrive.ts`, the floor lease in `daemon.ts`. Every instant computed inside the control loop comes from `Clock`, which is what lets `TestClock` drive it. |

## Considered, not applied

- **`static readonly layer` on services with several implementations.**
  `FleetSource` has three (sim, Envoy polling, Envoy push), and `EventSink`,
  `Outbox` and the two Coordination services have two each. `main.ts` chooses
  between them from flags. The guide's static layer is a single default. The
  push implementation also lives in a module that imports `FleetSource`, so
  attaching it to the class would create an import cycle. These keep named
  layers.
- **`HttpApi` instead of `HttpRouter`.** The guide recommends it for
  schema-first APIs with a generated typed client. This server's routes are two
  SSE streams, Prometheus text, a static page and a demo-only failure switch.
  Their consumers are a browser, `curl`, Prometheus and a webhook. Nothing would
  use the typed client, and the streaming routes are the ones HttpApi models
  least directly.
