# Plan: a plain JavaScript SDK beside the Effect SDK

Status: proposed. Nothing here is implemented yet.

## Goal

Two ways to use the same SDKs:

| Entry point | For | Returns | Contracts from |
| --- | --- | --- | --- |
| `@egress/rmq-producer` (as today) | Effect and TypeScript users | `Effect` | Effect `Schema` |
| `@egress/rmq-producer/promise` | plain JavaScript, or TypeScript without Effect | `Promise` | any [Standard Schema](https://standardschema.dev) (Zod, Valibot, ArkType, Effect Schema) |

The consumer gets the same pair later (`@egress/rmq-consumer/promise`).

The Effect SDK stays the core. The Promise entry point is a thin adapter over it and holds no logic of its own, so topology, ids, confirms, alarm handling and metrics are written once.

**Out of scope:** a callback API, CommonJS output, and browser support. The SDK speaks AMQP over TCP.

## Where things stand

- **Not installable.** Every package is `private`, and its `exports` point at TypeScript source (for example `"." : "./src/index.ts"`, `"./Client.ts": "./src/Client.ts"`). That works inside this repo because Node 26, Bun and Deno strip types at run time. It does not work once the SDK is installed as a dependency: Node refuses to strip types under `node_modules` (`ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`).
- **Ready to emit.** The tsconfig can already produce JavaScript. `rewriteRelativeImportExtensions` turns `./Client.ts` into `./Client.js`, and `erasableSyntaxOnly` keeps the source strip-only.
- **Package-to-package imports.** These name `.ts` subpaths (`@egress/rmq/Client.ts`). tsc does not rewrite package specifiers.
- **Dependency graph.** `rmq-producer` and `rmq-consumer` depend on `@egress/rmq`, `@egress/config`, `@egress/tracing`, `effect` and `@effect/platform-node`. All five `@egress` libraries would have to be published.
- **The amqp-client patch.** `patches/@cloudamqp__amqp-client@4.1.1.patch` (the unhandled confirm rejection when the broker shuts down) is applied by pnpm in this workspace only. An installed SDK would get the unpatched client, and a broker shutdown during a publish could crash the user's process.
- **Standard Schema in Effect.** Effect 4's `Schema.toStandardSchemaV1` exists, so an Effect Schema can also be used as a Standard Schema.

## Decisions

1. **One package per SDK, two entry points.** We don't add separate `-promise` packages: both entry points come out of one build and are released together.
2. **`effect` becomes a `peerDependency`** of every published `@egress` package. An Effect user's application has to share one copy of `effect` with the SDK, or layers, schemas and the tracer won't match. npm 7+ and pnpm install peers automatically, so plain JavaScript users don't notice.
3. **One contract type.** `Contract.make` keeps taking an Effect `Schema`. A new `contract()` takes any Standard Schema. Both produce the same `Contract`, which works with either entry point and with the consumer.
4. **Standard Schema contracts validate but don't transform.** Standard Schema only validates, so a contract built from Zod can't change a value as it's written. A `Date` stays a `Date` for the format to serialise. Effect Schema contracts keep their two-way codecs. This difference is documented, not hidden.
5. **The repo keeps running source.** Each `exports` entry gets a `source` condition that points at the `.ts` file. Vitest, the example apps and Docker resolve it with `--conditions=source`. Installed copies resolve `default`, the built JavaScript.
6. **Subpaths drop the extension.** `@egress/rmq/Client.ts` becomes `@egress/rmq/Client`, everywhere in the repo, in a single change.

## Phases

### 0. Prerequisites

- **amqp-client fix.**
  - Send the confirm-rejection fix upstream to `@cloudamqp/amqp-client`.
  - Until a release ships it, contain the problem in `@egress/rmq` itself, without relying on a patched dependency: attach the rejection handler before the write completes, in our own `publish` path.
  - The existing test "a broker shutdown with publishes still being written fails them, not the process" has to pass without the pnpm patch.
- **Names.** Confirm the npm scope (`@egress` or another) and the licence before anything is published.

**Done when:** the client integration suite passes with the patch removed.

### 1. Build and exports

- A `build` script per published package (`rmq`, `config`, `tracing`, `rmq-producer`, `rmq-consumer`): `tsc -p tsconfig.build.json` writes `dist/*.js`, `.d.ts` and source maps.
- `exports` per entry point:
  ```json
  "./Client": { "source": "./src/Client.ts", "types": "./dist/Client.d.ts", "default": "./dist/Client.js" }
  ```
- Rename the subpath imports across the repo (decision 6).
- Move `effect` and `@effect/platform-node` to `peerDependencies`.
- Add `files: ["dist"]`, drop `private`, and set `version` and `license`.
- Pass `--conditions=source` to Vitest, the example apps' `start` scripts, the Docker `CMD`s and the Compose commands, and use Deno's equivalent.

**Tests:**
- `pnpm run check` and `test:rmq` still pass on Node, Bun and Deno.
- A new `test:pack` CI job runs `pnpm pack` on each package and installs the tarballs into an empty Node project. That project imports the Effect SDK from JavaScript and from TypeScript (`tsc --noEmit` against the shipped `.d.ts`) and publishes one message to a throwaway broker.

**Done when:** `test:pack` passes on Node, Bun and Deno.

### 2. Contracts from Standard Schema

- Add `Contract.contract({ schema, exchange, formats, type?, undeclared?, route? })`, which accepts any Standard Schema. Effect Schemas are detected and keep their codec.
- Internally a contract keeps one validate-and-encode step and one decode-and-validate step, whatever schema built it.
- Add plain format helpers: `json` (UTF-8 JSON in both directions) and `bytes({ decode, encode })`, which already takes plain functions.

**Tests:**
- A Zod contract and an Effect Schema contract of the same shape round-trip the same messages.
- A value the schema refuses never reaches the encoder.
- A contract written with Zod can be consumed by an Effect consumer.

### 3. The Promise publisher (`@egress/rmq-producer/promise`)

```js
import { batch, connect, contract, json, one, PublishError } from "@egress/rmq-producer/promise";

const rmq = await connect({ host, port, vhost, username, password, tls, name });
const payments = await rmq.publisher(Payment, { format, routingKey, headers, mandatory });
const id = await payments.publish(one(message), { signal });   // Promise<string>
const ids = await payments.publish(batch(messages));           // Promise<string[]>
rmq.lost;                                                      // settles if recovery gives up
await rmq.close();
```

- **Runtime:** `connect` builds the `Rmq` layer on a managed runtime, and `close` disposes it, which closes both connections. Each call runs one effect on that runtime.
- **Errors:** a call rejects with the existing `PublishError`, an `Error` subclass, whose `reason._tag` is `ContractRefused`, `Unroutable` or `BrokerFailed`. A defect, such as a `routingKey` beside a contract's route, rejects as a plain `Error`.
- **Cancellation:** an `AbortSignal` interrupts the call. A publication already handed to the broker may still arrive, and its ids are on the rejection, as with any failure.
- **Lifecycle:** no CLI `run` and no flags. A lost connection settles `rmq.lost`, and the user's application decides what to do; the process isn't ended for them.
- **Types:** the `/promise` declarations mention no Effect type. `Option` becomes `undefined`.
- **Tracing:** if `@opentelemetry/api` is installed and a span is active, it parents `work.publish`. Without it, `work.publish` is a root span, as now.
- **Metrics:** `egress_producer_failed_total` is recorded on the Effect side as now. The Promise entry point adds a `metrics()` that returns the Prometheus text for an application to serve, because there is no CLI to serve `/metrics` for it.

**Tests:** a new integration file runs the existing publisher scenarios through `/promise`:
- the main path for `one` and `batch`;
- each failure reason rejecting as `PublishError`;
- `close`;
- an `AbortSignal` cancelling a held publish during a broker alarm;
- `lost` settling once recovery gives up.

The JavaScript example in `test:pack` runs the same path.

**Done when:** those pass on all three runtimes, and the README has a plain JavaScript section.

### 4. The Promise consumer (design first)

The consumer is larger than the publisher. A handler becomes `(message, meta, { signal }) => Promise<void>`, but the SDK still needs to tell a failure from a throttle from a client error, per dependency. Before writing code:

- Write a short design note for the plain-object `Dependency` description: name, timeout, a classifier from error or status to outcome, and breaker and limiter settings.
- Decide how a handler names the dependency a call goes to, for example `deps.thirdParty(() => fetch(...))`.
- Review the note before implementing. The breaker, probe permit, redrive and parking stay unchanged underneath.

### 5. Release

- **Versioning:** Changesets (or similar), one version line for all published packages.
- **CI:** publish on tag, after `check`, `test:rmq` and `test:pack` pass on all three runtimes.
- **README:** the Effect and plain JavaScript usage side by side, and the contract difference from decision 4.

## Risks and open questions

- **Duplicate `effect` copies.** An application whose `effect` version is outside the peer range would get two copies, and with them layers that don't match. Mitigation: a broad, tested peer range and a clear install error.
- **The amqp-client fix.** If upstream doesn't take it, we carry the containment in our own code for good (phase 0). We can't ship a patched dependency.
- **The source condition in containers.** Every Docker and Compose command gains a flag. One that's missed fails at start-up, not silently, but each runtime has to be checked.
- **Tracing context.** Bridging the caller's `@opentelemetry/api` context into the Effect tracer needs a spike before phase 3 commits to it.
- **Scope of the first release.** The publisher could ship after phase 3 without waiting for the consumer, or both together. To be decided before phase 5.
