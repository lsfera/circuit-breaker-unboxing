# Effect, and what it actually bought

Not ceremony — four concrete things:

- **`TestClock` replaced a shell script.** Verifying open → probe → backoff →
  reopen → close used to mean `sleep 6` between `curl`s, which was slow and
  flaky. The tick loop is `Effect.repeat(Schedule.spaced(...))` rather than
  `setInterval`, so tests drive 100 seconds of simulated time in milliseconds,
  deterministically.
- **Retry policy became a value.** `WebhookSink` delivery is
  `Effect.retry({ schedule: Schedule.exponential("100 millis"), times: 3 })`
  instead of a hand-rolled loop with a counter, a sleep and a try/catch.
- **`PubSub` replaced manual subscriber bookkeeping.** SSE clients are Streams
  that end with the request scope; there is no `Set` of response objects to
  clean up and no `clearInterval` to remember.
- **Two bugs surfaced.** Type-checking caught the Envoy stats parser indexing
  possibly-undefined regex groups under `noUncheckedIndexedAccess`. `TestClock`
  caught the aggregator publishing `observedSince: "1970-01-01T00:00:00.000Z"`
  for any API that had not yet changed state — a real wire-format bug that
  wall-clock time had been hiding. `Breaker.initial` now seeds from the real
  clock, and the test anchors `TestClock` to a realistic instant so epoch leakage
  stays detectable rather than becoming a legitimate value.

The state machine itself is *not* written in Effect, and that is the point.
Effect earns its place at the boundaries.

## Option, and where absence is allowed to be a `null`

The rule and the full inventory are in
[decisions/006](decisions/006-representing-absence.md); this is the practical
half — what it looks like in the source, and what the RC's API does not have.

The line is between absence as a *result* and absence as *structure*:

- **`Option`** when a caller has to ask "is there one?" — return types, values
  in a `Ref`, fields of a domain record. `Breaker.step` returns
  `[BreakerState, O.Option<Transition>]`, and the daemon holds
  `Ref<O.Option<Consumer>>` for each of its three churning channels.
- **`undefined`** when absence is structural rather than semantic: `Map.get`,
  memoisation sentinels (`let cached: T | undefined` means *not computed yet*,
  which is not the same as *computed and empty*), and optional fields in an
  options bag, where the caller omits a value rather than constructing an
  absence.
- **`null`** only where something outside the repo insists — `Schema.NullOr` on
  the published event, ioredis's `eval` signature, amqplib's `ConsumeMessage |
  null`, gRPC's `ServiceError | null`. Converted at the boundary, never carried
  further in.

Three things worth knowing before writing any of it:

- **There is no `Option.fromNullable`.** This version has `fromNullishOr`,
  `fromUndefinedOr` and `fromNullOr`. `O.fromUndefinedOr(map.get(k)?.field)` is
  the usual shape at a lookup boundary.
- **`O.toArray` is the filter-map.** `xs.flatMap((x) => O.toArray(parse(x)))`
  drops what did not parse, with no `.filter` and no type predicate to keep in
  step with it.
- **`O.match` reads better than `isSome` in a pipeline**, and `O.isSome` reads
  better than `O.match` in a guard. Both are used here, on that basis.

`Option` is imported as `O` everywhere. It is a value *and* a type
(`O.Option<T>`), and both spellings existed in this repo before they were made
one.

## Effect 4 RC, read from the `.d.ts` files

The published migration write-ups describe beta.5 and the API has moved since,
so these were taken from the installed types rather than from summaries:

- `ServiceMap` is `Context` again — the blog would have sent you into a wall
- `Effect.fork` is gone: `forkChild` / `forkScoped` / `forkIn` / `forkDetach`
- `Effect.zipRight` → `Effect.andThen`
- `Stream.repeatEffect` → `Stream.fromEffectSchedule`
- `HttpServerResponse.json` returns an `Effect` (encoding can fail);
  `jsonUnsafe` is the plain constructor
- `Schema.decodeUnknownEffect` does not exist; only `decodeUnknownOption` takes
  `unknown`
- `Effect.catchAll` is `Effect.catch` — and `catchCause` is *not* the
  drop-in it looks like, because it swallows defects as well as failures
- `Schema` is core now, not `@effect/schema`
- There is no `Runtime.runFork(runtime)` to bridge a callback back into the
  fiber tree the way v3 allowed; a `Queue` the callback writes to and a fiber
  that drains it is the shape that replaces it

`tsconfig.json` sets `erasableSyntaxOnly`, so the compiler enforces
strip-types compatibility rather than leaving it to discipline. It also sets
`noUnusedLocals`, `noUnusedParameters`, `noImplicitOverride` and
`noFallthroughCasesInSwitch` — with no build step and no linter in this repo,
`tsc` is the only automated thing that reads the source, so it may as well be
asked the questions a linter would. Turning them on found three dead imports
immediately, one of them left behind by the refactor in the same commit.
