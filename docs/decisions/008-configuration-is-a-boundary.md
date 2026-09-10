# 008 — Configuration is a boundary too

**Status**: decided — applied to every process here.
**Date**: 2026-09-07.
**Context**: the pass after [007](007-message-contracts.md), which gave every
message crossing the broker one declaration. Configuration was the last
boundary in this repo still being read by hand, and it turned out to be the
one where a bad value does the most damage with the least noise.

## What was there

Every process read its settings the same way:

```ts
const env = (name: string, fallback: string) => process.env[name] ?? fallback;

fleetSize: Number(env("FLEET_SIZE", "5")),
maxInFlight: Number(env("MAX_IN_FLIGHT", "32")),
redriveOnClose: env("REDRIVE_ON_CLOSE", "false") === "true",
```

The aggregator did the same over argv rather than the environment:
`Number(args.get("replicas") ?? 5)`, `args.get("source") ?? "sim"`.

`Number("five")` is `NaN`, and nothing downstream was written to notice one.

## What one mistyped variable does

Measured against the real pure code, not reasoned about:

| Variable | What `NaN` reaches | What happens |
| --- | --- | --- |
| `FLEET_SIZE` | `activeIndices` | `Array.from({ length: NaN })` is empty, so **every daemon idles forever** while the producer keeps filling the queue. Five containers up, healthy, consuming nothing. |
| `MAX_IN_FLIGHT` | the concurrency gate | `inFlight < NaN` is false, so the first message waits on a permit that never arrives. |
| `REDRIVE_MAX` | the redrive bound | `moved >= NaN` is false, so a pass is unbounded — the bound exists because an unbounded one was measured republishing two messages 17,703 times. |
| `RATE_PER_SECOND` | the producer's batch | `Array.from({ length: NaN })` again: it publishes nothing, quietly. |
| `DAEMON_INDEX` | `activeIndices(...).has(index)` | false forever; that daemon never works, and the other four cover for it. |

None of these crash. The first one is the worst thing in this table: it is the
same "healthy-looking process can be a deaf one" failure this repo already has
a finding about, now reachable from a typo in a compose file, with no error
anywhere in the system.

The booleans had a quieter version of the same problem.
`env("REDRIVE_ON_CLOSE", "false") === "true"` means `1`, `yes`, `on` and
`TRUE` all silently meant *off* — an operator turning a feature on and getting
nothing, with the container reporting a clean start.

And `--source` had a third: anything that was not a recognised mode *was* the
default. That file already carried a scar from it — a flag that never reached
argv at all, so the aggregator ran in `sim` mode against real Envoy replicas
and reported nothing wrong.

## The rule

The same one 007 applied to messages: **one declaration per process, decoded at
boot, and a value the process cannot use does not decode.** Two things follow
from it that a fallback cannot give:

- **The message names the variable.** `Expected a value greater than 0 at
  ["FLEET_SIZE"]` is a fix. An idle fleet is an investigation.
- **It fails before anything is built.** A process whose configuration cannot
  be read has nothing useful to do, and the one thing it must not do is open a
  broker connection, register consumers, and then behave oddly. Exit code 1
  before the first socket means the restart loop and the log line say the same
  thing.

`Config` is Effect's own facility for this, so the pass is mostly deletion:
`Config.schema(PositiveInt, "FLEET_SIZE")`, `Config.port("METRICS_PORT")`,
`Config.boolean("REDRIVE_ON_CLOSE")`, `Config.literals([...], "source")`.

## What is shared, and why it is a package

`@egress/config` holds three things, for the same reason `@egress/tracing`
exists: every process here needs them, and one declaration beats three that
have to agree.

- **`PositiveInt`.** Zero is excluded deliberately rather than left to
  `Natural`. Every value using it is a *capacity*, and zero capacity is the
  same silent stall a `NaN` produces — `MAX_IN_FLIGHT=0` deadlocks the gate
  exactly as `abc` does, and is far easier to type by accident.
- **`brokerAddress`.** `host:port` as a `TemplateLiteralParser`, the same shape
  as the lease token's wire form. It replaces `Number(addr.split(":")[1])`,
  which gave `NaN` on `rabbitmq:abc`, and `host ?? "127.0.0.1"`, which never
  fired for `RMQ=:5672` because an empty string is not nullish. Both halves are
  required now: a bare `RMQ=rabbitmq` used to mean "and assume 5672", and
  assuming is what this pass removes.
- **`load`.** Parse now, or log one fatal line and exit 1. `parse` is separate
  from it so the rules are testable without a process to kill.

## What this does not change

The *values* and their defaults are identical, and so is where they come from:
the daemons take environment variables because they are containers, the
aggregator takes flags because that is what `command:` writes. Nothing in
docker-compose.yml needed editing — every setting it passes decodes.

One thing did need editing, and it is worth recording because the Dockerfile
warned about it in a comment and the warning was still not enough: the image
copies workspace manifests one line at a time, "kept in step by hand", and a
package missing from that list fails as `ERR_MODULE_NOT_FOUND` at runtime
rather than at build time. Adding `@egress/config` reproduced exactly that,
one restart after the code was green. The list is the same class of problem
this pass is about — two things that must agree, with nothing checking that
they do — and it is still hand-maintained, because the alternatives Docker
offers for globbing manifests are worse than the comment.

## Consequences

A deployment that was quietly wrong now fails to start. That is the intent, and
it is a real behaviour change: `RMQ=rabbitmq` and `REDRIVE_ON_CLOSE=1` used to
start and misbehave, and now stop with a message. Every deployment in this repo
already writes values that decode.

## Read as a dependency, not at module load

Added 2026-09-10, applying EffectPatterns'
[access-config-in-context](https://github.com/PaulJPhilp/EffectPatterns/blob/main/content/published/patterns/core-concepts/access-config-in-context.mdx).

The half of that pattern this repo already had: `Config` and `HaSettings` are
`Context.Reference`s with defaults, `Aggregator.ts` does `const cfg = yield*
Config`, and tests override them with `Effect.provideService`. Config that is
read *at depth* was already a dependency rather than an argument.

The half it did not: process settings were read at module load by a `load` that
called `process.exit(1)` on a bad value. Fail-fast was right; owning the
process's fate from inside a library module was not. Importing
`@egress/config/Settings.ts` — or any `main.ts` — was enough to end a process,
which is also why none of them could be imported by a test.

`read` returns an `Effect` that fails with `SettingsUnreadable`, and each
composition root is `Layer.unwrap(Effect.gen(...))` around the graph it builds
from those settings. Nothing below an unbuilt layer is built, so a bad value
still stops the process before a socket is opened; the difference is that the
failure now travels the way every other startup failure does. Measured:

```
$ FLEET_SIZE=five node --experimental-strip-types src/main.ts
ERROR (#2): SettingsUnreadable: rmq-daemon: SchemaError(Expected a string
  representing a finite number at ["FLEET_SIZE"])
exit=1
```

Same outcome as before, one difference worth knowing: `runMain` prints a stack
with it, where `logFatal` printed one line. The variable is still named on the
first line.

What was *not* adopted: settings as a service every module yields. The pure
cores take their configuration as arguments on purpose — that is what makes
them total functions testable with plain assertions — and `makeRedrive`'s
options are the coupling written down deliberately. Passing values into a layer
at the composition root is dependency injection; it is not the prop-drilling the
pattern warns about.

