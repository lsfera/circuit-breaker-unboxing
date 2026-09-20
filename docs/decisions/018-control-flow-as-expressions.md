# 018 — Control flow as expressions: `Match`, `Option`, and one codec

**Status**: in progress — applied to `@egress/rmq`'s `Client.ts` and `Trace.ts`,
and to the base consumer's work message, on `article/01-base-scenario`
(uncommitted when this was written). Not yet carried up the article branches
or applied to this branch's other packages.
**Date**: 2026-09-20.
**Context**: a review pass over `Client.ts`, asked for in four steps: replace the
`if`/`else if` chain in `settle` with `Match`; then remove every `if` and every
`undefined`; then the `for` loops; then the hand-written `parse` of a work
message. It builds on [006](006-representing-absence.md) (how absence is
represented) and [007](007-message-contracts.md) (one declaration per message),
and on the earlier pass that moved tagged-union switches to `Match`
(`99904e7b4`). Where it disagrees with 006, that is recorded below rather than
edited into 006.

## The rule

**A closed set of cases is a `Match`, and it is exhaustive.** `settle` ended in a
bare `else`, so a fifth `Settlement` would have been acknowledged and dropped
without a word. `Match.exhaustive` makes the fifth a compile error. Same for the
topology replay (`Match.discriminatorsExhaustive("kind")`), where a new kind of
declaration would otherwise have been skipped on reconnect.

**Absence is an `Option` inside this code, and `undefined` is not written.**
Every site that asked "is there one?" now holds an `Option` and answers with
`O.match`, `O.map` or `O.liftPredicate`. That includes the three places 006
allowed `undefined` (memoisation, lookups, optional inputs), because the review
found each of them cost an `if` to read.

**A guard with nothing to return is a value with nothing in it, not a
statement.** Two shapes: `Option` where a value flows out of the guard, and a
two-line `when(condition, effect)` where it does not. `inSequence` does the same
for the two loops that awaited one step after another.

**One encoder and one decoder for what crosses the wire.** `WorkMessage` is a
`Schema` in `ControlPlane.ts`; the producer encodes with it and every daemon
decodes with it, to an `Option`. This is 007's rule applied to the one message
kind that still had a hand-written reader.

## What changed

| | Was | Now |
| --- | --- | --- |
| `settle` | four-way `if`/`else if`, bare `else` acks | `Match.value(outcome)`, exhaustive |
| `describe` | three `let x \| undefined` memos, each behind an `if` | `lazily(compute)`, memo held as an `Option` |
| topology | `Topology[]` plus a `Set` of keys, guarded by an `if` | `Map` keyed by declaration; first declaration wins, insertion order kept |
| topology replay | `for` loop over an `if`/`else if`/`else` | `inSequence` over `Match.discriminatorsExhaustive` |
| re-attaching consumers after a reconnect | `for … await` | `inSequence` |
| `amqpReplyCode` | `number \| undefined` | `Option<number>` |
| a consumer's first handle | `{ channel: undefined, … } as unknown as Consumer` | opened on a real channel first; no half-built handle, no cast |
| `parentFrom` | `string \| undefined` in, three lines of guards | `string` in, one `O.fromNullOr` |
| the consumer's work-message `parse` | 12 lines of `typeof`/`in` checks | `decodeWorkMessage`, shared with the producer's `encodeWorkMessage` |

## Where 006 bent

006 kept `undefined` for memoisation because "`Option` would need
`Option<Option<T>>`". That turned out to be a type, not a problem: `lazily`
holds `Option<A>` and `A` is itself an `Option` for `deadLetter`, `idempotencyKey`
and `parent`, and it reads without a sentinel. The lookups and optional inputs
006 left alone (`Map.get`, an options bag's `prefetch?`) still arrive as
`undefined`; they are converted at the first line that touches them
(`O.fromNullishOr`) and go no further. `null` stays where it was: the interop
state `out` and `opening`, which 006 already listed.

## What it rests on

The integration suite against a real broker (`pnpm run test:rmq`, 19 tests:
reconnect, consumer rebuild, single-active-consumer election, dead-lettering,
redrive republish, a broker restart): **19 pass before this pass and 19 pass
after**, and `pnpm run check` passes throughout (12 unit tests at the end, three
of them the new codec). That is the evidence the behaviour did not move, and it is
the only evidence: nothing here was measured for speed, and the per-delivery
path (`describe`, `handle`) gained a closure or two per message that no
benchmark has looked at.

## Behaviour that did change, deliberately

- **`n` must be an integer.** The old check was `typeof n === "number"`. `n`
  builds the idempotency key, so a `1.5` was never something this fleet
  published; it is now discarded like any other body that does not decode.
- **A consumer's first channel is opened before its handle exists**, instead of
  after a placeholder. `attach` then finds nothing stale to close.
- **Unknown fields on a work message are ignored**, which is `Schema.Struct`'s
  default and what the hand-written check did by not looking.

## Amendment — 2026-09-20: the message says what it is

The codec above decodes any body that parses. RabbitMQ's own guidance is that a
message should also say what it is: it "does not validate or use" `content_type`
and `content_encoding` — publishers set them, consumers are expected to respect
them ([consumers#content-type-and-encoding](https://www.rabbitmq.com/docs/consumers#content-type-and-encoding)).
So the format is now declared and read at both ends, the same rule as the
schema:

- A `Publisher` carries an optional `contentType` and `contentEncoding`,
  stamped on every message; the work producer declares `application/json`.
- `DeliveryInfo` exposes both as `Option`, read directly since a daemon decides
  on them before it looks at the body.
- A daemon reads JSON (parameters like `charset` ignored), unencoded (no
  encoding, or `identity`), **and a message that declares nothing**, because
  publishers that predate this and anything publishing raw (the chaos
  publisher) say nothing. Anything else is discarded to the dead-letter queue
  unread. `gzip` is refused, not inflated: nothing here compresses, so a
  `gzip` message was not published by this fleet.

Checked: 14 unit tests (the negotiation is a pure function of the two
declarations) and the integration suite against a real broker, 20 of 20, one of
them new and confirming both properties survive a publish and arrive as `None`
from a publisher that set none. **Not run:** a daemon discarding a `gzip`
message end to end against a live fleet; that path is covered by the pure
function and the property round-trip, not by an observed dead letter.

## What this does not settle

- **`when` is an `if` under another name.** It removes the statement, not the
  branch. Used for two guards with no value to carry (only warn while still
  connected; skip a confirm callback after a close). If a reviewer would
  rather those two stay as `if`, that is a fair reading of the rule.
- **`[...pending].forEach` is still an imperative call.** It is not a loop
  statement and is left.
- **The later article branches still carry the old code**, and from article 11
  the work message also carries `at` (its enqueue time), which the schema would
  need as an optional field. The same is true of `controlEvent`, the
  `circuit.control` body, whose shape is written out independently on each side
  of the wire on purpose (`@egress/consumer` does not depend on
  `@egress/aggregator`): a shared declaration there needs a home both can import.
- **Only `Client.ts`, `Trace.ts` and the consumer's parse were touched.** The
  rest of the codebase was not swept for `if`, `undefined` or loops, and this
  record should not be read as saying it was.
