# 015 — The console at a thousand APIs

**Status**: proposed 2026-09-12; steps 1 and 2 built 2026-09-13, steps 3–5
still proposed. The proposal below is as written; the amendment at the end
says what building the first two measured, and corrects one thing it got wrong.
**Date**: 2026-09-12.
**Context**: [measured limits](../measurements.md#at-a-size-nobody-runs-it-at)
found that the console breaks first at a thousand APIs — `/api/stream` re-sends
the whole state frame every 400ms, about 2.75 MB/s per browser — and that
nothing in the control path notices. The second half of that turned out to be
wrong, and the first half was three problems filed as one.

Every number below comes from
[`scripts/measure-console-stream.mjs`](../../scripts/measure-console-stream.mjs)
against `--source=sim --apis=1000 --replicas=10`, recording a real stream
through a quiet fleet, 5% of APIs failing and 25% failing, then replaying the
same recording through each candidate encoding.

The synthetic fleet had to be made less synthetic first. `synthetic-000` to
`synthetic-999`, all with six endpoints and identical timestamps, gzips 82:1;
the same frame with ids, endpoint counts and clocks varied the way a real fleet
varies them gzips 19:1. The script gives every API a fixed realistic identity
applied identically to every frame — so a field that did not change still does
not — and every compression figure here is from that. Quoting the first number
would have overstated what compression buys by four times.

## What is actually wrong

### 1. The bytes do not depend on whether anything happened

| per browser, state channel | quiet | 5% failing | 25% failing |
| --- | --- | --- | --- |
| today | 2.77 MB/s | 2.77 MB/s | 2.80 MB/s |
| APIs that actually changed, per frame | 0 | 46 | 231 |

A fleet in which nothing moves costs exactly what a storm costs. 62% of the
changes that do happen are in the replica strip alone — an API's state,
sequence and endpoint counts unchanged, one replica's vote flickering.

### 2. The control loop pays for every browser

The frame is not built once and sent to everyone. `Stream.fromEffectSchedule`
runs inside each request, so every connected browser builds its own 1.1 MB
frame and serializes it, every 400ms, on the event loop that runs the breaker.

| concurrent streams | control loop ticks/s | aggregator cpu |
| --- | --- | --- |
| 0 | 3.69 | 9% |
| 5 | 3.69 | 14% |
| 20 | 3.59 | 25% |
| 50 | 3.49 | 45% |
| 100 | 2.88 | 71% |

A hundred open consoles take 22% of the control loop's cadence — 16% on a
second run — and about 0.6% of a core each. The aggregator's compose ceiling is
one core ([ADR 014](014-the-measurement-envelope.md)), so by that slope, around
150 consoles is where the kernel starts throttling the process that decides
whether a payment provider is down. That is an extrapolation from a measured
slope, not a run; the run stopped at a hundred.

**This is the part that matters.** A console that costs bandwidth is a cost. A
console that slows the breaker is the observability surface degrading the thing
it observes, in the incident it is being watched for, when more people have it
open than at any other time.

### 3. A thousand cards is not a console

`renderState` rewrites every panel on every frame — six text nodes, four
attributes and a ten-span `innerHTML` per API, about eleven thousand DOM writes
2.5 times a second whether anything changed or not. What that costs a browser is **not measured**: there
is no browser in the environment this was written in, and it is the one number
here that is inferred rather than read. It does not need measuring to see the
design problem, though. Nobody reads a thousand cards; during an incident they
look for the ones that are not green.

### 4. The contract checker downloads the console

`subscriber.ts` — offered as the shape a real subscriber should take — reads
`/api/stream` and keeps only `cloudevent` frames, discarding 2.77 MB/s of state
it never looks at. Its events come from a `PubSub.sliding(256)`, which drops the
oldest for a reader that falls behind. So the one client whose job is reporting
gaps is made a slow reader by bytes it throws away, and a slow enough one would
have events dropped that it then reports as gaps. That is the mechanism; it was
not reproduced.

## The candidates, measured

Steady state per browser for the state channel; connecting once costs the
last column. The event tape — 24–46 KB/s raw — is the same in every scheme and
excluded.

| | quiet | 5% failing | 25% failing | on connect |
| --- | --- | --- | --- | --- |
| today | 2.77 MB/s | 2.77 MB/s | 2.80 MB/s | 1.14 MB |
| today, gzip | 124 KB/s | 127 KB/s | 138 KB/s | |
| today, brotli with a 4 MiB window | 0.1 KB/s | 3.3 KB/s | 16.1 KB/s | |
| per-API delta | 0 | 123 KB/s | 620 KB/s | 1.14 MB |
| per-API delta, gzip | 0 | 8.0 KB/s | 38.1 KB/s | |
| per-field delta, gzip | 0 | 5.7 KB/s | 26.3 KB/s | |
| attention view (50) + counts | 0.5 KB/s | 49 KB/s | 134 KB/s | 0.2–33 KB |
| attention view, gzip | 22 B/s | 1.3 KB/s | 8.5 KB/s | |
| attention view as deltas, gzip | 0 | 1.4 KB/s | 7.8 KB/s | |

Compressor state held per connection: **0.22 MiB for gzip, 6.5 MiB for brotli
with the 4 MiB window.**

Three things the table says that were not obvious going in:

- **The view does the work, not the delta.** Once the console shows the fifty
  worst APIs rather than all of them, sending only what changed saves 6% in a
  storm — because the fifty worst are exactly the ones churning. Deltas matter
  enormously for a *whole-fleet* view (quiet: 2.77 MB/s to nothing) and barely
  at all for the view a person should be looking at.
- **Brotli with a wide window is deltas for free.** Its window reaches the
  previous frame, so an unchanged API encodes as a back-reference to itself:
  16 KB/s in a storm with no protocol change at all. And 6.5 MiB per browser.
- **gzip alone is a 20× cut for one header** — and it is still flat regardless
  of churn, still costs the server the full frame per connection, and does
  nothing for problems 2, 3 or 4.

## The proposal

Five steps, in the order they should land. Each is shippable on its own, and
each is justified by a row above rather than by the one after it.

### Step 1 — The tape gets its own stream

`GET /api/events/stream`: cloudevents only. `subscriber.ts` and any machine
consumer move to it; `/api/stream` stays the console's. An hour, and it removes
problem 4 outright — the contract checker stops being made slow by bytes it
discards.

### Step 2 — One frame per tick, shared by every connection

One fiber builds and serializes the frame on the 400ms schedule and publishes
the encoded bytes to a `SubscriptionRef` — latest value wins, so a slow browser
skips frames rather than queueing them. Each connection writes bytes it did not
compute.

This is the step that fixes problem 2, and it goes first among the substantial
ones because it is the only problem here that touches correctness. The work per
connection becomes a write, and the measured slope of 0.6% of a core per
browser should collapse to near zero. **That is a projection**; re-running part
2 of the script is the verification, and the step is not done until it has
been.

### Step 3 — A view, not the fleet

The console subscribes to what a person can use:

- **counts by state** — `CLOSED 894 · DEGRADED 71 · OPEN 35` — which is how a
  thousand healthy APIs should be shown: as a number;
- **the attention list** — everything not `CLOSED`, worst state first then most
  recently changed, capped at fifty, with the truncation stated rather than
  silent: *106 need attention, 50 shown*;
- **one API in full**, replica strip and all, when someone drills into it —
  `GET /api/stream?api=<id>`;
- **search** as a request, `GET /api/state?q=`, not a stream.

Measured: 8.5 KB/s gzipped in a storm against 2.80 MB/s today, 33 KB to
connect against 1.14 MB — and, unlike every other row, **bounded by the cap
rather than by the fleet**, so it costs the same at ten thousand APIs. It is
also what fixes problem 3: a fifty-row list re-renders cheaply, and it is the
list someone would actually read.

It needs a UI redesign, which is why it is days rather than hours: a summary
strip, the attention list, a detail pane. Cards survive in the detail pane,
where there is one of them.

### Step 4 — Snapshot, then patches, with resume

- On connect: a `snapshot` event, carrying a revision.
- Afterwards: a `patch` event only when something changed — `upserts` (whole API
  objects), `removed` (ids that left the view) and `counts`. A keep-alive
  comment every fifteen seconds otherwise, so a quiet fleet costs nothing.
- Every event carries SSE `id: <revision>`, so a reconnecting browser sends
  `Last-Event-ID` and receives the patches it missed if they are within a
  bounded history — the last 64 revisions — or a fresh snapshot if not.

The revision cannot be the API's `sequence`. `sequence` moves on published
transitions, and 62% of what changes on screen is a replica's vote, which
publishes nothing. It is a per-frame counter, and an API is in a patch when its
serialized object differs from the previous frame's — exactly what the script
does to produce the rows above.

For the attention view this saves little, as measured. It is proposed anyway
for two reasons the table does show: a quiet fleet sends *nothing* rather than
a count every 400ms, and it is what makes an opt-in whole-fleet table
(`?view=fleet`, for the engineer who does want all thousand) cost 38 KB/s in a
storm instead of 2.80 MB/s.

Whole API objects rather than changed fields. Per-field patches save a further
31% gzipped in a storm, and a client merging partial objects into state it
already holds is where this kind of code grows its bugs. Worth revisiting only
if a measurement after step 5 says the remaining bytes are a problem.

### Step 5 — gzip on the stream

`Content-Encoding: gzip`, flushed after every event so nothing waits for a
buffer to fill, with `Cache-Control: no-transform` and `X-Accel-Buffering: no`
so that a proxy does not decide to buffer it for the browser. 0.22 MiB per
connection.

Last, because compression multiplies whatever is being sent. Applied to today's
frames it gives 124 KB/s and leaves problems 2–4 untouched; applied after steps
3 and 4 it gives **7.8 KB/s in a storm — 360 times less than today — with a
33 KB connect.**

## Considered, and not proposed

**Brotli with a wide window, protocol unchanged.** The most efficient encoding
of today's frames measured here, for a structural reason: a window larger than
a frame makes resending an unchanged frame nearly free. But its state is
6.5 MiB per connection, so a hundred consoles hold 650 MiB — more than the
aggregator's entire 512 MiB container limit — and the browser still parses and
renders 1.1 MB 2.5 times a second. It is a good answer in a process whose only
job is fan-out, which is the next item.

**A console gateway.** The aggregator publishes patches once; a separate
stateless process fans them out to browsers. This is the right shape past a few
hundred viewers — it puts every console in a different failure and CPU domain
from the breaker, for the same reason [ADR 002](002-enforcement-authority.md)
keeps enforcement out of the aggregator — and it is where wide-window brotli
becomes affordable. Not proposed now because after step 2 the aggregator's
per-connection cost should be a write, and a new service should be built when
a measurement says the write is the problem, not before.

**gzip alone.** Twenty times smaller for one header. Named because it is the
obvious first move and it looks like a fix in a bandwidth graph; it is flat
regardless of churn and leaves the control loop paying for every browser.

**WebSockets.** Change nothing about the bytes. SSE already resumes through
`Last-Event-ID`, and changing the view is a reconnect with a different query.
Worth revisiting only if a client needs to change its subscription several
times a second.

**JSON Patch, or a binary encoding.** Neither was measured. A whole API object
is the unit the client already keys its state by, and a binary encoding would
trade the ability to read the stream in a browser's network tab for a factor
gzip already mostly takes. Listed so that the absence of a measurement is
visible rather than implied.

## Verifying it

Each step is done when [`scripts/measure-console-stream.mjs`](../../scripts/measure-console-stream.mjs)
says so, at 1000 APIs × 10 replicas:

| | today | target |
| --- | --- | --- |
| console, 25% failing, per browser | 2.80 MB/s | under 10 KB/s |
| console, quiet, per browser | 2.77 MB/s | under 1 KB/s |
| connecting | 1.14 MB | under 50 KB |
| ticks/s with 100 consoles open, against none | −16% to −22% | within 3% |
| aggregator cpu per open console | ~0.6% of a core | under 0.05% |

The targets are targets, not measurements; the "today" column is. The browser's
rendering cost is the gap: it needs a performance trace of the console at a
thousand APIs, before and after step 3, which this repository cannot take yet.

## What this does not address

**Prometheus cardinality** — sixteen series per API — is a separate cost in
someone else's system, with its own paragraph in
[measurements.md](../measurements.md#at-a-size-nobody-runs-it-at).

**Memory at this size.** During these runs the aggregator's RSS read between
0.9 and 1.2 GiB, against 462 MB for the same configuration in the scale table.
It is a host process with no limit, so this is at least partly V8 sizing itself
from 47 GiB ([ADR 014](014-the-measurement-envelope.md)) and partly uptime — it
grew across the runs — but it was not investigated, and whether the aggregator
at a thousand APIs fits its own 512 MiB compose ceiling is unmeasured.


---

## Steps 1 and 2, built

Added 2026-09-13. `GET /api/events/stream` carries the tape alone and
`subscriber.ts` reads it; [`ConsoleFrames.ts`](../../packages/aggregator/src/ConsoleFrames.ts)
builds the console's frame once per interval for every connection.

Measured with part 2 of the script against the code before and after, each on a
freshly started aggregator at 1000 APIs × 10 replicas with the fleet quiet — the
same conditions on both sides, which the proposal's own table did not have:

| open consoles | ticks/s before | after | aggregator cpu before | after | RSS before | after | frames built/s after |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 3.69 | 3.69 | 8% | 10% | 396 MiB | 429 MiB | 0 |
| 20 | 3.59 | 3.69 | 28% | 10% | 562 MiB | 449 MiB | 2.49 |
| 50 | 3.49 | 3.69 | 49% | 13% | 709 MiB | 463 MiB | 2.49 |
| 100 | 3.28 | 3.69 | 79% | 15% | 1166 MiB | 489 MiB | 2.49 |

The control loop no longer notices a hundred consoles. The frame is built 2.49
times a second whether one browser is open or a hundred, and not at all when
none are. Against the proposal's targets: the cadence at a hundred consoles is
within 3% of none — it is identical — and the cost per console is 0.05% of a
core, which is the target line, not under it. What is left per connection is
writing 1.1 MB it did not compute, 2.5 times a second, and that is steps 3 to 5.

**The bytes on the wire did not change.** A browser still receives 2.77 MB/s at
a thousand APIs. That was never what steps 1 and 2 were for.

Four things came out of building it.

**The proposal named the wrong primitive.** It said `SubscriptionRef`, latest
value wins. `SubscriptionRef.changes` is backed by `PubSub.unbounded`, so a
browser pulling 1.1 MB frames slower than they arrive would have had them
queued in the aggregator without limit — the per-request schedule it replaced
never had that failure, because it built the next frame only when the last one
had been written. It is `PubSub.sliding` with a capacity of one: a newer frame
replaces an unread one, and a reader that stops reading costs the frame in
flight and the one in the slot. The test that says so fails against an
unbounded PubSub; it was run that way to check.

**The memory question the proposal left open was the measurement.** It recorded
0.9–1.2 GiB for the aggregator against 462 MB in the scale table, and guessed at
V8 and uptime. The table above answers it: the old code, fresh, read 396 MiB
with nothing connected and 1166 MiB with a hundred streams — a 1.1 MB frame
built per connection per tick, and the garbage from it. The proposal's own load
test was what grew the process it was reading. Shared, a hundred consoles add
60 MiB — less than one 1.1 MB frame in flight per socket, which is the most the
bounded buffer lets each of them hold.

**No replay, on purpose.** A replayed frame would be the last one built, and the
last one built could be from whenever a console was last open — an hour-old
incident shown as current. A new console waits at most 400ms for a frame that is
not stale.

**The tape needed a keep-alive the proposal did not mention.** The state frames
had been keeping the subscriber's connection busy by accident. Taking them away
turns a quiet hour into an idle connection, and an idle connection is one a load
balancer closes; the tape sends an SSE comment every fifteen seconds, which the
decoder `subscriber.ts` uses is tested to ignore.

What was verified beyond the table: five tests in
`ConsoleFrames.test.ts`, each run against an implementation broken in the way it
names (unbounded buffer, building with nobody watching, no finalizer) and seen
to fail; 100 unit, 16 broker and 9 Redis tests; the subscriber on the compose
stack receiving an entire incident over the new route — sequence 180 to 186,
`DEGRADED` through two probes to `CLOSED`, no gap; and the demo passing on the
rebuilt aggregators.

What was not: the aggregator's CPU on the compose stack at three APIs, where the
effect is too small to matter and the comparison could not be taken cleanly —
the host suspended during two instrumented runs, which read 2,169 and 1,047
seconds of wall clock inside a five-minute timeout. The harness recorded both
windows; neither is quoted.

