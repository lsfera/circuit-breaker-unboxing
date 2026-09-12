# Answering the actual question

The design in this repository began as an interview question, and the question
matters for reading everything else here — because it fixes both the picture
you start from and the one thing you are not allowed to do about it.

**Given** the system in [where it starts](../history/journey.md#0-where-it-starts): a
producer, a broker, a fleet of competing consumers, and a third-party API they
all call.

**Design reliability around it, given that the flaky service is outside your
control.**

That constraint is the whole exercise. It is worth spelling out what it
removes, because it eliminates entire families of answer before any design
begins:

- **You cannot ask whether it is healthy.** No health endpoint you trust, no
  status feed, no capacity you were promised. Everything you know about it, you
  learn from calls you made yourself — and every one of those calls is load on
  something that may already be failing.
- **You cannot make it faster.** The only variable on your side is how much you
  send it.
- **You cannot be told when it recovers.** You have to find out, which means
  calling it, which costs the thing in the first point.

Everything below is a way of spending those three facts. They are grouped by
the scope they act at, because that turns out to be what separates them — and
because the first three groups are **not alternatives to the fourth**. This
repository uses most of them at once.

```mermaid
flowchart TB
  classDef used fill:#dcf3f1,stroke:#0c8b86,stroke-width:1.5px,color:#04302e;
  classDef partial fill:#fbedd6,stroke:#c07f16,stroke-width:1.5px,color:#3a2c12;
  classDef no fill:#fbe2dd,stroke:#c9432f,stroke-width:1.5px,color:#3a1c17;

  L1["per call<br/>timeout · retry · backoff"]:::used
  L2["per process<br/>breaker library · bulkhead · rate limit"]:::partial
  L3["per queue<br/>prefetch · dead-letter · redrive"]:::used
  L4["per fleet<br/>one verdict, published"]:::used
  L5["per platform<br/>mesh · gateway · build the proxy"]:::no

  L1 --> Q["how much reaches the third party"]
  L2 --> Q
  L3 --> Q
  L4 --> Q
  L4 --> A["and what the rest of the system is told"]
  L5 -.-> Q
```

Only one of those rows reaches the second box.

## Per call

**Timeout.** Bounds how long one call may hold a worker. Necessary, and cheap.

*Limitation:* it cannot reduce load, and set aggressively it *increases* it —
a timeout that fires and triggers a retry turns one slow call into two. It
bounds your exposure, not the upstream's. Here: 2s on every egress call.

**Retry.** Recovers the genuinely transient — one dropped connection, one 503
from one host behind a balancer.

*Limitation:* a retry is another call. Under a real outage, retries multiply
load at the exact moment the dependency can least take it, and the picture in
[section 1 of the journey](../history/journey.md#1-what-the-picture-hides) is that
happening by default. Worse, an in-process attempt counter is lost the moment
the message moves to another consumer — so "three attempts" quietly becomes
three attempts *per consumer*. Here the broker counts instead
(`x-delivery-limit`), so the budget belongs to the message.

**Backoff and jitter.** Spreads retries out in time so they do not arrive as a
wall.

*Limitation:* it still assumes retrying is the right thing to do, and it
coordinates nothing — N consumers that failed at the same moment back off by
the same schedule and converge on the same instant again unless each jitters
independently. Backoff makes a herd politer; it does not stop it being a herd.
This repository backs off at the *breaker* rather than the call, and doubles
`openBackoffMs` on each failed probe.

## Per process

**A breaker library.** The first answer, and the one with the most literature.
Analysed in full in [a breaker library in every service](breaker-library.md).

*Limitation:* the state is a variable in one process. Measured at this fleet's
shape, five daemons against an upstream failing 45% of the time agree with each
other 17% of the time and send 370 probes in five minutes at one that is fully
down. There is also nothing to publish from.

**Bulkhead.** Cap the concurrency any one dependency may consume, so a slow
third party cannot exhaust the workers a healthy one needs.

*Limitation:* it protects *you*, not the upstream. N processes each with a
bulkhead of *k* still send *N × k* concurrent calls, and the third party
experiences the product, not the factor.

**Local rate limit.** Cap outbound calls per second, per process.

*Limitation:* the same multiplication, plus a worse problem — you have to pick
a number, and the correct number is a property of a system you were told you
do not control and cannot ask. A rate limit set from guesswork is either
throttling a healthy dependency or not throttling a failing one.

## Per queue

This group is where the given picture already has leverage, and it is
underrated: the queue is the one component that can hold work without doing it.

**Prefetch as the concurrency ceiling.** The broker refuses to hand over an
*n+1*th message until one is settled, so concurrency is enforced by the thing
holding the work rather than by a counter in the consumer — see
[ADR 011](decisions/011-the-ceiling-belongs-to-the-broker.md).

*Limitation:* it bounds how much is in flight. It does not decide whether any
of it should be.

**Dead-lettering.** A message that has exhausted its attempts leaves the work
queue instead of cycling forever.

*Limitation:* a dead-letter queue nobody drains is a slower way of losing
things — which is why this repository has a redrive at all.

**Redrive on recovery.** Replay the dead-letter queue once the dependency is
back, in bounded passes, from one elected consumer.

*Limitation:* it needs to know recovery happened. That is not a queue
primitive; it is precisely the verdict the next group exists to produce.

**Delay queues.** Hold retries for a fixed interval instead of releasing them
immediately.

*Limitation:* backoff again, in the broker rather than the process, with the
same blindness — the delay is a constant chosen in advance, not a response to
what the dependency is doing now.

## Per fleet

Everything above reduces or reschedules load. None of it produces **one answer
to "is this API up" that anything outside the calling process can act on** —
and the given picture has four parties who need that answer: the consumer
fleet, the producer, whoever is on call, and whatever the business does when a
payment provider is down.

**Share breaker state in Redis.** The tempting fix: let the consumers agree
before any of them acts.

*Limitation:* it puts a network round trip and a shared failure domain in the
hot path of the component whose entire job is surviving other people's
failures. It fails the constraint in the direction that hurts most.

**Publish from the proxy.** If an egress proxy is already doing outlier
detection, point its ejection events at a webhook.

*Limitation:* those records are per replica, so subscribers receive the
disagreement rather than a verdict — and the signal that matters most,
threshold saturation, is a counter delta rather than an event, so it is not in
the feed at all.

**Enforce locally, decide centrally, publish once.** What this repository does.
Each proxy replica ejects hosts on its own evidence, off the hot path; an
aggregator takes a quorum of what the replicas see and runs one breaker per
API; the verdict is published as a gapless per-API sequence that the consumer
fleet, a status page and anything else act on.

*Limitation — and it is real:* it is three components and a leader election to
answer a question a library answers in one line. The aggregator is a new thing
that can be down, which is why its `OPEN` is
[observational rather than authoritative](decisions/002-enforcement-authority.md):
if the control plane dies, enforcement is still happening in the proxies, and
the fleet holds its last known target rather than stopping.

## Per platform

Included because they come up, and dismissed because they change the question
rather than answer it. A service mesh puts the same Envoy outlier detection in
a sidecar and leaves the aggregation problem exactly where it was. A gateway
like APISIX gives a coarser per-route breaker and a logger that fires per
request, which is the hot-path coupling the constraint rules out. Building the
proxy gives exactly the semantics wanted and costs a quarter of engineering to
arrive where Envoy starts. All three are covered in the
[README's comparison](../README.md#approaches-and-where-each-one-runs-out).

## The review, in one table

The column that matters is the third one.

| | What it fixes | What it cannot fix | Used here |
|---|---|---|---|
| Timeout | unbounded waits | load — may increase it | yes, 2s |
| Retry | transient failures | multiplies load in an outage | yes, budget held by the broker |
| Backoff / jitter | retry bursts | still retrying blindly | at the breaker, not the call |
| Breaker library | one process calling a dead API | agreement, publishing, `DEGRADED` | no — [why](breaker-library.md) |
| Bulkhead | your workers | the upstream sees *N × k* | prefetch does this per channel |
| Local rate limit | your outbound rate | picking the number | no |
| Prefetch ceiling | how much is in flight | whether it should be | yes |
| Dead-lettering | poison messages cycling | getting the work back | yes |
| Redrive | recovering the backlog | knowing recovery happened | yes, elected to one consumer |
| Shared state in Redis | agreement | a round trip in the hot path | no |
| Publish from the proxy | telling someone | per-replica flapping; saturation is not an event | no |
| **Aggregate and publish** | **one verdict, off the hot path** | **it is three components** | **yes** |

The first nine rows are not alternatives to the last. They are what the last
one sits on top of, and a design that reaches for the control plane without
them is answering a question nobody asked.

What none of the first nine can do — individually or together — is make the
verdict a **fact the rest of the system can act on**, rather than an
implementation detail of whichever process happened to make the last call.
That is the question the constraint forces, and it is the only one the last
row answers.
