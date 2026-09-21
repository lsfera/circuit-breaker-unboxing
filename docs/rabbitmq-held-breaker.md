# A circuit breaker with no memory: keeping the open state in RabbitMQ

*Part 2b of the circuit-breaker series. Part 2 put a breaker inside each
consumer. This one moves everything that breaker remembered into the broker.*

The breaker in part 2 was a library object. Its state — closed, open,
half-open, how long until the next probe — lived in the memory of five
processes, and the only thing it could do with a message while open was reject
it, wait a hundred milliseconds, and hand it back.

That worked, and it cost something we had not counted. In a 20-second total
outage, five replicas made 50 calls that reached the failing third party. The
dead-letter queue grew by **2,745**.

Nothing was wrong with those 2,745 messages. An open breaker rejects locally
and requeues, a requeue spends one of the message's three delivery attempts,
and a message that is rejected three times is parked. I did not instrument the
old build to watch it happen; fifty real failures cannot account for 2,745
dead letters, and that is the mechanism the numbers point to.

The fix is not a better hold. It is to stop rejecting. An open breaker should
receive nothing, and the thing that ends the silence should be a message the
broker keeps, so that it can be as long as it needs to be — a minute, or a day.

## Open is a consumer that isn't there

RabbitMQ already has a way to say "no more work for this process": cancel the
consumer. Messages stay in the queue, no delivery reaches the replica, no call
is made, nothing is requeued and nothing spins. The breaker becomes three
facts about the broker:

| phase | what the broker sees | leaves when |
| --- | --- | --- |
| **closed** | a work consumer at full prefetch | 5 calls in a row fail |
| **open** | *no* consumer; a wake token is in flight, addressed to this replica | the token comes back |
| **half-open** | a consumer with prefetch 1 — the first message it gets is the probe | probe succeeds → closed; fails → open again, longer |

```mermaid
sequenceDiagram
  participant W as work queue
  participant R as replica
  participant D as delay chain
  participant K as replica's wake queue
  W->>R: deliveries (prefetch 20)
  R->>R: 5th failure in a row
  R--xW: cancel, let in-flight calls settle (open)
  R->>D: token, attempt 0, hold 1s
  D-->>K: 1s later
  K->>R: attempt 0
  R->>W: consume, prefetch 1 (half-open)
  W->>R: one message, the probe
  alt probe fails
    R--xW: release it, cancel
    R->>D: token, attempt 1, hold about 2s
  else probe succeeds
    R->>W: consume, prefetch 20 (closed)
  end
```

The only thing a process still keeps while closed is the counter that decides
to trip: how many of its own calls have failed in a row. Everything after that
is in RabbitMQ. The token carries the attempt number, so the growing hold —
`initial · 2^attempt`, capped, jittered into its upper half so replicas that
tripped together do not return together — is read off the message, not off a
variable.

## The timer is the part I borrowed

A token has to come back after an arbitrary time without any consumer holding
it. RabbitMQ has per-queue message TTL and dead-lettering, and Particular
built [delayed delivery for NServiceBus](https://docs.particular.net/transports/rabbitmq/delayed-delivery)
out of exactly those two. It is a binary counter made of queues.

Level *n* is a queue whose messages all expire after exactly 2ⁿ seconds and
dead-letter into level *n−1*. A delay is written in binary in the routing key,
and each level's exchange asks one question: is my bit set? If so the message
waits in this level's queue; if not it is passed straight down. Because every
message in a queue shares one TTL, the next to expire is always the head, which
is the case per-queue TTL handles exactly and a single queue of mixed TTLs
would not.

```mermaid
flowchart TB
  send["send: 5 seconds = 101 in binary"] --> x2
  subgraph L2["level 2 — TTL 4s"]
    x2{{"bit set?"}} -->|yes| q2[("waits 4s")]
  end
  q2 -->|expires| x1
  subgraph L1["level 1 — TTL 2s"]
    x1{{"bit set?"}} -->|"no: pass down"| x0
  end
  subgraph L0["level 0 — TTL 1s"]
    x0{{"bit set?"}} -->|yes| q0[("waits 1s")]
  end
  q0 -->|expires| dx{{"delivery exchange"}}
  dx --> dest[("the replica's wake queue")]
```

Seventeen levels count to 131,071 seconds, just over 36 hours, so **a
24-hour hold is one ordinary durable message** — no consumer, no unacked slot,
no timer in anyone's memory. The levels are quorum queues, so it survives a
broker restart. Resolution is one second, which is fine for a breaker.

Measured against a real broker: a 5-second delay (waits in two levels, passes
through one) arrived at 5.2 s. A **100-second** delay (binary 1100100: levels 6,
5 and 2), sent, then the broker restarted ten seconds in, arrived at 100.1 s
and 100.5 s in two runs, once each.

## Chaos

A breaker that behaves in a demo is the easy half. I ran it the way the earlier
reliability work was run: real faults, injected under a traffic spike, judged
first on correctness and only then on what the breaker did.

**The bar** is per message, not from broker counters (the management API's
totals drift whenever a channel closes). Every message carries `message_id:
<run>:<n>`. The publisher reports exactly which *n* the broker confirmed; the
flaky third party reports which *n* it answered 200; and after the fleet has
drained, whatever was confirmed but never processed is either in the dead-letter
queue or lost. Nothing may be lost, and the dead-letter queue may not grow.

Each scenario runs 200 messages a second, spikes to 1,000/s the moment the
fault goes in, holds the fault for 40 seconds, restores it, and waits for every
breaker to close and the queue to empty — about 45,000 messages each.

| scenario | fault |
| --- | --- |
| `outage` | the third party answers 503 to everything |
| `outage-hang` | it accepts the call and never answers (2 s client timeout) |
| `kill-open-replica` | `docker kill` a replica while its breaker is open, restart it 3 s later |
| `kill-broker-while-open` | restart RabbitMQ with all five tokens in the chain |
| `delay-survives-broker-restart` | a 100 s delay across a broker restart |
| `partial` | 60% of calls fail — informational, no correctness claim |

Every replica's log is then read back and each transition checked against the
machine (closed → open → half-open → closed or open). Not one illegal
transition in any scenario.

### Results

Same scenarios, same load, one run each. One rule shapes them: a failure that
follows another failure on the same replica, and any failed probe, is `release`d,
which hands the message back with no strike against its three delivery attempts.
A failure that stands alone, a poison message between successes, is still
charged and still parked. It is a heuristic, and it trades some poison-message
protection during an outage for not dead-lettering healthy messages while the
breaker is still opening.

| | sent | processed | duplicates | lost | dead-lettered | openings | peak tokens | longest hold | all closed after restore |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `outage` | 45,345 | 45,345 | 0 | 0 | **0** | 30 | 5 | 29 s | 17 s |
| `outage-hang` | 47,363 | 47,363 | 0 | 0 | **0** | 28 | 5 | 26 s | 27 s |
| `kill-open-replica` | 46,585 | 46,585 | 0 | 0 | **0** | 31 | 5 | 32 s | 23 s |
| `kill-broker-while-open` | 41,742 | 41,742 | 0 | 0 | **0** | 30 | 5 | 24 s | 16 s |
| `delay-survives-broker-restart` | 1 | 1 | 0 | 0 | 0 | — | — | 100 s | — |
| `partial` (informational) | 44,505 | 44,494 | 0 | 0 | 11 | 83 | 5 | 15 s | 13 s |

The same scenarios again on 2026-09-21, after the dependencies moved to the latest
(Effect rc.116), one run each; every graded scenario passes again
([the run](runs/chaos-breaker-rc116.json)):

| | sent | processed | duplicates | lost | dead-lettered | openings | peak tokens | longest hold | all closed after restore |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `outage` | 47,035 | 47,035 | 0 | 0 | **0** | 30 | 5 | 32 s | 25 s |
| `outage-hang` | 48,617 | 48,617 | 0 | 0 | **0** | 29 | 5 | 32 s | 34 s |
| `kill-open-replica` | 44,989 | 44,989 | 0 | 0 | **0** | 31 | 5 | 28 s | 15 s |
| `kill-broker-while-open` | 42,857 | 42,855 | 0 | 0 | **0** | 30 | 5 | 32 s | 25 s |
| `delay-survives-broker-restart` | 1 | 1 | 0 | 0 | 0 | — | — | 100.8 s | — |
| `partial` (informational) | 45,721 | 45,709 | 0 | 0 | 12 | 67 | 5 | 32 s | 19 s |

In `kill-broker-while-open` the publisher sent 42,857 messages and the broker
confirmed 42,845. The 12 it never confirmed were in flight when the broker
restarted; 10 of them reached the queue anyway and were processed, and 2 did not.
Nothing confirmed was lost, which is the bar, but a publisher that restarts the
broker under itself cannot say more than that about its last few messages. The
first table's run had none in flight.

A killed replica comes back closed and starts over, and the token addressed to
its dead predecessor goes to a queue nobody reads. That is deliberate: each
process names its wake queue with a fresh random id rather than the container's
hostname, because a restarted container keeps its hostname and would otherwise
be woken early by a stale token. I reasoned that out and did not test the
alternative.

The claim the design rests on — an open breaker has no consumer — was checked by
comparing the broker's own consumer count on the work queue with the number of
replicas not open, sampled once a second: it matched in 47 of 56 samples in
`outage`, and in fewer (37 of 61) when calls hang for two seconds. The
mismatches are the gap between a consumer being cancelled and a Prometheus
gauge, scraped every two seconds, noticing. It is a sanity check, not a proof.

## The incident on screen

A 12-second total outage against the running stack, recorded from the
Grafana dashboard, at the compose producer's ordinary 200/s. Two panels were
added for this: the broker's consumer count on the work queue, which now *is*
the fleet's state, and the wake tokens sitting in the delay chain, one line per
level. The dead-letter line is flat, but not at zero: it is the ~4,500 messages
that earlier experiments left there, and that it does not move is the point.

The [recording](media/incident.webm) is 43 seconds, start to finish.

**1 · Steady.** Five closed breakers, five consumers, no tokens.

![steady state](media/1-steady.png)

**2 · The outage begins.** All five open within two milliseconds of each other
(the replicas' own logs), the consumer count on the work queue falls to zero,
and the tokens appear in the delay chain.

![the outage begins](media/2-mid-outage.png)

**3 · The third party is restored.** The backlog is about 2,200 messages, held in
the queue, not spinning through it. No consumers. Tokens are still in flight,
because the holds have grown to 5–7 s; the replicas do not yet know.

![restored, replicas still holding](media/3-restored.png)

**4 · Recovered.** The breakers came back at different moments. Three closed
within two seconds of the restore, one at 10 s, and the last at 14 s, from a 15 s
hold. The backlog drained as they did, and no dead letters appeared. Each step in
the tokens panel is a replica's hold ending. (The times are from the replicas'
logs.)

![recovered](media/4-recovered.png)

## What it costs, and what it doesn't fix

- **A long hold is a late recovery.** A hold of *h* seconds means a replica
  notices the third party is back up to *h* seconds late. In the recording the last
  breaker closed 14 s after the restore, from a 15 s hold; in the chaos runs the
  last one closed 16 to 27 s after it. With a 24-hour
  ceiling, a day-long outage can leave a replica dark for most of another day.
  The ceiling is how late you are willing to find out, not only how gently you
  want to probe.
- **A lost token is a stuck breaker.** If someone deletes a replica's wake
  queue, or purges a delay level, that replica stays open until it restarts.
  Nothing re-sends the token. I left this out deliberately: a re-send timer
  needs token identity to discard the late duplicate, and I would rather
  document the hole than half-close it.
- **It does not see a partial failure.** At a 60% failure rate a breaker that
  counts consecutive failures barely notices: it opened 83 times, flapping,
  and 11 healthy messages were still parked. Nothing was lost, but the
  dead-letter queue grew. That is the case a failure-*rate* breaker exists for,
  and it is the next part of the series.
- **Five replicas are still five breakers.** They trip at their own moments
  and come back at their own moments — the recovery in the recording is
  spread over 14 seconds, from under one to 14 s after the restore. What has changed is that the state now lives in one place. Turning
  five tokens into one fleet-wide verdict is a smaller step than it was, and it
  is the part after that.
- **One run each.** Every number above is a single run, not a distribution.

## Reproduce

```bash
docker compose up -d
node infra/incident.mjs                      # one 20 s outage, prints the summary
node infra/chaos-breaker.mjs                 # all scenarios, saved under history/runs/
node infra/chaos-breaker.mjs --list          # or pick some with --scenarios=a,b
```

From the devcontainer the services are reached by name; set `BROKER`,
`FLAKY_UPSTREAM` and `PROMETHEUS` to `amqp://guest:guest@rabbitmq:5672`,
`http://flaky-upstream:8080` and `http://prometheus:9090`. The recording is
`infra/capture-incident.mjs`, which needs `playwright-core` (not a dependency
of this repo). The chaos run above is
kept in [docs/runs/](runs/) as `chaos-breaker-after-fix.json`, with the
per-second series behind the table; `chaos-breaker-before-fix.json` is an
earlier run, from before failures were settled by the rule above.
