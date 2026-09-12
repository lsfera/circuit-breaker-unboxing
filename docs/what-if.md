# What if the environment is not this one

Every number in this repo was measured against the stack in `docker-compose.yml`:
three Envoy replicas, and upstream clusters with several endpoints each — six for
payments-provider, four for shipping-rates, three for tax-calc. That shape is not
incidental, and some of it is load-bearing in ways that are easy to miss until the
environment differs.

This document holds those questions. One section per "what if", each answered by
running the thing rather than reasoning about it, and each honest about which part
of the design survives and which part does not.

---

## What if the flaky API is behind a load balancer?

Then Envoy's cluster has **one endpoint** — the balancer's VIP — no matter how many
backends sit behind it.

### What is unaffected

The quorum across Envoy replicas. `Breaker.step` divides by `live.length`, the
replicas that reported inside `replicaTimeoutMs`, and that denominator has nothing
to do with how many endpoints each replica sees. [ADR 009](decisions/009-what-the-quorum-is-a-quorum-of.md)
is about that denominator shrinking; this is a different one.

### What collapses

The per-API health gradient. `voteOf` reads:

```ts
total > 0 && healthy === 0 ? DOWN : healthy < total || overflowSinceTick > 0 ? DEGRADED : OK
```

With `total === 1`, `healthy` is 0 or 1 and `healthy < total` is unreachable, so no
replica can vote DEGRADED from ejection. Envoy's ratio detector is out too:
`failure_percentage_minimum_hosts` is 3, which `infra/envoy/envoy.yaml` already calls
"dead configuration" with a single host per cluster, and
[architecture.md](architecture.md#why-envoy-for-the-data-plane) says the same about
`healthy < total`.

What neither says is what the breaker *does* instead.

### Measured

The real `Breaker`, five simulated minutes, same 45% failure rate, changing only the
endpoint count:

| | transitions published | time in each state |
|---|---|---|
| six endpoints | **1** → DEGRADED | DEGRADED 297s · CLOSED 3s |
| one endpoint (the VIP) | **88** | OPEN 119s · CLOSED 94s · HALF_OPEN 87s |

Thirty OPEN, twenty-nine HALF_OPEN, twenty-nine CLOSED. It never settles. Envoy ejects
the single host on `consecutive_5xx: 5`, the active health check un-ejects it after
`base_ejection_time: 5s`, and the cycle repeats. The breaker's own damping does not
absorb it, because the damping is shorter than the cycle: `dwellMs` is 2s and
`minStateMs` is 3s against a 5s ejection.

So the failure mode is not blindness. It is **flapping**, and the fleet pays for every
transition: each one is a reconcile across five daemons plus an SAC probe election, and
`targetActive` drops to 0 and restarts the CLOSED ramp from its first rung roughly every
ten seconds. An API that is 55% healthy ends up served at about probe rate.

Nothing in the system reports this. The sequence stays monotonic throughout, so
`/api/subscriber` reads `duplicates=0 gaps=0` and the delivery contract holds — the
system would call itself healthy while doing it.

### What would fix it

The first clause of `voteOf` is a **host-count proxy for "partially failing"**. Behind a
balancer the proxy breaks, but the quantity it stands for is still measurable, and the
second clause is already a second signal path — it is simply fed saturation counters
(`upstream_rq_pending_overflow`, `upstream_cx_overflow`, `upstream_rq_retry_overflow`)
rather than an error rate.

The signal is already on the wire. From the running stack:

```
cluster.payments-provider.upstream_rq_5xx:   10178
cluster.payments-provider.upstream_rq_total: 5555044
```

Per-cluster, and independent of host count. `SUFFIXES` in `FleetSource.ts` does not read
them. So the options, ranked by what they actually buy:

1. **Read the 5xx ratio** and feed it the way `overflowTotal` is already fed — as an
   edge-detected delta. DEGRADED becomes reachable with one host. This is an *ingestion*
   change, not a state-machine one: `Breaker` already has the slot.
2. **Point Envoy at the real backends** (EDS, or DNS resolution to the pool rather than
   the VIP). Restores the gradient properly. Usually not available for a third party.
3. **Raise `minStateMs` past the ejection cycle.** Cheap, stops the flapping, leaves the
   breaker binary. A palliative, and worth knowing as one.

None of these is implemented. This is a documented limit, not a plan.
