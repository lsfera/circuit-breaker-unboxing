# 002 — The aggregator's `OPEN` is observational, not authoritative

**Status**: decided — observational.
**Date**: 2026-09-05.
**Context**: Phase 1 of `.claude/plans/production-readiness.md`; closes the
fork `README.md` has deferred since the first commit.

## The question

Envoy already enforces. Each replica's outlier detection ejects hosts locally,
with no coordination, and traffic through that replica stops reaching the
ejected endpoints whether or not anything else in this system is running. The
aggregator's job is to reconcile those per-replica views into one coherent
per-API event stream.

The fork: is that stream **advice** that subscribers act on, or is the
aggregator's `OPEN` the authority, which Envoy must then be made to obey?

## The decision

Observational. The aggregator publishes; it never pushes configuration.

## Why

Enforcement is already local, immediate, and independent of this system's
availability. Making the aggregator authoritative would move the enforcement
decision *out* of the data path and into a control plane that can be slow,
stale, or down — the aggregator would become in-band for every request, and
its failure modes would become traffic failures rather than reporting gaps.
That is a large increase in blast radius bought against a small increase in
consistency.

What the aggregated stream is genuinely for is the thing no single replica can
do: tell a *consumer* of the API — the RabbitMQ daemon fleet here, a
subscriber elsewhere — that the API as a whole is out, so it can take a
compensating action Envoy has no way to take on its behalf. Stopping work at
the source is not a routing decision, and it is exactly what
`packages/rmq-consumer` does with the stream today.

The properties that make that work are already built and tested: a gapless,
strictly ordered per-API sequence, snapshots that are safe to re-apply, and a
delivery-integrity check that will say so when either breaks. Those are
contract guarantees, not enforcement.

## Consequences

- **No xDS push path.** No `direct_response` route pushed to Envoy, no
  proto codegen for the config API, no consistency protocol between the
  aggregator's state and the replicas' running config.
- The aggregator can be down without traffic being affected. Losing it costs
  visibility and the fleet's compensating actions, not enforcement — which is
  why the HA work targets *continuity of the stream* (leases, fencing,
  checkpoints) rather than failover speed.
- Subscribers are responsible for their own reaction. The repo owes them a
  contract they can trust, which is what `/api/subscriber` verifies.
- `README.md`'s "The fork this defers" section is no longer a fork. It stays
  as the reasoning, pointing here for the answer.

## What would flip it

- A subscriber that cannot be trusted to react in time, where the only
  effective control is to stop the traffic itself.
- A requirement that egress to a failing third party stop *fleet-wide within a
  bounded time*, which local ejection cannot promise: each replica learns from
  its own traffic, so a replica that is not currently calling the failing API
  has nothing to eject.
- Compliance or contractual limits on calls to a third party during an
  incident — an obligation, not an optimisation, and one that advice cannot
  discharge.

If any of those become real, this record is superseded rather than amended,
and the three consequences listed in `README.md`'s fork section (the xDS push
path, the authority question, the rollback story) become the next plan.
