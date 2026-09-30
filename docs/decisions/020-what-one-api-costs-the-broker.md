# 020 — What one API costs the broker

**Status**: decided 2026-09-29. Measured, not changed.

## The shape

A daemon serves one API (`--api-id`), and every API brings its own topology:

| Queue | Type | Per API |
|---|---|---|
| `work`, `work.dead`, `work.parked` | quorum | 3 |
| `probe-trigger`, `redrive-trigger` | quorum, single active consumer | 2 |
| `floor` | classic, single active consumer | 1 |
| `control.<daemon>` | classic, `x-expires` | one per daemon |

Every quorum queue is its own Raft group. ADR 015 sizes the console for a
thousand APIs; this is what a thousand would cost the broker.

## Measured

An idle RabbitMQ 4.3 node, one API's queues for a fleet of five daemons,
declared by `packages/rmq/test/integration/TopologyMemory.probe.ts`:

| APIs | quorum queues | classic queues | broker memory |
|---|---|---|---|
| 0 | 0 | 0 | 167 MB |
| 100 | 500 | 600 | 232 MB |
| 250 | 1,250 | 1,500 | 362 MB |
| 500 | 2,500 | 3,000 | 632 MB |

About **0.93 MB per API, idle**, before a message is stored. This stack's broker
flow-controls at 1 GiB (`infra/rabbitmq.conf`), so an idle node reaches its
watermark at about 900 APIs, and the deepest backlog measured here (391 MiB)
halves that. On a three-node cluster each quorum queue is three Raft members.

## Decision

The topology stays per API. It is what makes an API's outage its own: one
API's backlog, dead letters and elections never share a queue with another's.
The documented scale is **tens of APIs per broker, a few hundred at most**, and
each API brings its own daemon fleet.

## What would change it

- **Hundreds of APIs.** Multi-API daemons first (one process following several
  control routing keys), then shared election queues keyed by API in the body.
  The work queues stay per API.
- **A broker per group of APIs** before either, if isolation matters more than
  the count of brokers.
