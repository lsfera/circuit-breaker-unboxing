# 021 — Two coordination backends

**Status**: decided 2026-10-01.

## The shape

The aggregator coordinates through three ports: `LeaderElection`,
`CheckpointStore` and `Outbox`. Until now they lived in the aggregator with
Redis as their only implementation, so a platform that already runs
PostgreSQL had to run Redis as well, for a few small writes per tick.

What a backend must give, whichever it is:

- **a lease with a TTL on the store's own clock**, never an aggregator's;
- **a fencing token that only grows within an epoch**, and a checkpoint write
  that is checked against it in the same atomic step. Per-key fencing is not
  enough (see [high-availability.md](../high-availability.md));
- **an outbox per API**, ordered and bounded, that commits by position.

Redis gives these with Lua scripts. PostgreSQL gives them with single
statements: an upsert for the lease, a checkpoint write that reads the lease
row `FOR SHARE`, and absolute outbox positions. RabbitMQ was the other
candidate, since it is already deployed: single-active-consumer elects a
leader, but nothing in it writes conditionally, so it cannot mint or check a
fencing token.

## Decision

Three packages, and the URL's scheme picks one at startup
(`--coordination=redis://…` or `postgres://…`):

| Package | Holds |
|---|---|
| `@egress/coordination` | the ports, the token rules (`isFenced`), the checkpoint schema, the 1 s call guard |
| `@egress/coordination-redis` | the ports over Redis (ioredis) |
| `@egress/coordination-postgres` | the ports over PostgreSQL (postgres.js) |

The ports need a package of their own: the backends implement them and the
aggregator depends on the backends, so leaving the ports in the aggregator
would make each backend depend on the package that depends on it.

`test/integration/suite.ts` is one conformance suite that both backends must
pass against the real store. It found a bug before anything shipped: the
PostgreSQL outbox selected its position as text under the column's own
name, so `ORDER BY pos` sorted `"100"` before `"50"`.

Chaos-load `kill-store` found a second: after PostgreSQL restarted, neither
aggregator led again. Each call made during the outage timed out while
postgres.js was reopening a connection for it, and the backend cancelled it;
postgres.js then never releases that connection, so every pool wedged with all
its connections "busy" and idle on the server. A call given up on is now
abandoned, not cancelled, and a test drops a connection, makes its replacement
slower to open than a call is given, and checks the next call still goes
through.

Review found a third. With a pool of four, a call the client had given up on
could still run after a later one: a checkpoint save under the same lease
(an instance that stands down keeps its lease, and renews it) or an outbox
append behind an event delivered after it. The older sequence then stayed.
The pool has one connection, as Redis has, so calls land in the order they
were issued; a test opens the first connection slowly and checks the later
save is the one stored. One connection made postgres.js's reconnect backoff matter,
since it grows to 20 s: `kill-store` took 8.6 s to recover until it was
fixed at half a second.

PostgreSQL creates its tables on the first call that reaches it, under an
advisory lock, rather than at startup: an unreachable store is a failed call
the aggregator stands down over, never a crash.

## What would change it

- **A third backend** (etcd is the obvious one: its lease, revisions and
  transactions match these ports closely) is a new package plus a scheme,
  provided it passes the suite.
- **A failover that differs between the stores.** Today the coordination
  faults measure within a second of each other
  ([measurements.md](../measurements.md#redis-against-postgresql-2026-10-01));
  only `low` has been run.
