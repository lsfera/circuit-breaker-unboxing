# 018 — Control flow as expressions, and the message says what it is

**Status**: decided 2026-09-20 on article 1; carried to this branch
2026-09-24. Applied to `@egress/rmq` and the work message; the rest of the
code was reviewed for Effect style, not swept for every `if`.

## Rules

- **A closed set of cases is an exhaustive `Match`.** `settle` ended in a bare
  `else` that acked; a new `Settlement` is now a compile error. Topology replay
  uses `Match.discriminatorsExhaustive("kind")`.
- **Absence is an `Option`, and `undefined` is not written.** Memoised
  delivery fields are held as `Option` (an `Option<Option<A>>` is only a type).
- **The work message has one codec**, `WorkMessage` in `ControlPlane.ts`, used
  by the producer to encode and the daemon to decode.

## The work message, against RabbitMQ's guides

| Property | Rule here |
|---|---|
| `content_type` / `content_encoding` | declared by the producer (`application/json`); a daemon reads JSON, unencoded, or undeclared, and dead-letters anything else unread |
| `type` | `egress.work`; another type is declined |
| `message_id` | the idempotency key, `<run>:<n>`, assigned once by the producer and carried by every retry and redrive; sent as the third party's `x-idempotency-key`. No id, no call |
| `mandatory` | set on queue publishers; a `basic.return` fails `send` with `Unroutable` |

Every declined delivery is counted in `egress_daemon_discarded_total{reason}`
(`format`, `malformed`, `keyless`) and dead-lettered.

**A handler that throws is dead-lettered**, not acked. Acking had lost its work.

## Two connections

The client opens one connection for consuming and one that owns the confirm
channel. A memory alarm blocks publishing connections only; on one shared
connection, a consumer's acks went unread until the alarm cleared (measured:
5 unacked, 7 stuck ready).

## Evidence

The broker integration suite (now 28 tests) passes before and after, including
negative controls for `Unroutable` and the throwing handler.
