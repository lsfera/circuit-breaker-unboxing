# 001 — Which AMQP client the daemon fleet uses

**Status**: **superseded by [004](004-downgrade-to-amqp-0-9-1.md)** on
2026-09-06 — the repo now uses `amqplib` (AMQP 0-9-1). The reasoning below
stands as it was written; what changed is the evidence, and 004 says which.
Originally: decided — keep `rabbitmq-amqp-js-client` (AMQP 1.0).
**Date**: 2026-09-05.
**Context**: Phase 1 of `.claude/plans/production-readiness.md`.

## The question

`packages/rmq` wraps [`rabbitmq-amqp-js-client`](https://github.com/coders51/rabbitmq-amqp-js-client)
1.0.0, which describes itself as early-stage. Three of its bugs are worked
around inside `packages/rmq/src/Client.ts`, and one hazard is documented but
unpinned: dead-lettering becomes unreliable on a connection where the
stranding bug has been provoked (`docs/rmq-control-plane.md`). Two capabilities
were written up as absent from it — a redelivery budget, and any way to reduce
a live consumer's credit — and both are load-bearing for the design
(`DEGRADED`, and one-attempt-then-dead-letter).

The alternative is AMQP 0-9-1 via `amqplib`, which was assumed to have both.

## What was measured

A bounded spike, 2026-09-05, against `rabbitmq:4.0-management-alpine`, run
outside the workspace so no dependency was added. Numbers, not impressions:

**On the current client (AMQP 1.0), with a quorum work queue carrying
`x-delivery-limit: 3` and a dead-letter target:**

```
deliveryCount reported to the handler : [0, 0, 0, 0]
total deliveries before it stopped    : 4
dead-lettered                         : reason "delivery_limit"
```

The handler returned `requeue` every time — the outcome the earlier finding
called useless. The **broker** counted the attempts and dead-lettered the
message itself. The client still cannot *see* a count, and does not need to.

**On `amqplib` (AMQP 0-9-1), same broker:**

```
1. x-single-active-consumer            : a=6 b=0      (elects one, as expected)
2. unacked at prefetch(1)              : 1
   after raising to prefetch(5)        : 1            <-- unchanged
   after raising to prefetch(10)       : 1            <-- unchanged
3. consumer registered after prefetch(5): 5
   original consumer cancelled and re-consumed: 5
4. quorum x-delivery-count seen        : ["absent", 1, 2, 3] then "delivery_limit"
```

## Why the decision goes this way

The two reasons to migrate both weakened when measured.

**The redelivery budget is not a client capability at all.** It is a queue
property. `x-delivery-limit` on a quorum queue is enforced by the broker,
works through the client already in use, and survives the message moving to
another daemon — which is exactly the property an in-process attempt counter
could never have. What `amqplib` adds is *visibility* (`x-delivery-count` in
the headers), which is worth having and is not worth a migration.

**Live credit reduction does not exist in 0-9-1 either.** This is the finding
that settles it. `basic.qos` did not affect a consumer that was already
registered: the unacked count stayed at 1 across two increases. A consumer
registered *after* the change got the new value, and cancelling and
re-consuming applied it to the existing one — which is the same
cancel-and-re-register operation the current design already performs when it
retires a consumer. So `DEGRADED`-as-credit-reduction is not a lever `amqplib`
hands us; it is the same lever, reached the same way, in both protocols.

What remains genuinely better on `amqplib` is that cancelling a consumer with
deliveries in flight is ordinary, well-trodden behaviour there, whereas in this
client it strands deliveries and can stall every link on the connection —
which is why the fleet runs two connections per daemon and why dead-lettering
becomes unreliable afterwards. **That comparison was not measured**, and it is
the one thing that could reopen this decision.

Against that: a migration rewrites `packages/rmq/src/Client.ts` entirely,
gives up RabbitMQ 4's native protocol, and invalidates every property in
`docs/rmq-control-plane.md` until each is re-verified against the new client.

## Consequences

- The daemons keep AMQP 1.0 and the two-connection topology.
- `test/integration/Client.test.ts`'s stranding test stays as the upgrade
  gate: a client release that fixes the bug turns CI red and tells us the
  workaround can go.
- **A redelivery budget becomes available in Phase 2**, when the work queue
  becomes quorum: `x-delivery-limit` replaces "one attempt then dead-letter"
  with "N attempts then dead-letter", with no daemon code change. The claim in
  `README.md` and `docs/rmq-control-plane.md` that a budget cannot be
  expressed is true of the *client* and false of the *system* — Phase 2 owns
  that correction.
- `DEGRADED` stays as "scale the number of active daemons", which is now a
  positive choice rather than a workaround: the alternative costs a
  cancel-and-re-register in either protocol.

## What would change this

- The stranding bug reproducing on a workload we cannot restructure, or
  dead-lettering proving unreliable in production the way it does after the
  stranding test.
- A measured comparison showing `amqplib` cancel-under-load is clean where
  this client is not, at which point the reliability argument outweighs the
  rewrite.
