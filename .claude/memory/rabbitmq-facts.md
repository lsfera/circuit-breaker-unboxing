---
name: rabbitmq-facts
description: RabbitMQ 4.x behaviours measured on /workspace that the docs or argument names don't make obvious.
metadata:
  type: reference
---

- `x-overflow: reject-publish` nacks the refused publish on the publisher's confirm; expect it where losing a
  race is normal.
- `x-max-length` counts only ready messages: a held-unacked token lets a second one in. Return a token by
  publish-then-ack, never a requeuing nack.
- Quorum queues default to `x-delivery-limit` 20, and without a dead-letter target they drop at the limit;
  terminal queues need `-1`. From 4.3 a requeuing `reject` counts toward it and a requeuing `nack` does not.
- 4.3 refuses a transient non-exclusive queue by closing the connection.
- Changing a queue's arguments means deleting it; a mismatched redeclare is `406 PRECONDITION_FAILED`.
  Declarers must agree on exchange `durable` too.
- RabbitMQ sizes its memory watermark from host RAM, ignoring the cgroup limit; `infra/rabbitmq.conf` sets it.
- `consumer_capacity` reads 0 for every quorum queue.

Related: [[working-style]].
