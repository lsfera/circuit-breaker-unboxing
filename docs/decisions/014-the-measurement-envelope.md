# 014 — Every service declares what it may use

**Status**: adopted 2026-09-12.

## Decision

- Every compose service declares `deploy.resources.limits`.
- `infra/instrument.mjs` samples Docker's stats stream once a second per
  container and **refuses to produce a record** from a stack with an unlimited
  service, an OOM kill, CPU throttling past its budget, or a failed scenario.

## Why

An unlimited container is sized by the host. The same demo, same machine,
without and with a 512 MiB limit:

| | no limit | limited |
|---|---|---|
| rmq-daemon | 121 MiB | 81 MiB |
| aggregator | 106 MiB | 74 MiB |
| rabbitmq | 391 MiB | 324 MiB |
| envoy | 35 MiB | 34 MiB |

Everything with a garbage collector shrank; Envoy and Redis did not.

## Gotchas

- **CPU quotas are per 100 ms period**, so a process averaging half its limit
  is still throttled in some periods. The harness allows 1% of periods, and the
  load fixtures get a full core so the run measures the system, not the
  generator.
- **RabbitMQ ignores its cgroup limit** and sizes its watermark from host RAM.
  `infra/rabbitmq.conf` sets it (1 GiB of a 2 GiB limit);
  `RABBITMQ_VM_MEMORY_HIGH_WATERMARK` is gone in RabbitMQ 4.
- **Per-container peaks do not add up**: 3.3 cpus summed, 1.65 in the stack's
  busiest actual second. Samples are bucketed by second.

## Limits

A ceiling is not a reservation, and nothing pins CPU speed or architecture;
each record stamps the host so comparisons know what they compare.
