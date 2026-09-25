# 012 — Durable workflows: evaluated, not adopted

**Status**: decided 2026-09-10.

## Decision

Do not use `effect/unstable/workflow`.

## Why

- **The unit of work is one call.** There is no second step to resume before
  and nothing to compensate.
- **Durability is already the broker's.** A workflow engine would be a second
  record of "has this work been done" beside the unacked delivery, and the two
  would have to agree across a crash.
- **The breaker is a fold, not a sequence.** Its transitions are decided by
  observations arriving every 250 ms, not by reaching a next step. The
  durability it needs — resuming a sequence across failover — is the fenced
  checkpoint.
- **Real durability needs SQL.** `WorkflowEngine.layerMemory` says it is not
  for durable use; `ClusterWorkflowEngine` needs `Sharding` and
  `SqlMessageStorage` — a third stateful dependency.

## What would change it

Work with several side-effecting steps (charge, then write the ledger, then
notify). Redelivery would replay steps that already succeeded, and a table of
"already charged" ids is a workflow engine with the durability left out.
