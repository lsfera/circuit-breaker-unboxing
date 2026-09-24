---
name: article-series-branches
description: The article/NN-* branch lineup after the 2026-09-23/24 merges and renumbering, and how changes move between them.
metadata:
  type: project
---

The branch lineup, as of 2026-09-24:

- 01-base-scenario
- 02-in-process-breaker
- 02-rabbitmq-only-breaker: kept as a separate alternative approach, by the user's choice.
- 03-rabbitmq-coordination: probe permit, counted attempts and redrive, the fleet view as Prometheus rules (infra/monitoring/rules.yml, with a 10s freshness filter), and, merged in from 04 on 2026-09-24, the 429 work: `throttled` classification, the AIMD limit (Limiter.ts), and the failure-rate negative result. The last article.
- 04-429-backpressure: merged into 03 on 2026-09-24; kept until the user decides to delete it.

History:

- 2026-09-23: old 05+06 and old 07+08 were merged, and the bounded-queue article was dropped. The user holds that a bounded queue is an anti-pattern.
- 2026-09-24: old 03+04 were merged. The user's reason: both use RabbitMQ features the project already had.
- 2026-09-24: the aggregator was dropped.
- 2026-09-24: 04 was renamed 04-429-backpressure, then merged into 03.
- 2026-09-24: the failure-rate breaker (EitherBreaker) was dropped. It cost 7,000–9,000 good calls to avoid ~750 bad ones, and its table now opens the shedding article.

The user prefers fewer, denser articles and lean READMEs.

Older notes that cite article numbers may use earlier numbering. For the original 11 articles, the mapping from old to new is:

- 1 → 1
- 2 → 2
- 3 → 3
- 4 (fleet verdict) → dropped
- 5 → 3
- 6 → 3
- 7 → dropped
- 8 → dropped
- 9 → dropped (its measurements are in 4)
- 10 → 4
- 11 → dropped

The branches share no git ancestry except the repos/effect subtree commit, so a change is carried forward by cherry-picking it. Every README diagram marks what is new with an amber classDef. `.claude/memory` is tracked in git, so this note exists only on the branches it was committed to.

The user's position on bounded queues: a length is a poor proxy for age, and a broker nack turns backpressure into loss. For staleness, use a per-message deadline plus parking, and refuse at the edge. Don't propose `x-max-length` as a staleness or load fix.

**Why:** there is no remote. Deleted branches are recoverable only from the reflog.

**How to apply:** when a change should land in every article, cherry-pick it along 01→04. Then grep each branch for stale article numbers and branch names.

Related: [[chaos-test-new-components]], [[rmq-control-plane-design]].
