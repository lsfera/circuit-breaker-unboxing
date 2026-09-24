---
name: article-series-branches
description: The article/NN-* branch lineup after the 2026-09-23/24 merges and renumbering (04 = master, platform level), and how changes move between them.
metadata:
  type: project
---

The branch lineup, as of 2026-09-24:

- 01-base-scenario
- 02-in-process-breaker
- 02-rabbitmq-only-breaker: the breaker held by RabbitMQ (delay-chain token), kept as 02 by the user's choice. Since 2026-09-24 it carries everything 03 has (permit, redrive, fleet-view rule, the 429 classification and Limiter.ts), each measured again on this design, plus a same-harness comparison with 03 in its README.
- 03-rabbitmq-coordination: probe permit, counted attempts and redrive, the fleet view as Prometheus rules (infra/monitoring/rules.yml, with a 10s freshness filter), and, merged in from 04 on 2026-09-24, the 429 work: `throttled` classification, the AIMD limit (Limiter.ts), and the failure-rate negative result.
- 04-platform-control-plane: created 2026-09-24 from master (no ancestry change; master stays). The last article: Envoy + aggregators + published per-API events, framed as "scaling the design at platform level", with entry criteria (other systems act on the verdict, visible hosts for DEGRADED, shared egress for many services, many APIs). The user's assessment: for a consumer-only daemon 03 / 02-rabbitmq-only are the right compromise; master is ~4× the code and only pays off at platform level.
- (old) 04-429-backpressure: merged into 03 and ported to 02-rabbitmq-only-breaker on 2026-09-24, then deleted at the user's request (was 27443d8d1; reflog only).

History:

- 2026-09-23: old 05+06 and old 07+08 were merged, and the bounded-queue article was dropped. The user holds that a bounded queue is an anti-pattern.
- 2026-09-24: old 03+04 were merged. The user's reason: both use RabbitMQ features the project already had.
- 2026-09-24: the aggregator was dropped from articles 1–3; it returns only as article 4 (master), the platform-level design.
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

**How to apply:** when a change should land in every article, cherry-pick it along 01→03 (04 is master-derived and structurally different). Then grep each branch for stale article numbers and branch names.

Related: [[chaos-test-new-components]], [[rmq-control-plane-design]].
