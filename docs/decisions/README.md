# Decision records

One per decision worth arguing with later: what was chosen, over what, and the
measurement it rests on.

| | Decision | Rests on |
| --- | --- | --- |
| 001 | [Which AMQP client](001-amqp-client.md) — *superseded by 004* | a spike against both clients |
| 002 | [The aggregator's `OPEN` is observational](002-enforcement-authority.md) | who still serves requests when the control plane is down |
| 003 | [Distributed tracing, tail-sampled](003-tracing.md) | what a span shows that a metric does not |
| 004 | [Down to AMQP 0-9-1, on amqplib](004-downgrade-to-amqp-0-9-1.md) | the stall that did not reproduce |
| 005 | [The client recovers its own connection](005-connection-recovery.md) | a broker restart with every container staying up |
| 006 | [Absence, failure and identity](006-representing-absence.md) | a lease token that was not fenced; an outbox that sent twice |
| 007 | [One declaration per message](007-message-contracts.md) | three probes from one malformed trigger |
| 008 | [Configuration is a boundary](008-configuration-is-a-boundary.md) | a typo that idled the whole fleet |
| 009 | [What the quorum is a quorum of](009-what-the-quorum-is-a-quorum-of.md) | three silent ways a replica left |
| 010 | [A proxy that fails on its own behalf](010-a-proxy-that-fails-on-its-own-behalf.md) | 22,226 dead letters with the circuit closed |
| 011 | [The concurrency ceiling belongs to the broker](011-the-ceiling-belongs-to-the-broker.md) | 340 messages parked in a JS array |
| 012 | [Durable workflows: not adopted](012-durable-workflows.md) | one call per message |
| 013 | [The target as a fraction](013-the-target-as-a-fraction.md) | 5,000 simulated fleets |
| 014 | [Every service declares what it may use](014-the-measurement-envelope.md) | the same run with and without limits |
| 015 | [The console at a thousand APIs](015-the-console-at-a-thousand-apis.md) | a recorded stream through five encodings |
| 016 | [The retry budget travels with the message](016-the-retry-budget-travels-with-the-message.md) | 1,570 messages dropped by the dead-letter queue's default limit |
| 017 | [Detecting a lost control plane inside the lease](017-a-heartbeat-off-the-delivery-channel.md) | two-leader window from 4 s to 0 s |
| 018 | [Control flow as expressions; the message says what it is](018-control-flow-as-expressions.md) | the broker suite before and after |
| 019 | [What a daemon does when it does not know the circuit](019-a-daemon-that-does-not-know.md) | a daemon that assumed CLOSED at start and believed OPEN for ever |
| 020 | [What one API costs the broker](020-what-one-api-costs-the-broker.md) | 0.93 MB per API on an idle node |
