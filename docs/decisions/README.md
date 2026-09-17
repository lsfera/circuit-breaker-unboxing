# Decision records

One per decision that was hard enough to be worth arguing with later. Each
states what was chosen, what it was chosen over, and — the part that makes them
worth keeping — the measurement it rests on, so a reader can disagree with the
evidence rather than with the conclusion.

Several have dated amendments below their original text. The text is not
rewritten when a decision changes: what the decision was, and why it stopped
being right, are both the record.

| | Decision | Rests on |
| --- | --- | --- |
| 001 | [Which AMQP client the daemon fleet uses](001-amqp-client.md) | a bounded spike against both clients, same broker |
| 002 | [The aggregator's `OPEN` is observational, not authoritative](002-enforcement-authority.md) | who is still serving requests when the control plane is down |
| 003 | [Distributed tracing: available, deliberately not wired](003-tracing.md) | what a span would have shown that a metric did not |
| 004 | [Down to AMQP 0-9-1, on amqplib](004-downgrade-to-amqp-0-9-1.md) | the two client bugs that survived every workaround |
| 005 | [The client recovers its own connection](005-connection-recovery.md) | a probe showing a forked defect does not end `Layer.launch` |
| 006 | [How absence, failure and identity are represented](006-representing-absence.md) | the bugs each sentinel produced before it was removed |
| 007 | [One declaration per message, read at both ends](007-message-contracts.md) | a malformed POST that disabled gap detection for an API |
| 008 | [Configuration is a boundary too](008-configuration-is-a-boundary.md) | `process.env` reads that bypassed every validated setting |
| 009 | [What the quorum is a quorum of](009-what-the-quorum-is-a-quorum-of.md) | three ways a replica left the denominator in silence |
| 010 | [A proxy that fails on its own behalf](010-a-proxy-that-fails-on-its-own-behalf.md) | every error the proxy served that it made itself |
| 011 | [The concurrency ceiling belongs to the broker](011-the-ceiling-belongs-to-the-broker.md) | an in-process gate that was the buffer it denied being |
| 012 | [Durable workflows: evaluated, deliberately not adopted](012-durable-workflows.md) | what Temporal would have replaced, and what it would not |
| 013 | [The target as a fraction, not a count](013-the-target-as-a-fraction.md) | 5,000 simulated fleets, then a real one: ±60% at five daemons, ±16% at a hundred |
| 014 | [Every service declares what it may use](014-the-measurement-envelope.md) | the same demo run with and without limits: every garbage-collected process a third smaller |
| 015 | [The console at a thousand APIs](015-the-console-at-a-thousand-apis.md) — *steps 1–2 built, 3–5 proposed* | a recorded stream replayed through five encodings; a hundred consoles took up to a fifth of the control loop's cadence, and after sharing the frame take none of it |
| 016 | [The retry budget travels with the message](016-the-retry-budget-travels-with-the-message.md) | a kill-broker audit — 0 lost, 18 duplicate calls under a producer-assigned key — and the sweep's first pass against a 22,246-message backlog |
| 017 | [A heartbeat off the delivery channel](017-a-heartbeat-off-the-delivery-channel.md) — *split-brain window narrowed, not closed* | a live split-brain window shrunk from unbounded to 4s, a shared-channel heartbeat producing a real duplicate, and the fix that removed it |
