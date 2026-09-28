# What a circuit breaker is made of

"Add a circuit breaker" usually means adding a library to every service that calls
the flaky dependency. Written that way, the breaker looks like one thing. It is
really four answers that a library happens to give together:

- **What counts as a failure.** A `503` is the third party failing. A `429` is it
  asking you to slow down. A `422` is it refusing this one request.
- **Where the state lives.** In each process's memory, in something the fleet
  shares, or in a control plane.
- **What "open" does.** It rejects calls locally, or it stops taking work at all.
- **Who decides to try again.** A timer in every replica, or one probe the whole
  fleet agrees on.

With the four pulled apart, each can be given a better answer than the default.
This series does that one step at a time on the same system: a producer, a
RabbitMQ work queue and a fleet of competing consumers calling a flaky third
party. Each step is a branch with its own code and measurements, and each link
below opens that branch's README, which holds its diagram.

| | Where the state lives | What open does | Measured in an outage |
| --- | --- | --- | --- |
| [01 · No breaker](../../blob/article/01-base-scenario/README.md) | nowhere | nothing: every message spends its 3 attempts | ≈ 4,000 dead-lettered in 20 s |
| [02 · A breaker in every process](../../blob/article/02-in-process-breaker/README.md) | each replica's memory | rejects locally, spending the message's attempts | 1,577–2,246 dead-lettered in 15 s; ~27 openings for one outage |
| [03 · Coordinated through the broker](../../blob/article/03-rabbitmq-coordination/README.md) | each replica, plus a shared probe permit | releases the message without spending an attempt | 0 dead-lettered in 40 s; one probe at a time instead of 6 |
| [04 · Held by the broker](../../blob/article/04-rabbitmq-only-breaker/README.md) | RabbitMQ: a consumer on or off, a token in a delay chain | stops consuming; the work waits in the queue | 0 dead-lettered where cockatiel lost 2,745 |

Two things carry through the steps:

- **The breaker gets simpler, not bigger.** By article 4 the breaker keeps no
  state of its own. The broker already holds messages durably, delays them and
  elects one consumer among many, and the breaker is built from those.
- **Most of the loss came from what open did, not from the outage.** In article
  2, 52–59 real failures cost 1,577–2,246 dead letters, because every call an
  open breaker turned away still spent one of the message's delivery attempts.

Article 5 (`article/05-platform-control-plane`) does the same at platform level:
Envoy enforces, and an aggregator publishes one verdict per API as events. It
costs about three times the code, and pays only when other systems act on the
verdict.
