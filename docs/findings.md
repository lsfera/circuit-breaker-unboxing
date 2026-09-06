# What the build surfaced

The long form of every finding this repo produced. [journey.md](journey.md)
tells the story these belong to and summarises them in a table; this is the
detail behind each row.

Thirteen things worth knowing — twelve found by running the thing, one by
reading it afterwards:

- **A reason code cannot be derived from averaged endpoint counts.** With four of
  five replicas seeing zero healthy hosts, the mean rounds to 1, so "all
  endpoints gone" silently became false. The check is now
  `votes.DOWN === live.length` — unanimous, not merely quorate.
- **Ejection backoff outlives the outage.** `base_ejection_time × ejection_count`
  walks up to the `max_ejection_time` cap, so a recovered upstream stays ejected
  long after it heals. The fix is not shorter timers, it is
  `successful_active_health_check_uneject_host` — and that flag does nothing
  on its own. It needs an actual `health_checks` block, which the config was
  missing for a long time while the README claimed the problem was solved.
  With one configured: six hosts carrying ~25s of accumulated ejection
  backoff (34 enforced ejections between them) came back **3.1s** after the
  upstream recovered.
- **Snapshots are not duplicates.** The first delivery-integrity check counted
  them as such, because they deliberately repeat the current sequence. Only
  `state_changed` is sequence-checked; snapshots exist precisely to be re-applied
  harmlessly.
- **`event_log_path` lives under `cluster_manager.outlier_detection`,** not on
  the cluster. Easy to get wrong from memory.
- **Fencing per-key is not fencing.** The first version of `CheckpointStore`
  rejected a stale write only if a *newer* write had already landed for that
  same API. That is not a fencing guarantee — it just means a stale leader
  wins by default on any API the new leader has not gotten to yet, which is
  the exact split-brain window fencing tokens exist to close. The fix is
  checking against the one shared lease-token counter, not a per-key value —
  see [High availability](high-availability.md).
- **A healthy-looking process can be a deaf one.** A daemon in the RabbitMQ
  fleet stopped reacting to circuit events entirely — container up, CPU at
  0.01%, sockets and file descriptors identical to a healthy peer, and 64
  undelivered messages behind a consumer the broker still considered
  registered. Closing a consumer while the broker has deliveries in flight
  strands them, and enough strandings stall *every* link on that connection.
  Two fixes came out of it: the daemon now keeps its control plane on a
  connection that never closes a link, and it logs a heartbeat independent
  of the event stream — because until then every log line it produced was
  emitted while handling an event, so "gone deaf" and "nothing happened"
  looked identical. See
  [docs/rmq-control-plane.md](rmq-control-plane.md).
- **Backpressure is about *when you ack*, not how much you buffer.** The
  first daemon accepted each message on arrival and fired its third-party
  call afterwards, so draining a 50k backlog meant tens of thousands of
  concurrent requests from one process — the herd the fleet policy exists to
  prevent, self-inflicted. Capping concurrency and dropping the excess was
  worse (32,000 messages shed in one drain). Deferring the AMQP accept until
  the call settles is what actually works: credit stops refilling, the
  broker stops pushing, and the backlog stays in the queue where it is
  visible.
- **A documented sandbox limitation turned out to be a misconfigured
  environment variable.** "Docker cannot bind-mount the project directory
  here" had been true every time it was checked — until `HOST_WORKSPACE_FOLDER`
  got explicitly re-exported to a container-internal path while debugging
  something unrelated, silently shadowing the correct host path the
  devcontainer had already set. The fix was not a workaround, it was
  removing the override — three real Envoy replicas, two real aggregator
  instances, and a real Redis now run end to end (see
  [Running against real Envoy](operations.md#running-against-real-envoy)). The lesson
  travels further than this one variable: re-verify an environment
  assumption before designing around it, especially one written down as
  fact by an earlier pass over the same repo.
- **A demoted leader that comes back is not a cold start, and that broke the
  one guarantee this repo is about.** Rehydrating `sequence` from the Redis
  checkpoint only ever fired for APIs an instance had never seen — which is
  right on a fresh takeover and wrong on a *re*-takeover, because the
  instance still holds its own breakers in memory from last time while
  whoever led in between has already published past them. The result is two
  different payloads under one sequence number: exactly the break
  `/api/subscriber` exists to detect, invisible to every test because both
  failover tests used a freshly built instance. Losing the lease now drops
  the registry, so re-promotion takes the rehydrate path that was already
  tested, and `Coordination.test.ts` keeps one instance alive across a full
  demotion to pin it.
- **There is no way to tell this AMQP client "this attempt failed, try
  again".** `requeue()` sends `modified{delivery_failed: false}` and
  `discard()` sends `modified{delivery_failed: true, undeliverable_here:
  true}` — nothing in between. RabbitMQ increments AMQP 1.0's
  `delivery-count` only for a delivery marked failed, so releasing the same
  message three times returns `delivery_count: 0` three times, and a
  redelivery budget that outlives the message moving to another consumer
  cannot be built. Same shape as the link-credit finding: the lever is
  genuinely absent rather than merely undocumented, so the daemons
  dead-letter on the first failure and a test pins the behaviour in case a
  client release changes it. **Amended 2026-09-05**: that is a fact about the
  client, not about the system — a quorum queue with `x-delivery-limit` makes
  the *broker* count attempts and dead-letter at the limit, through this same
  client (measured: four deliveries, then `reason "delivery_limit"`). The
  lever was in the queue the whole time. See
  [docs/decisions/001-amqp-client.md](decisions/001-amqp-client.md).
  **Amended again 2026-09-06**: the repo moved to `amqplib` (AMQP 0-9-1), where
  `x-delivery-count` is simply readable — the same test now reads
  `[0, 1, 2, 3, 0, 1, 2, 3]`, the budget and the redrive's reset. Enforcement
  stays the broker's, because an in-process counter still dies when a message
  moves to another daemon; what is gone is the blind spot. See
  [decisions/004](decisions/004-downgrade-to-amqp-0-9-1.md).
- **The check that proves the contract had no test, and a blind spot for the
  one bug most likely to break it.** `/api/subscriber` is what this README
  points at to claim the stream is gapless and non-repeating. It counted a
  *repeated* sequence as a duplicate and a *skipped* one as a gap — and let a
  sequence going **backwards** fall through both branches, uncounted. That is
  exactly the shape a leadership bug produces: an instance resuming from
  stale in-memory state republishes numbers a later leader already used. So
  the demotion bug found the same day would have been invisible to the very
  check that exists to catch it, and the function had no unit test at all
  despite being pure, twenty lines, and load-bearing. It has six now. The
  lesson is not "write more tests" — it is that a verification mechanism is
  itself code, and an untested one is a claim, not a proof.
- **A one-minute dependency outage killed the control plane permanently, and
  every health signal stayed green.** The Redis calls used `Effect.promise`,
  which turns a rejected promise into a *defect* rather than a failure — and a
  defect out of the tick terminates `Effect.repeat`, so the loop was simply
  gone. Measured on the running stack: 55 seconds without Redis stopped the
  loop after seven more ticks, it never restarted when Redis returned, and a
  total upstream failure afterwards published nothing at all. Both instances,
  because a shared dependency fails for everyone at once. Meanwhile both
  processes served HTTP 200, `/api/state` still reported `isLeader: true` with
  three APIs, and every gauge held its last value — which is indistinguishable
  from a system where nothing is happening. Three things were wrong and all
  three are worth naming: an expected failure was modelled as a defect; the
  loop had no supervision, so its death was unobservable; and there was no
  metric that moves when the loop runs, so nothing could have alerted. Now:
  coordination failures are typed and a tick that cannot reach the coordinator
  stands down and retries (an instance that cannot confirm it holds the lease
  must not act as leader), a defect logs fatally and exits so the restart
  policy does its job, and `egress_aggregator_ticks_total` plus a
  `ControlLoopStalled` alert make the silence audible. Same 55-second outage
  after the fix: the loop kept ticking, recovered on its own, and published
  the next incident at `sequence=102` with no gap.
- **The guarantee this repo is about stopped one hop short of the party it is
  for.** Inside the aggregator the per-API sequence is gapless and strictly
  ordered, proven by tests at three levels. At the edge it was not: a webhook
  that failed its three retries went into a 200-entry in-memory list that
  exists to be *read*, not replayed, and that dies with the process. So a
  subscriber down for longer than about a second lost whatever happened while
  it was away, permanently, and nothing in the system disagreed — the
  aggregator's own counters called it delivered-then-dead-lettered and moved
  on. `Outbox.ts` is the durable half: the same `RedisLike` port the lease
  uses, replay in order on the instance that holds the lease, bounded per API.
  The failure mode worth naming is the one the drain deliberately refuses:
  skipping a stuck event to deliver the ones behind it would manufacture
  exactly the gap this system exists to prevent, and it would look like
  progress.
- **"Stands down and retries next tick" was only true if the call came back.**
  The aggregator's answer to an unreachable coordinator — demote, count it, try
  again — had only ever been tested by stopping Redis for everyone. Pointing
  *one* instance's `redis` at a black hole while the other kept working
  produced something else entirely: the client queues commands against a
  connection it cannot establish and retries a request across twenty
  reconnection attempts, so the promise never settled and the tick that was
  supposed to fail fast simply blocked. Measured: **two ticks in twenty-five
  seconds and then nothing**, a single coordination error, and the instance
  neither leading nor standing down. `/livez` was right about it — 503, a
  control loop that has stopped — which is the Phase 4 endpoint earning itself
  inside a week. Coordination calls are bounded at 1s now, well under
  `leaseTtlMs` so a leader can fail a call and still renew in time, and the
  Redis client is configured to fail rather than queue. Same partition
  afterwards: 4 ticks/s sustained, one error per tick, and the other instance
  holding the lease throughout.
- **The alert for the worst failure this system can have fired because
  Prometheus was scraping one process twice.** `SplitBrain` — two aggregators
  believing they hold the lease — went off with exactly one leader running. The
  scrape config had a `host.docker.internal:8088` target so the same monitoring
  stack could also watch a sim fleet started on the host, with a comment
  calling it "harmless if nothing is listening there". It stopped being
  harmless the moment something was: the compose aggregator publishes 8088 to
  the host, so that target and `aggregator:8088` were the *same process* under
  two instance labels, and `sum(egress_aggregator_is_leader)` read 2. The
  target is gone, and the fleet-wide rules aggregate `by (deployment)` so a
  future mixed setup cannot reproduce it. The lesson is not about Prometheus:
  an alert that fires falsely on its first outing teaches everyone to ignore
  the one thing it exists to say.
- **The gauge that says who leads was *absent* on the instance that could not
  lead, rather than zero.** `egress_aggregator_is_leader` is only written after
  an acquire attempt returns, so an instance that has never reached the
  coordinator publishes no series at all — and `max(egress_aggregator_is_leader)
  == 0`, which is how anyone would write the "nobody is leading" alert, cannot
  fire on a metric that does not exist. Found while watching the partitioned
  instance above: the gauge did not drop, it vanished. The stand-down path sets
  it to 0 explicitly now.
- **A lease that is only ever surrendered by expiring turns every deploy into
  an outage-shaped event.** `LeaderElection.release` was written in the first
  version of `Coordination.ts` and never called from anywhere — grepping the
  aggregator for `.release(` returned nothing — so leadership moved only when
  a lease timed out. Correct for a crash; paid on every planned stop, forever.
  Measured on the running stack: `docker kill` of the leader put the standby
  in charge after 4952 ms, the full `leaseTtlMs`, while a `docker compose
  stop` now does it in 234 ms. The interesting part is not the twenty-fold
  difference, it is that nothing was broken — the primitive existed, the
  interface exposed it, the tests passed, and no code path connected the two.
  A capability nobody calls is indistinguishable from one nobody built.
- **A dead-letter queue that does not outlive the broker is a dead-letter
  queue in name only.** Every queue the RabbitMQ fleet declared was
  transient, because `durable: false` was hardcoded in the client wrapper —
  a default nobody had ever decided on. Rejecting a failed message therefore
  preserved it exactly as long as the broker process lived: measured on the
  running stack, a dead-letter queue holding 24 messages held **0** after
  `docker compose restart rabbitmq`. What makes it worth naming is that
  nothing looks wrong afterwards — the first daemon back redeclares the queue
  with the same name and the same arguments, so a dead-letter queue that lost
  everything and one that never received anything are the same queue from the
  outside, in the management UI and in the metrics alike. Messages were also
  published without the durable header, which on its own would have moved the
  loss one level down and left the fix looking like it worked. The decision
  is now explicit and made in one place (`ControlPlane.ts`, so the producer
  and the daemons cannot disagree): work and dead-letter queues are durable
  because nothing can reconstruct them, control and election queues stay
  transient because a restarting daemon relearns the state from the next
  snapshot. A real broker restart is in the test suite now, asserting both
  halves — the contrast is the point.
- **A fencing token that can go backwards is not a fence.** The lease token
  came from Redis `INCR`, so a coordinator that lost its own state — a
  restart without persistence, a failover to an empty replica — began issuing
  from 1 again. A leader paused across that moment still held token 5, and
  the checkpoint script's `attempted < current` test then read `5 < 1`, which
  is false: the stale leader was waved through and overwrote the live one's
  checkpoints. The same split brain as the per-key fencing bug above, reached
  with no race at all — just by making the counter smaller. Tokens are now
  `<epoch>:<counter>`, the epoch minted by whichever coordinator finds no
  state to inherit, and they are ordered only within an epoch: a token from
  before the wipe is not a low number, it is an unrecognisable one. The Lua
  script requires the epoch *and* the counter to have survived together,
  deliberately — an epoch that outlived its counter would let the counter
  restart inside an epoch that stale leaders still recognise, which is the
  same bug wearing a disguise. Pinned against a real Redis by deleting its
  keys mid-test.
