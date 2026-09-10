# What the build surfaced

The long form of every finding this repo produced. [journey.md](journey.md)
tells the story these belong to and summarises them in a table; this is the
detail behind each row.

Nearly every one of these was found by *running* the thing rather than by
reading it — and the handful that were not were confirmed by running it
afterwards. No count here, deliberately: the list grows, and a number beside
it is one more pair of things that have to agree.

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
- **A counter that reads its source twice loses whatever happens in
  between.** Each daemon counts control-plane events in a plain map, and a
  flush loop publishes the difference to Prometheus once a second — counting
  cheaply on the hot path and paying for the metric elsewhere, which is the
  right shape. But publishing *suspends*: `Metric.update` is an Effect, and
  the loop then recorded what it had sent by re-reading the live map
  afterwards (`flushedEvents = new Map(eventsByType)`). Anything counted
  during those suspensions landed in the second read but not the first, so it
  was marked as published without being published — and because the mark had
  moved, no later flush would ever pick it up. Reproduced with a single
  control event arriving mid-flush: observed 2, published 1, lost for good.
  The numeric counters beside it escaped the same bug by accident, their two
  reads sitting in adjacent *synchronous* statements where nothing can
  interleave. The fix is structural rather than careful: take one immutable
  snapshot, derive both the delta and the new high-water mark from that single
  reading, and let publishing suspend as much as it likes
  ([Tally.ts](../packages/rmq-consumer/src/Tally.ts), pure and tested).
  Checked against a real incident afterwards — every daemon's published
  `egress_daemon_control_events_total` now equals the count it keeps
  independently for its own heartbeat line, exactly.
- **A quorum is a fraction, and nothing was watching the denominator.** Every
  state this system publishes comes from `votes.DOWN / live.length`, where
  `live.length` is however many replicas reported inside `replicaTimeoutMs`.
  There is no floor, so a replica leaving does not make the verdict smaller —
  it changes what the verdict *means* at the same value. Three replicas
  reporting means a 0.6 quorum needs two to agree; one replica reporting means
  that replica is unanimous by itself, and a fleet-wide guarantee has quietly
  become one instance's opinion, published with the same confidence and the
  same event contract. `EnvoyPushSource.ts` already named this hazard in its
  own module doc — "a quorum computed from a partial fleet, which is worse than
  no data because it looks like data" — and the fix it describes closes exactly
  one cause of it. Three others were found by reading the ingestion path, all
  silent: a replica pushing with no node id was discarded message by message
  (up and connected, absent from the fleet, nothing said); a stream that
  stopped expired with nothing said and its entry left in the map; and the
  polling path's `Effect.catch(() => [])` dropped an unreachable replica
  without a word. All three are counted on
  `egress_fleet_replica_lost_total{reason}` now and logged once per departure —
  once, not per failed poll, because at `tickMs` of 250ms the honest version of
  this is four identical lines a second. The arithmetic is deliberately
  unchanged, and
  [decisions/009](decisions/009-what-the-quorum-is-a-quorum-of.md) says why:
  enlarging the denominator would suppress a legitimate `OPEN` when replicas
  genuinely die, which hides the failure the system exists to catch. Verified by
  stopping a real Envoy replica: one warning, the counter at 1, the gauge 3 → 2
  — and then by killing the *leader*, which showed the standby had been
  receiving pushes all along (it logged the same departure once on promotion and
  reported the two survivors immediately), confirming the one-sink-per-aggregator
  claim live.
- **A missing gauge was decoded as "every host is gone".** `parseStats` read
  Envoy's two membership gauges with `?? 0`, and a test pinned that as correct
  under the name "missing stats default to zero rather than NaN". Avoiding the
  `NaN` was the right instinct; the zero was not, because zero is not a neutral
  value in this domain. `Breaker.voteOf` reads `total > 0 && healthy === 0` as
  `DOWN` — so a stat set that arrived carrying `membership_total: 6` and no
  `membership_healthy` did not degrade gracefully, it voted for a total
  outage. Measured against the real pure code with that stat set from three
  replicas: three DOWN votes, candidate OPEN, and `CLOSED -> OPEN` published
  with reason `ALL_ENDPOINTS_EJECTED` — a fleet-wide outage declared for an
  upstream about which nothing had actually been reported. It is the exact
  mirror of this document's first entry, where averaging endpoint counts made
  "all endpoints gone" silently *false*; here the absence of a number made it
  silently *true*. A cluster missing either gauge is now not reported at all,
  which is not the same as reporting it healthy — the replica abstains on that
  API, the caller is told which cluster and why, and it is counted as one more
  way of leaving a quorum (`reason="incomplete-stats"`). The four remaining
  `?? 0` defaults stay: ejections are surfaced rather than voted on, and the
  overflow counters are edge-detected as deltas, where zero reads as "nothing
  new" rather than as a state.
- **Recovery covered the connection, not the channel.** amqplib reconnects and
  this client rebuilds its topology and consumers from the `setup` hook, which
  is the failure everyone thinks of. Every consumer here has its own channel
  though, and a channel can die alone — a protocol error, a queue deleted
  underneath it, a settle on a delivery tag the broker has already seen. The
  connection stays healthy, so `disconnect` never fires, `setup` never runs, and
  the restart policy never sees anything; the `Consumer` handle the caller holds
  still looks live, and the daemon's own reconciliation compares against it and
  concludes there is nothing to do. Measured by closing a consumer's channel out
  from under it against a real broker: the next message never arrived, and not
  one line was logged. The same deaf daemon this repo has an alert and a runbook
  for, reached without any connection loss. Consumers are rebuilt on a fresh
  channel now. Both directions are pinned: a channel that dies alone comes back,
  and a consumer retired on purpose is *not* resurrected by a reconnect, which
  was the other half of this and had no test either.
- **A repair budget that punished the queues it was meant to protect.** The
  rebuild above first shipped with a bound — five attempts, reset by a delivery —
  and a guard that skipped the repair when the connection was what went. Both
  were reasoned, not measured, and measuring them found the reasoning wrong in
  opposite directions. The guard never fired: a channel's `close` arrives before
  the connection's `disconnect`, so the flag still read `true` at the moment it
  was consulted, and the rebuild attempt it was meant to prevent happened anyway
  — harmlessly, because `createChannel` on a closed connection rejects at once.
  The budget did fire, on exactly the wrong queues. The two SAC election queues
  are idle *by design* — being registered and empty is their whole job — so a
  budget reset by deliveries never reset on them, and six channel deaths over
  the life of a process dropped that daemon out of the election for good, over a
  condition the next rebuild fixed immediately. Measured: after the sixth close
  the consumer was abandoned and the election trigger that followed was never
  received; unbounded, the same run receives it. The repair is unbounded now,
  deliberate shutdown is expressed with the `live`/`forget` bookkeeping that
  already existed rather than a second flag, and the flag survives only where it
  is honest — deciding whether a *failed* rebuild is worth reporting, which runs
  late enough for it to be true.
- **One published contract, three readers, two of them guesswork.** The event
  this repo exists to publish crosses three boundaries — AMQP to the daemon
  fleet, Redis to the outbox that replays it after a restart, SSE to a
  subscriber — and each hop had grown its own idea of what a valid event is.
  Only the AMQP one decoded. The outbox cast: `JSON.parse(raw) as CircuitEvent`
  followed by two `typeof` tests, checking two of the event's nine fields and
  asserting the rest, under a comment claiming "only the shape the publisher
  guarantees is let through". Measured against the old check, it would have
  replayed a `state` that does not exist, a `reason` outside the vocabulary, a
  sequence that cannot be ordered, and an object missing seven of its nine
  fields — straight to subscribers, from the durable path that exists precisely
  to survive the version skew that produces such entries. The subscriber's hop
  decoded properly but reached it through a bare `JSON.parse`, so a frame that
  was not JSON at all threw out of the stream and ended the process, which is
  the failure the AMQP reader had already been given two distinct outcomes to
  avoid. There is one reader now, in `@egress/domain` beside the schema it
  reads, and all three hops go through it — so the tests that pin what a valid
  event is now guard three call sites instead of one.
- **The work most worth tracing was the only work that arrived untraced.** A
  redrive replays messages that failed their third-party call and spent their
  delivery budget — the ones someone would actually want to follow — and it
  republished the body alone. The `traceparent` was not missing: RabbitMQ keeps
  application headers through dead-lettering, so it was sitting on the message,
  readable, and thrown away on the way out. The replay then arrived at a daemon
  with no parent, took the untraced fast path, and looked like a brand new unit
  of work with no history. A replayed message now rejoins the trace that
  produced it, under a `work.redrive` span, and only a message that carried a
  parent pays for one — the same gate the daemon's egress call uses. Measured
  both halves: a dead-lettered message still reports a parent, a republish of
  the body alone does not, and one carrying the header does. Also measured, and
  the reason the span is safe as the mechanism: `Effect.withSpan` inherits the
  parent's trace id and yields a valid `traceparent` even with **no tracer
  installed**, which is the default here — so the link survives in deployments
  that export nothing.
- **`Effect.runPromise` inside a message handler builds a runtime per message.**
  `daemon.ts` documents this and captures its context once
  (`Effect.runPromiseWith(services)`); `Redrive.ts`, extracted from it, kept the
  naive form and paid it on every message a pass moves — thousands. Fixed by
  capturing the context the same way, which is also what makes the span above
  reach a real tracer rather than the default no-op one.
- **Durable execution answers a question this system does not ask.**
  `effect/unstable/workflow` was evaluated the way every other `unstable` module
  here was, and declined — recorded in
  [decisions/012](decisions/012-durable-workflows.md) rather than left as a
  shrug. The short version: a daemon's unit of work is *one* activity, so there
  is no partial progress to resume and nothing to compensate; durability for
  that work already belongs to the broker by a decision that was measured; and
  the state machine is a fold over an unbounded stream of observations rather
  than a sequence of steps, which is the shape a workflow engine is for. The
  cost side was checked rather than guessed at: `layerMemory` documents itself
  as unsuitable for durability, so real durability means
  `ClusterWorkflowEngine`, which needs `Sharding` plus a `MessageStorage` whose
  only durable implementation is SQL — a third stateful dependency for a stack
  that keeps its durable state in RabbitMQ and Redis. What would reopen it is
  named: work that is more than one step, where an unacked delivery cannot say
  how far it got.

