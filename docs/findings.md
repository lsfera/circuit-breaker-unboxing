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
- **A subscriber offered as exemplary had a parser that fit one publisher.**
  `@egress/subscriber`'s own doc calls it "a downstream consumer, written the
  way a real one should be", and it read the SSE stream by splitting on a blank
  line: `buffer.split("\n\n")`. That works against this aggregator and only
  against this aggregator. The format also separates on `\r\n\r\n` and `\r\r`,
  `data:` may span several lines or omit the space after the colon, `id:`,
  `retry:` and comment lines exist, and a stream that never separates grows the
  buffer without bound — the last of which the runtime's own decoder has an
  error for (`EventTooLarge`). Both ends use `effect/unstable/encoding/Sse` now,
  so the wire format has one definition rather than an encoder and a parser that
  happen to agree. The encoder's bytes were compared before the swap and are
  identical, which is what made it a safe change rather than a hopeful one.

- **A cache that memoised an object literal, and a doc that explained why it
  was there.** `AmqpControlPlaneSink` kept a `Ref<Map<string, Publisher>>` with
  a get-or-create around it, under a comment about publishers being "declared
  lazily on first delivery and cached". A `Publisher` in `@egress/rmq` is
  `{ exchange, routingKey }` and `publisherToExchange` is `Effect.succeed` of
  one — no channel, no declare, no I/O. The cache existed because the comment
  said it should, and the comment described a client this repo does not have.
  Deleted: the sink addresses each send directly.
- **The one module that read `process.env` was the one telling everything else
  not to.** `@egress/tracing` hand-parsed both of its variables —
  `Number(process.env[...] ?? "1")` plus a range check that silently coerced
  anything invalid back to 1 — while
  [decisions/008](decisions/008-configuration-is-a-boundary.md) had just
  established that a value a process cannot use must stop it. Both are `Config`
  now, so `OTEL_TRACES_SAMPLER_ARG=banana` says
  `Expected a string representing a finite number at ["OTEL_TRACES_SAMPLER_ARG"]`
  instead of exporting everything and calling it the default. Verified across
  all four paths, including the one the hand-rolled version was written for:
  docker-compose interpolates an unset variable to the empty string, and an
  empty endpoint still means "no tracer" rather than "an exporter pointed at
  nowhere".
- **A test kept a production function alive.** `Settings.parse` — a `Result`
  wrapper over `Config.parse` — had no caller left after the CLI pass except
  the test that tested it. The rules it existed to make testable are the
  `Config`s themselves, so the test now parses them directly and the wrapper is
  gone.
- **Three declarations of what a broker address is.** The schema, then
  `Flag.filterMap(decodeBroker, ...)` in `@egress/config`, then the same
  `Flag.filterMap(decodeBroker, ...)` copied into the aggregator because its
  flag is optional rather than defaulted. One `rmqFlag` now, decorated at each
  entry point — the duplication `brokerFlag` was introduced to remove had
  simply moved up a level.
- **`Date.now()` arithmetic where the runtime has a combinator.** Three
  hand-rolled deadline loops — two in the demo driver, one bounding a redrive
  pass — each with a `while (true)`, a `Date.now() + timeoutMs` baseline and a
  mutable accumulator. The driver's two were the same twenty lines written
  twice; they are one `awaitOn` over `Schedule.spaced` and `timeoutOrElse`, and
  the transition wait now tolerates a transient scrape failure the way the
  fleet wait always did. The redrive pass keeps `Date.now()` for its idle timer
  on purpose — that measures how long the *broker* has gone without handing
  over work — but its five stop conditions are one total function polled on a
  schedule, with the deadline as the timeout around it rather than a sixth
  branch that could exit with no reason recorded.
- **A fencing epoch minted from `Math.random().toString(36).slice(2, 10)`.**
  Eight base-36 characters, in the one value whose entire job is to be
  unrecognisable to a coordinator that lost its state. A collision there is a
  fencing token that fails open. `randomUUID`, which the redrive already used
  two packages over for pass ids that matter less.
- **A fourth hand-written JSON reader.** `@egress/domain`'s `readerFor` was
  introduced to give one definition of "parse, then decode, and say which step
  failed", and two more `try { JSON.parse } catch` blocks were still sitting
  behind it — the checkpoint load and the outbox's list parser. Both go through
  it now, which is why the Redis suite's warnings read `malformed-json` and
  `schema-mismatch`: the same two words the daemon's undecodable metric uses.
- **The delivery contract was checked twice and stated twice.** Two observers
  watch the guarantee this repo exists to make — the aggregator's own webhook
  subscriber over HTTP, and five daemons over AMQP — and each carried its own
  `sequence <= last` comparison under its own paragraph explaining why it is
  `<=` rather than `===`. That comment is there because checking only for
  equality *was* a bug: a sequence going backwards reuses a number just as
  surely as one repeating it, and that is the shape a leadership bug produces.
  Having the fix in two places is having it in neither. The rule is
  `classifySequence` in `@egress/domain` now; what stays local is the shape
  each observer needs, a per-API map on one side and a single last sequence on
  the other. The vantage points were always the independent part, and they
  still are.
- **The one instant that skipped the clock was the one that got published.**
  The aggregator is `TestClock`-driven throughout — its loop is a `Schedule`
  precisely so a simulated minute costs microseconds — and `Events.build` read
  `new Date()` for the CloudEvent's `time`. Each tick emitted an event whose
  payload was in simulated time and whose envelope was in wall time. Production
  never noticed, because there the two agree; the test did, and could say
  nothing stronger than `Date.parse(e.time) > 0`. `build` takes the `now` its
  caller already held, and the assertion is now that `time` lands inside the
  window the clock was advanced through — checked to fail when `new Date()` is
  put back, because a strengthened assertion that cannot fail is not one.
- **`Effect.clockWith((c) => c.currentTimeMillis)`, seven times.** The runtime
  exports that exact effect as `Clock.currentTimeMillis`. Nothing subtle — it
  is worth listing only because it is the same shape as the rest of this pass:
  a thing written by hand often enough to look like the idiom.
- **Two sinks, two retry budgets.** The webhook and AMQP sinks both wrote out
  `Schedule.exponential(100ms)` with `times: 3`. Tuning one and not the other
  would leave a subscriber and a broker disagreeing about how long an outage
  has to last before an event is given up on — a difference nobody would have
  chosen. One `DELIVERY_RETRY`.
- **The module that documents the hazard had five instances of it.**
  `daemon.ts` explains, above its context capture, that a bare `Effect.run*`
  builds a fresh runtime with *default* services — which is why
  [003](decisions/003-tracing.md) records a span that reached the no-op tracer
  and never left the process. That fix was applied to the one `runPromise` path
  and to nothing else: five `Effect.runFork` calls in AMQP callbacks below it
  kept the naive form, three of them starting real broker work rather than a
  log line. Measured rather than reasoned about: a bare
  `runFork(logWarning(...))` never reaches a provided `Logger`, it goes to the
  default one. All five go through `Effect.runForkWith(services)` now.
- **The one module whose logs are about the thing most likely to be wrong at
  3am was the one not using the logger.** `@egress/rmq` reported connection
  loss, reconnect attempts, restored topology, broker-side consumer
  cancellation and every channel rebuild through `console.warn` — ten of them.
  They reached stderr, so they looked fine in `docker compose logs`, and they
  were invisible to any level filter or sink the process's logger has, with no
  level, timestamp or fiber of their own. Same root cause as the entry above,
  same fix, and now verified in the broker suite's own output:
  `[13:55:44.350] WARN (#16): [rmq] consumer channel on … closed — rebuilt`.
  One `console.error` stays, in `connectionLost`, because it is the last thing
  before `process.exit(1)` and a logger that batches would lose it.
- **A third copy of the sequence rule, in the module offered as exemplary.**
  The pass before this one found the delivery contract implemented twice and
  gave it one definition; `@egress/subscriber` had a third, open-coded —
  `sequence <= current.sequence` for a duplicate and `sequence >
  current.sequence + 1` for a gap, with the event type spelled as a string
  literal rather than `SEQUENCED_EVENT`. It is the file whose own doc offers it
  as what a real subscriber should look like, which is the second time that
  claim has turned out to describe the thing that had drifted. Three observers
  now read one rule.
- **`undefined` as a sentinel travelling down a Stream.** The same subscriber
  mapped an undecodable frame to `undefined` and then checked for it in
  `runForEach` — precisely what
  [006](decisions/006-representing-absence.md) rules out, in a repo that has an
  ADR about it. `Stream.filterMapEffect` drops what it cannot use; the consumer
  no longer has a case to remember.
- **A sweep that was not a sweep.** The previous pass reported replacing
  `Effect.clockWith((c) => c.currentTimeMillis)` "seven times". It was eleven —
  the script ran over a hand-written list of aggregator files rather than a
  grep of the tree, so four in `@egress/rmq-consumer` and one in a test stayed.
  Worth recording because the mistake is the same shape as everything else
  here: a list maintained by hand alongside the thing it is supposed to cover.
- **The checkpoint was written on every transition and read back never.**
  `--source=envoy-push` is what docker-compose runs, and under it every leader
  change silently cold-started every API at sequence 0 — with the checkpoint
  sitting in Redis, valid and unread, and nothing logged. Rehydration required
  *both* "this is the acquisition tick" and "reports for this API arrived",
  which is one condition too many: a polling source answers its first poll
  immediately, so it held in the simulator and in every test, and a push source
  cannot, because no Envoy has streamed to a process that started milliseconds
  ago. The guard is gone; `known.has(apiId)` alone already bounds the work to
  one load per API per instance.

  Worth recording how it surfaced, because none of the tests could have. The
  Redis suite exercises `save`/`load` directly and passes. The failover tests
  drive two full aggregators and pass. What caught it was the *daemon fleet's*
  duplicate counter during an ordinary demo run: `gaps=0 dup=6`, six
  transitions, six duplicates, identical across all five daemons — the fleet
  correctly reporting that the publisher had reused numbers it had already
  seen. The delivery contract's independent observer earned its place.

  The regression test is a fleet source that is silent for the first five
  ticks, which is the shape the deployment actually has and the shape no
  existing harness had. Confirmed to fail against the old code with
  `sequence must continue past the checkpoint (41), got 0`.
- **The endpoint whose readings are quoted as proof was the one not checking.**
  `/subscriber/webhook` read its body as `(yield* request.json) as CircuitEvent`
  — a cast, on input from the network, in the handler that exists to verify the
  delivery contract. Measured rather than argued: two *identical* events with no
  `sequence` were accepted as `received=2, duplicates=0`, and the per-API
  high-water mark became `undefined`, after which `n <= undefined` and
  `n > undefined + 1` are both false — so every later event for that API reads
  as an ordinary next one. One malformed POST permanently disabled gap
  detection for an API, and returned 202. Both POST routes use
  `HttpServerRequest.schemaBodyJson` now. `/api/failure` gained a bound with it:
  `rate` is a probability, and `47` meant `Math.random() < 47`, which is
  "always".
- **The demo driver's contract check ran on a cast.** It declared its own copy
  of the event payload and cast `/api/events` to it — in the script whose doc
  says what it prints *is* the published contract rather than an assumption. It
  decodes against `CircuitEvent` now and counts what does not.
- **Three alerts pointed at metrics that had no series.** `Http.ts` carries a
  block whose entire job is registering counters at zero before anything
  happens to them, under a comment saying why: an `effect` counter has no
  series until its first update, and "a tile whose whole job is to sit at zero
  through an incident is worse than useless when zero looks like broken." The
  block covered the per-API counters and missed every counter without an
  `apiId` — so `egress_aggregator_coordination_errors_total` and
  `egress_aggregator_fencing_conflicts_total` did not exist in Prometheus at
  all, and `egress_fleet_replica_lost_total` only grew a series once a replica
  had already been lost. All three are named by alerts in
  `infra/monitoring/alerts.yml`. Checked against the running Prometheus rather
  than by reading: two returned `NO SERIES` before, and afterwards coordination
  errors, fencing conflicts and all four documented `replicasLost` reasons
  report zero on both instances, with 14 rules healthy and none firing.

  The rule was stated in one place and applied to one of the two shapes it
  covers. That is this review's most common finding by some distance.
- **`Effect.forever(sleep >> act)`, four times.** `Aggregator.ts` explains why
  its tick loop is a `Schedule` — "not a `setInterval` … that is what lets
  TestClock drive thousands of simulated seconds instantly, and what makes the
  loop interruptible as a value rather than via a `clearInterval` handle
  someone has to remember to call" — and the daemon's metrics flush, ramp
  advance and heartbeat, plus the demo's queue sampler, were all hand-rolled
  the other way. They are `Effect.repeat(action, Schedule.spaced(…))` now, with
  the three daemon intervals named where they can be read together. One
  visible consequence, since a schedule runs its first pass immediately: the
  daemon's gauges carry real numbers from startup rather than after a second,
  and the first heartbeat lands next to the "up" line instead of fifteen
  seconds later.
- **The AMQP client owned the process's fate, and its escape hatch was
  fiction.** `@egress/rmq` called `process.exit(1)` when recovery gave up —
  the same thing [008](decisions/008-configuration-is-a-boundary.md) had
  already removed from `@egress/config`, on the grounds that fail-fast is right
  and owning the process's fate from inside a library is not. It offered
  `onLost` to override it, documented as what "tests that deliberately take a
  broker away" use; no caller in the repo ever passed it, those tests included.
  The service exposes `lost: Effect<never, RmqError>` now, and one helper,
  `launchWithRmq`, turns it into a stopped process.

  The shape of that helper was decided by measurement, not taste. The obvious
  move — fork a fiber into the layer's scope and let it die — does not work: a
  probe showed `Layer.launch` running its full 1200 ms timeout whether or not a
  scoped fiber had died. Only the launching fiber can end it, which is also why
  this is one helper rather than a rule each `main.ts` has to remember.
- **What was deliberately left imperative, including one change that was
  written and then dropped.** Most of this client is `ch.on(...)` and callback
  plumbing over amqplib. Converting that to `Ref` would mean reading state from
  an event handler through `runSync`, which is worse rather than more
  Effect-shaped, so the mutable `live`/`topology` bookkeeping and the `null`
  channel slots stay — with the module's existing note on why `null` and not
  `Option` there.

  The publish confirm was converted to a `Deferred` and then reverted. It is a
  hand-rolled `new Promise` plus a `Set` of reject callbacks, and a `Deferred`
  would deregister on interruption where the promise leaves its rejector until
  the channel happens to close. That leak is real, bounded, and has never cost
  anything; the change was two dozen lines in the hottest path in the client,
  the one every message goes through. It fixed no bug anyone had hit, which by
  this review's own standard is spelling rather than behaviour. Recorded
  because "we tried it and it was not worth it" is more useful than silence,
  and because the reasoning is what to re-read if a send ever does need a
  timeout.
- **A regression of this review's own making, and how it was caught.** Moving
  `process.exit(1)` out of `@egress/rmq` left the aggregator with a `lost` that
  nothing awaited: its control-plane broker could die for good and the process
  would keep serving 200s while the daemon fleet silently stopped hearing state
  changes — the exact failure the fatal stance exists to prevent, reintroduced
  by removing the mechanism that prevented it. Found by asking what now
  observes `lost` rather than by a test, which is worth recording: nothing in
  the suite covers "a broker that never comes back", because the only honest
  test for it takes five minutes of real retries.

  The aggregator now has one fatal channel for both ways it ends itself — a
  dead control loop and a lost control plane — and there is no `process.exit`
  left anywhere in `packages/`. Verified the five-minute way: broker stopped,
  60 attempts, then `FATAL: aggregator-1: control plane lost` followed by
  `Fatal: control plane lost: connection: recovery gave up: getaddrinfo
  ENOTFOUND rabbitmq`, both instances restarted by the policy, and the sequence
  resumed at its checkpoint when the broker came back.
- **Two container images, each declared twice.** The four integration suites
  each carried their own `before`/`after`, their own `skipIfNoDocker`, their
  own `waitFor`, and their own copy of the image tag — so
  `rabbitmq:4.0-management-alpine` and `redis:7-alpine` each existed in two
  files, and bumping one would have left two suites testing different brokers
  with nothing to say so. Two `harness.ts` modules now own that, 141 lines
  lighter, with the tags in one place each.

  Two things moved into the harness rather than staying inline because they are
  knowledge, not setup: `restartBroker` re-reads the mapped port, which changes
  across a restart and which the one test that restarts had to remember by
  hand; and `asRedisLike`, the adapter the README advertises as one line, which
  existed as two.
- **The verdict was read through a cast.** The demo driver prints
  `duplicates=N gaps=N` as its delivery-contract result and then checks
  `sub.duplicates > 0`. It read `/api/subscriber` through
  `as Promise<{...}>`, so a field that went missing arrived as `undefined` —
  and `undefined > 0` is false. A drifted contract would have passed the
  contract check silently, printed `duplicates=undefined`, and been read as a
  pass. Same shape as the webhook endpoint two passes earlier, in the script
  whose whole claim is that what it prints *is* the published contract.
  Decoded now, and the four cases were checked rather than assumed: a healthy
  reading decodes, a real violation decodes, and both a missing `duplicates`
  and a renamed `gaps` are rejected naming the key.

## Where this leaves the sweep

The patterns this review kept finding are, as of this pass, absent from
`packages/*/src`:

| Pattern | Remaining |
| --- | --- |
| `process.exit` outside a composition root | none anywhere |
| Bare `Effect.runFork` / `runPromise` / `runSync` | none |
| `console.warn` / `console.error` bypassing the logger | none |
| `Effect.forever(sleep >> act)` / `setInterval` | none |
| `Effect.clockWith((c) => c.currentTimeMillis)` | none |
| `JSON.parse` | one, inside `readerFor`, which is the point of it |
| Exports unused outside their module | one, a public function's own result type |

That is not "nothing left to improve" — it is that the specific rules this
review established are now applied everywhere they apply. The recurring finding
was never a particular API; it was **a rule written down in one module and
applied to only some of the places it covers**, which accounted for most of the
defects above, including the two that mattered: a checkpoint written every
transition and read back never, and an endpoint quoting numbers it had not
checked.

## After the sweep: a rule the sweep could not see

The table above is about *patterns*. This one was a disagreement between two
correct-looking pieces of code, which no grep finds.

- **The outbox drain committed by a count it no longer had.** `Outbox.peek`
  filtered out entries that fail to decode — a deliberate choice, argued in a
  comment: an event a subscriber cannot parse is worse than an event it never
  sees. `drainPass` then delivered what came back and committed `delivered`,
  and `commit` is an `LTRIM` over the *stored* list. Filtering broke the
  correspondence between the two: every entry dropped on the way up shifted a
  delivered event one position right, and the trim stopped short of it.

  Measured against a real Redis before touching anything. An undecodable entry
  ahead of sequences 2 and 3 delivered **`[2, 3, 3]`** — the duplicate the whole
  system exists to prevent, produced by the code that protects it. An
  undecodable entry alone was worse: `replayed 0` on every pass, forever, with
  the API never leaving the pending set and its depth gauge stuck at 1 — an
  alert that can never clear.

  This is not hypothetical. The entries are written by whichever replica held
  the lease, into a Redis all of them share, so an undecodable entry is what a
  rolling upgrade that changes the event schema *produces*.

  Fixed by making the absence explicit rather than erasing it: `peek` returns
  `ReadonlyArray<Entry>` where `Entry = Option<CircuitEvent>`, so an unreadable
  entry keeps its position, and the drain commits `consumed` — delivered plus
  unreadable — rather than `delivered`. ADR 006's rule, applied to the one
  place that had quietly dropped it: an absence that occupies a position must
  be represented, not filtered away.

- **`Option` was being asked whether, rather than told what to do.** A sweep of
  every `Option` site in `packages/*/src`, prompted by one `O.isNone`-then-
  `.value` in `seedFromCheckpoint` that turned out not to be alone. Eleven
  sites left `Option` immediately after entering it, and three of them were
  hiding something:

  `makeInMemoryCoordination`'s lease decision tested `held !== undefined`
  twice and reconstructed "expired *or* never held" as a fallthrough past both
  `if`s. As an `O.match`, `onNone` and an expired `onSome` are visibly the same
  branch — `handOver` — which is what the code always meant.

  `release` was `O.getOrUndefined(current)?.holderId === holderId ? O.none() :
  current`. That is `O.filter((held) => held.holderId !== holderId)`: drop the
  lease only if this holder still owns it. The `Ref.update` is now point-free
  and the test name — "release only removes the lease if the caller still holds
  it" — is the code.

  The aggregator's step loop stamped `lastSnapshotAt` in two places, once per
  emitting branch. Composing the choice instead — `O.orElse(transition,
  () => heartbeat-if-due)` — makes the precedence explicit (a transition wins;
  it is the one carrying `from`) and leaves one stamp, so the two cannot drift
  into "a transition that forgets to restart the snapshot clock".

  `Redrive`'s provenance was a `??` chain over `getOrUndefined`, written out
  twice for queue and reason. The first rewrite reached for `O.firstSomeOf`
  behind a higher-order helper taking a field picker and a property key, and
  that was worse than what it replaced — a correction worth recording, because
  it is the failure mode of this whole exercise: reaching for the combinator
  with the matching name rather than the one that fits. Both fields come from
  the same `Option` and are present or absent together, so it is one
  `O.getOrElse` over the pair, destructured. The `??`s that remain are between
  two plain `string | undefined` properties, where `??` is the right tool.

  What is deliberately left: `O.isSome` where the answer really is a boolean
  (a gauge, a log line, `until: O.isSome`), and two `O.isNone` guards whose
  narrowing makes the following `.value` checked — `attemptTick`'s standby
  return, and the outbox drain, which `break`s at the first undeliverable entry
  and so cannot be a fold. A fold is right where a value comes out; a guard
  clause is right where control flow does.

- **The same shape, one construct over.** `Result` was being read exactly the
  way `Option` had been: `Result.isFailure(decoded)` as a guard, then
  `decoded.failure` to pick a message and `decoded.success` to get the value —
  in the daemon's control consumer, in `onTrigger`, and in the subscriber's SSE
  reader. `Coordination.ts` already folded its checkpoint read with
  `Result.match`, so these three were an inconsistency rather than a style
  preference, and the fold turns "decode, then either report or act" into what
  it always was: one expression with two named branches.

  `perform` was a `switch` over `Action["_tag"]` returning an Effect per case —
  a total mapping from tag to behaviour, which is what `Match.typeTags` is for.
  It also destructures `sequence` per branch, so the two publishing cases stop
  repeating `action.sequence`.

  Where the constructs sweep stopped: the imperative loops in `FleetSource`,
  `EnvoyPushSource`, `Tally` and the outbox drain accumulate across `break`s,
  early `continue`s and mutable counters that outlive the iteration. Rewriting
  those as folds would move the mutation into a closure rather than remove it,
  and they are parsing and accumulation code where the step order is the point.
  `switch (classifySequence(...))` stays a `switch`: it matches a string union,
  not a tagged type, and `Match` over it would be heavier than the thing it
  replaced.

- **The constant existed so the rule would have one name; four call sites spelt
  it out instead.** `SEQUENCED_EVENT` is the published delivery guarantee's one
  name, and `docs/decisions/007` argues at length that the rule must not be
  written twice. The *string* was written five times anyway: in `Events.ts`
  building both event kinds, twice in `Aggregator.ts` deciding what to
  checkpoint and publish, and in `driver.ts` — which already imports
  `SEQUENCED_EVENT` and uses it correctly sixty lines away.

  Nothing was broken, and that is the point: a misspelling at any of them
  compiles and matches nothing, so the aggregator would simply stop
  checkpointing, or the driver would report seq=0 and read it as a pass.

  `EventType = CircuitEvent["type"]` closes the union at the schema that
  publishes it, and both constants are `satisfies EventType` rather than
  annotated — the annotation would widen them to the union and break the
  narrowing that `event.type === SEQUENCED_EVENT` relies on. Both failure modes
  are now compile errors, measured:

      build("egress.circuit.state_change", ...)  -> TS2345 not assignable to
                                                    the schema's union
      SNAPSHOT_EVENT = "egress.circuit.snapshots" -> TS1360 does not satisfy

  `Contract.observe(self, eventType, sequence)` and `Tally`'s `byType` maps took
  the union too; they were `string`, which is how an event type that matches no
  branch gets counted under its own misspelling.

  Not a tagged union: the discriminator here is CloudEvents' `type` field, which
  is the wire contract. `Schema.TaggedStruct` puts `_tag` in the *encoded* form,
  so tagging these would either change what every subscriber receives or need a
  transform on both sides — for exhaustiveness the closed union already gives.

- **HALF_OPEN with a DEGRADED verdict was a state the machine could enter and
  never leave.** `step` resolved the graph as a chain of `if`s, and the
  HALF_OPEN branch ended `return [{ ...next, probeStreak: 0 }, O.none()]` —
  every pair that had not matched above meant "stay put". Two pairs reached it:
  `candidate === OPEN && !dwelled`, which should stay put, and
  `candidate === DEGRADED`, which should not.

  Measured before touching anything, three replicas reporting 3 of 6 hosts
  healthy: **600 ticks in HALF_OPEN, zero transitions published.** No timeout
  rescues it — `openBackoffMs` is only consulted in the OPEN branch — and if
  every replica goes quiet the empty-fleet guard returns early and leaves the
  state alone.

  The cost is in `DaemonPolicy.step`: HALF_OPEN means `targetActive: 1`, one
  SAC-elected prober on `prefetch: 1`; DEGRADED means `ceil(fleetSize / 2)`.
  So a persistently half-healthy upstream — the most ordinary failure there is —
  pinned the fleet at one message at a time while the state that exists for
  exactly that case sat unreachable, and published nothing, so nothing
  downstream could notice.

  The graph is now a `Record<State, Record<Candidate, Resolve>>` — every state
  against every verdict the fleet can return, twelve cells, each named.
  `Candidate` excludes HALF_OPEN because replicas cannot report it. Falling
  through is spelt `hold`, so it is a choice rather than the absence of one.

  Worth being precise about what that buys: a *missing* pair is now a compile
  error (measured: `TS2741: Property 'DEGRADED' is missing`), but a pair wired
  to the *wrong* resolution still compiles. Pointing HALF_OPEN × DEGRADED back
  at `hold` typechecks clean and fails the new test. The table makes the graph
  legible and closes one failure mode; the test is what holds the behaviour.

  Dropped in the same pass: `live`, an array built alongside `replicas` holding
  the same slots and read only for `.length`, at five sites that all mean
  `replicas.size`.

  A second pass over the result found the same shape one level down, introduced
  by the fix itself: `settleInto` chose its `Reason` with a nested ternary on
  `to`, and its DEGRADED arm — `overflowDrove ? THRESHOLD_OVERFLOW :
  OUTLIER_EJECTION` — was character-for-character the expression in
  `probeDegraded`. One rule, two copies, a day old. `REASON` is now a
  `Record<Candidate, (tick) => Reason>` read by both, keyed by the verdict
  rather than by the state being left, because that is what the reason is a
  property of: a DEGRADED fleet means the same thing whether it is reached from
  CLOSED or out of a probe. Missing an arm is a compile error, measured the same
  way as the table above.

- **The producer backed off exactly when its comment said it must not.**
  `runProducer` publishes a batch of `ratePerSecond / 10` every 100ms and its
  module comment is explicit about why it must not adapt: *"a producer that
  backed off would hide the backlog the fleet has to survive."* It used
  `Schedule.spaced(100ms)`, which delays 100ms **after** each batch completes,
  so the period is 100ms plus however long the broker took to confirm twenty
  messages.

  Measured against the running stack, configured at 200/s:
  `rate(egress_producer_published_total[2m])` = **189.49/s** — a 5.3% shortfall,
  and the shortfall grows with broker latency, so the producer slows down
  precisely when the queue is deepest and the demo's arrival-versus-completion
  story is being made. `Schedule.fixed` keeps the cadence and skips a tick only
  if one overruns. Same query after: **200.00/s**.

  The distinction generalises, and the other seven periodic schedules here were
  checked against it: a *poller* should space, because leaving the interval
  after a slow call is politeness toward the thing being polled — the
  aggregator's tick against Envoy admin, the redrive pass, the SSE keepalive.
  A *rate source* should be fixed, because the rate is the contract. Exactly one
  site in this repo has a rate contract, and it was the one that was wrong.

- **The outbox drain was the one swallowed cause in the aggregator.**
  `drainOutbox` ended `Effect.catchCause(() => Effect.succeed(0))` — correct in
  refusing to end the loop, but it logged nothing, so an outbox that could not
  be replayed said so nowhere.

  Worth being precise about the scope, because the obvious scenario is already
  covered: a total Redis outage fails `tryAcquireOrRenew` first, and the tick is
  abandoned with a "coordination unavailable, standing down" warning before the
  drain is reached. What is silent is an outbox failing while the lease does
  not — a separate Redis for the outbox, a memory limit hit on write, an error
  on the read itself — where the instance stays leader, keeps publishing, and
  simply never replays what a recovered subscriber is owed.

  Edge-triggered the way coordination reachability already is. Measured with a
  stub outbox that fails on demand: three consecutive failing passes emit one
  warning, recovery emits one "readable again", and `drainOutbox` still returns
  0 without failing, so the tick loop is untouched.
