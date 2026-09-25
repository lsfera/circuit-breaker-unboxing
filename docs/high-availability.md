# High availability

Two aggregators run against one Redis. Exactly one publishes; the other only
tries to take the lease. Two instances publishing would hand out conflicting
sequence numbers for the same API, which is the one thing the contract forbids.

## Lease, fencing token, checkpoint

`Coordination.ts` provides two services built on one shared counter:

- **`LeaderElection`**: every tick calls `tryAcquireOrRenew(instanceId, ttl)`
  (5 s). A non-leader does not poll, step or publish. Every genuine handoff,
  never a renewal, issues a **fencing token** `<epoch>:<counter>`.
- **`CheckpointStore`**: after publishing, the leader saves each API's
  `sequence` and `openBackoffMs` under its token, and a newly promoted instance
  resumes from them rather than from `CLOSED` at zero. A save is refused unless
  its token is the current one.

Two ways fencing went wrong before it was right:

- **Per-key fencing** only stops a stale writer after someone else has written
  that key, so a paused leader could still win every API the new leader had not
  published for yet. The check is against the one lease counter.
- **A bare counter** restarts at 1 when Redis loses its state, and a surviving
  stale leader's 5 then outranks the live one's 1. The epoch is minted by
  whichever coordinator finds nothing to inherit; tokens from different epochs
  are incomparable, so the stale one is fenced.

## The fence is on the checkpoint, so the events carry the lease

The leader publishes and then checkpoints, so a checkpoint never runs ahead of
what the broker confirmed. That means a leader paused past its lease can still
publish once when it resumes, before its save is refused. A successor that
re-derives an event its predecessor published but never checkpointed also reuses
that sequence, possibly with another state.

So every event carries `data.lease`, and `supersedes` (in
`packages/domain/src/Model.ts`) ranks by it before the sequence:

- a newer lease wins whatever its sequence; an older one loses whatever its
  sequence;
- within one lease, a transition must move the sequence forward and a snapshot
  must not move it back;
- a new epoch is accepted, since a coordinator that lost its state restarts
  sequences. A leader still holding an old-epoch token is fenced at its next
  checkpoint.

The daemons apply this and count what they ignore in
`egress_daemon_control_stale_total`.

## Failover, measured

| How the leader went away | Standby leads after |
|---|---|
| `docker kill` (a crash) | 4,952 ms — the full lease TTL |
| `docker compose stop` (a deploy: the lease is released on shutdown) | 234 ms |

A killed leader mid-incident at `sequence=127`: the standby took over, resumed
the API from its checkpoint as `OPEN`, and published 128 onward with nothing
published twice.

**One-sided partition**: one aggregator cannot reach Redis, the other can. Every
coordination call has a 1 s timeout under the 5 s lease, so the cut-off instance
stands down each tick (`is_leader` 0) instead of hanging, and the healthy one
holds the lease throughout.

**Liveness is not leadership.** `/livez` is the tick loop (three tick intervals
or 5 s). `/readyz` waits for one completed pass and is true on the standby too:
marking a standby unready would take the pair out of rotation during a rolling
deploy.

## The outbox

Inside the aggregator the sequence is gapless; the webhook's last hop was not. A
delivery that fails its retries is appended to a per-API outbox in Redis
(`Outbox.ts`), and the leader drains it:

- **only the leader drains**, or every event would be delivered twice;
- **nothing overtakes it**: deliveries for one API run one after another, and
  while the outbox holds anything for that API a new event is appended behind
  it rather than posted. Posting it directly would deliver 8 before a stuck 7;
- **in order, stopping at the first failure**, since skipping a stuck event
  manufactures a gap;
- **committed only after delivery**, by position: `peek` returns where its
  first entry sits, and the drain commits the position it reached. A count
  would trim undelivered entries if the bound dropped the oldest mid-drain;
- **bounded at 500 per API, dropping the oldest**, so a subscriber that stays
  down sees a gap it can detect rather than a state it wrongly trusts.

Solo mode runs the same interfaces in memory; `pnpm run test:redis` runs them
against a real Redis.
