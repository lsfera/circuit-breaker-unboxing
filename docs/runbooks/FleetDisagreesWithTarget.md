# FleetDisagreesWithTarget

`sum(egress_daemon_self_active) != max(egress_daemon_target_active)` for 2m.

## What it means

Every daemon derives the same target from the same events, with no
coordination, so these two numbers track each other by construction. When they
stop tracking, at least one daemon is not acting on what the fleet agreed.

This is the deaf-daemon signature. The failure it was written for: a daemon
that stopped hearing `circuit.control` entirely while its container, CPU,
sockets and file descriptors all looked completely normal.

## Check first

```bash
docker compose logs rmq-daemon-0 --tail 5 | grep heartbeat
curl -s prometheus:9090/api/v1/query?query=egress_daemon_self_active | head -c 400
```

Each daemon logs a heartbeat every 15s *independent of the event stream*,
which exists because until it did, "gone deaf" and "nothing happened" produced
identical logs. A daemon whose heartbeat still prints but whose `control=` count
has stopped rising is the deaf case.

## Causes

- The AMQP consumer bug this repo works around: closing a consumer with
  deliveries in flight strands them, and enough strandings stall every link on
  that connection. The fleet runs two connections per daemon for this reason —
  the control plane never closes a link.
- A daemon mid-ramp. During recovery the target rises one rung at a time, so a
  brief disagreement is expected; two minutes of it is not.
- A daemon that died and was not restarted (`docker kill` is a manual stop).

## Resolution

Restart the daemon that disagrees. It rebuilds its control queue and relearns
the state from the aggregator's next snapshot, so nothing needs to be replayed
by hand.
