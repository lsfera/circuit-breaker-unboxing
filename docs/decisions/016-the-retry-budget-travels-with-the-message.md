# 016 — The retry budget travels with the message

**Status**: decided 2026-09-13; the key became the producer's `message_id`
2026-09-24.

## Decision

- **Classify each response.** 2xx acks; `429` is released uncounted; another
  4xx is parked on `<api>.work.parked` as `refused-<status>`, since retrying
  cannot change it; 408, 5xx or no response spends an attempt.
- **A failed attempt is republished, never requeued**, with the same
  `message_id` and `x-egress-attempts` + 1. Publish first, then ack: a crash in
  between is a duplicate, not a loss. The third attempt goes to
  `<api>.work.dead` with origin headers.
- **The queue's `x-delivery-limit: 3` is the backstop** for deliveries that
  never reach a republish (a daemon killed mid-call).
- **Redrive** replays `<api>.work.dead` on the transition to `CLOSED` (by the
  daemon elected on `redrive-trigger`) and every 30 s while closed (by the
  floor), one pass at a time. Each replay resets the attempt budget; past
  `MAX_REDRIVES` (5) a message is parked. A dead letter that is not work — a
  control event or trigger that would not decode — is parked, never replayed.

## Why republish

A broker requeue hands back the original message; nothing can be added to it.
The attempt count has to be written somewhere that travels, and a header on a
republished copy is the only place.

## Evidence

- A quorum queue defaults to `x-delivery-limit: 20` and **drops** a message at
  it when it has no dead-letter target. Each redrive pass returns what it does
  not move, so the dead-letter queue itself lost messages: 0 of 50 survived 22
  channel closes. The dead-letter and parked queues declare `-1`
  (`DeadLetter.test.ts` fails without it). This was 1,570 lost messages in the
  first chaos matrix.
- The first sweep replayed 21,646 of a 22,246-message backlog left by
  [010](010-a-proxy-that-fails-on-its-own-behalf.md)'s incident.
- A key minted by the daemon per first attempt left a gap: a daemon killed
  after the call got the redelivery back with no key and minted a new one. The
  producer's `message_id` closes it.
