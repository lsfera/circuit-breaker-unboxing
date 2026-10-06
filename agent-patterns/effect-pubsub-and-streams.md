# PubSub, streams and back-pressure

Derived from `repos/effect/packages/effect/src/PubSub.ts`,
`SubscriptionRef.ts`, `Stream.ts`, `testing/TestClock.ts`, their tests under
`packages/effect/test/`, and `packages/platform/node/src/NodeHttpServer.ts`.
Written while building
[`ConsoleFrames.ts`](../packages/aggregator/src/ConsoleFrames.ts), where
guessing got one of these wrong.

## Choosing a PubSub by what a slow subscriber should cost

| Constructor | When a subscriber falls behind | Source |
| --- | --- | --- |
| `PubSub.bounded(n)` | the **publisher** waits | `PubSub.ts:365` |
| `PubSub.dropping(n)` | the **newest** message is dropped | `PubSub.ts:412` |
| `PubSub.sliding(n)` | the **oldest** message is dropped | `PubSub.ts:458` |
| `PubSub.unbounded()` | nothing is dropped, and memory grows | `PubSub.ts:498` |

All four take `{ capacity, replay }` (unbounded takes `{ replay }`): `replay`
hands a new subscriber the last *n* messages.

Decide by asking what one stalled consumer may do to everyone else. In this
repository:

- **Events to subscribers** — `PubSub.sliding(256)` in `Events.ts`. A stuck
  reader must never stall the control loop that publishes, and losing the
  oldest events is detected downstream as a sequence gap.
- **Console frames** — `PubSub.sliding(1)` in `ConsoleFrames.ts`. Only the
  newest frame is worth having; a frame that has been superseded is garbage.
  No `replay`, because the retained frame could be an hour old.
- **Never `bounded` on a path the control loop publishes on.** One slow
  browser would be back-pressure on the breaker.

## `SubscriptionRef` is not "latest value wins"

It reads like the natural primitive for "the current state, pushed to
whoever is listening". It is backed by an unbounded PubSub:

```ts
// SubscriptionRef.ts:111-112
export const make = <A>(value: A): Effect.Effect<SubscriptionRef<A>> =>
  Effect.map(PubSub.unbounded<A>({ replay: 1 }), (pubsub) => {
```

and `changes` (`:160`) is `Stream.fromPubSub` over it. A subscriber that
reads slower than the value changes accumulates **every** intermediate value,
without limit. For small values changing rarely that is fine. For a 1.1 MB
frame every 400ms to a browser on a slow link it is a leak in the publishing
process. Use `PubSub.sliding(1)` and publish to it.

## Streams that must notice their consumer leaving

- `Stream.fromPubSub(pubsub)` (`Stream.ts:1173`) subscribes when the stream
  runs, so one stream value served to many HTTP requests is many
  subscriptions.
- `Stream.onStart(effect)` (`:10061`) and `Stream.ensuring(effect)` (`:10170`)
  bracket a run. `ensuring` runs on interruption, which is how a closed
  browser tab arrives: the response stream is interrupted with the request's
  scope. `ConsoleFrames.ts` counts watchers this way, so it builds nothing
  once the last one leaves.
- `Stream.merge(a, b)` (`:2960`) runs until **both** end. Merge a
  never-ending stream, such as `Stream.tick` (`:513`), only into one that is
  meant to run until the client disconnects.

## HTTP responses already apply back-pressure

`HttpServerResponse.stream` on Node waits for the socket's `drain` event
before pulling the next chunk (`NodeHttpServer.ts:650-658`). The stream feeding a
response is therefore pulled no faster than the client reads. Whatever
buffers *upstream* of it — the PubSub — decides what a slow client costs.

## Testing streams under `TestClock`

`TestClock.adjust(duration)` (`testing/TestClock.ts:507`) moves virtual time
and wakes sleepers. It does not give forked fibers a chance to run in
between. Advancing ten intervals in one call publishes ten messages "at once",
and a sliding subscriber correctly sees only the last. That is a test bug that
looks like a product bug.

Advance one interval at a time, and yield between steps:

```ts
const settle = Effect.forEach(Array.from({ length: 25 }), () => Effect.yieldNow, { discard: true })
const intervals = (n: number) =>
  Effect.forEach(Array.from({ length: n }), () => Effect.andThen(TestClock.adjust(EVERY), settle), { discard: true })
```

When waiting for a condition, such as "five subscribers attached", bound the
wait. An unbounded `yieldNow` loop turns a broken finalizer into a hung suite
instead of a failed assertion. See
`packages/aggregator/test/ConsoleFrames.test.ts`.
