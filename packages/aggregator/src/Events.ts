import {
  Context,
  Duration,
  Effect,
  Layer,
  Metric,
  Option as O,
  PubSub,
  Ref,
  Schedule,
  Stream,
} from "effect";
import { randomUUID } from "node:crypto";
import { DeliveryFailed, SEQUENCED_EVENT, SNAPSHOT_EVENT } from "@egress/domain/Model.ts";
import { Outbox, OUTBOX_DRAIN_LIMIT } from "./Outbox.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSnapshot, CircuitEvent, EventType, State } from "@egress/domain/Model.ts";

export const SOURCE = "egress-proxy/control-plane";

/**
 * How many dead letters a sink keeps in memory for inspection.
 *
 * This list is a diagnostic buffer, not a ledger: an unbounded one grows for
 * as long as a subscriber stays broken, in a process meant to run for months.
 * The authoritative total is `egress_webhook_dead_lettered_total`, which is a
 * counter and costs nothing to keep exact.
 */
export const DEAD_LETTER_BUFFER = 200;

/**
 * How hard any sink tries before dead-lettering. One declaration because both
 * sinks had it written out: a subscriber and a broker that disagree about how
 * long an outage has to last before an event is given up on is a difference
 * nobody chose.
 *
 * Low on purpose. Delivery is forked off the tick loop, so a longer retry does
 * not stall anything — but the outbox behind the webhook sink is the durable
 * answer to a subscriber that stays down, and retrying into one that is gone is
 * just latency before the durable path takes over.
 */
export const DELIVERY_RETRY = {
  schedule: Schedule.exponential(Duration.millis(100)),
  times: 3,
} as const;

/**
 * `now` is passed in rather than read here. Every other instant this system
 * publishes comes from the Effect clock — `observedSince` included, two lines
 * down — and a `new Date()` in this one field meant a tick carried two clocks:
 * simulated time in the payload and wall time in the envelope, which is also
 * why no test could assert what `time` should be.
 */
const build = (
  type: EventType,
  snap: ApiSnapshot,
  previousState: State | null,
  now: number,
): CircuitEvent => ({
  specversion: "1.0",
  type,
  source: SOURCE,
  subject: `api://${snap.apiId}`,
  id: randomUUID(),
  time: new Date(now).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId: snap.apiId,
    sequence: snap.sequence,
    previousState,
    state: snap.state,
    reason: snap.reason,
    healthyEndpoints: snap.healthyEndpoints,
    totalEndpoints: snap.totalEndpoints,
    observedSince: new Date(snap.observedSince).toISOString(),
    reportingReplicas: snap.reportingReplicas,
  },
});

// `null`, not `Option`: this feeds `Schema.NullOr(StateSchema)` on the
// published event. JSON has null, subscribers parse null, and the first event
// for an API genuinely has no predecessor — see
// docs/decisions/006-representing-absence.md.
export const stateChanged = (snap: ApiSnapshot, previous: State | null, now: number) =>
  build(SEQUENCED_EVENT, snap, previous, now);

export const snapshotEvent = (snap: ApiSnapshot, now: number) =>
  build(SNAPSHOT_EVENT, snap, null, now);

// ---------------------------------------------------------------------------
// EventBus — one PubSub, many subscribers. Backpressure and per-subscriber
// cursors come with it, and each SSE client is a Stream that ends with its
// request scope rather than a listener something has to remember to remove.
// ---------------------------------------------------------------------------

export class EventBus extends Context.Service<
  EventBus,
  {
    readonly publish: (event: CircuitEvent) => Effect.Effect<void>;
    readonly subscribe: Stream.Stream<CircuitEvent>;
    readonly recent: Effect.Effect<ReadonlyArray<CircuitEvent>>;
  }
>()("EventBus") {}

export const EventBusLayer = Layer.effect(
  EventBus,
  Effect.gen(function* () {
    const pubsub = yield* PubSub.sliding<CircuitEvent>(256);
    const ring = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
    return {
      publish: (event) =>
        Ref.update(ring, (xs) => [...xs, event].slice(-200)).pipe(
          Effect.andThen(PubSub.publish(pubsub, event)),
          Effect.asVoid,
        ),
      subscribe: Stream.fromPubSub(pubsub),
      recent: Ref.get(ring),
    };
  }),
);

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

export class EventSink extends Context.Service<
  EventSink,
  {
    readonly name: string;
    readonly deliver: (event: CircuitEvent) => Effect.Effect<void>;
    readonly deadLetters: Effect.Effect<ReadonlyArray<DeliveryFailed>>;
    /** See SinkImpl: replay what an earlier attempt could not deliver, leader-only. */
    readonly drainOutbox: Effect.Effect<number>;
  }
>()("EventSink") {}

/** The shape every sink builds — split out from the Layer wrapper so main.ts can compose several before mounting the one EventSink tag. */
export type SinkImpl = {
  readonly name: string;
  readonly deliver: (event: CircuitEvent) => Effect.Effect<void>;
  readonly deadLetters: Effect.Effect<ReadonlyArray<DeliveryFailed>>;
  /**
   * Replay whatever an earlier attempt could not deliver, and answer how many
   * got through. A sink with nothing durable behind it returns 0.
   *
   * Required rather than optional so that adding a sink is a decision about
   * this, not an omission. It is the *caller* that decides when to run it,
   * because the answer is "only on the instance that holds the lease" — a
   * standby replaying the same outbox would deliver every event twice, which
   * is precisely the break the sequence contract exists to make visible.
   */
  readonly drainOutbox: Effect.Effect<number>;
};

/**
 * Stand-in for the real middleware hop. In production this is a produce() to
 * Kafka or NATS keyed by apiId; HTTP keeps the demo broker-free.
 *
 * The retry policy is the reason this is worth doing in Effect: v1 hand-rolled
 * a loop with a counter, a sleep and a try/catch. Here the policy is a value —
 * exponential backoff, capped attempts — and it composes.
 */
export const makeWebhookSink = (url: string): Effect.Effect<SinkImpl, never, Outbox> =>
  Effect.gen(function* () {
      const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
      const outbox = yield* Outbox;

      const post = (event: CircuitEvent) =>
        Effect.tryPromise({
          try: async (signal) => {
            const res = await fetch(url, {
              method: "POST",
              headers: {
                "content-type": "application/cloudevents+json",
                // Partition key. On Kafka this is the message key; ordering
                // per API is the only ordering subscribers actually need.
                "ce-partitionkey": event.data.apiId,
                "idempotency-key": `${event.data.apiId}:${event.data.sequence}`,
              },
              body: JSON.stringify(event),
              signal,
            });
            // Drained even though the status is all this cares about: an
            // unconsumed body keeps its connection out of the pool.
            await res.text().catch(() => {});
            return res;
          },
          catch: (cause) =>
            new DeliveryFailed({
              sink: "webhook",
              apiId: event.data.apiId,
              cause: String(cause),
            }),
        }).pipe(
          Effect.filterOrFail(
            (res) => res.ok,
            (res) =>
              new DeliveryFailed({
                sink: "webhook",
                apiId: event.data.apiId,
                cause: `HTTP ${res.status}`,
              }),
          ),
          Effect.timeout(Duration.seconds(2)),
        );

      const deliver = (event: CircuitEvent) => {
        const apiId = event.data.apiId;
        const attempt = post(event).pipe(
          Effect.retry(DELIVERY_RETRY),
          Effect.tapError(() => Metric.update(Metric.withAttributes(Telemetry.webhookFailed, { apiId }), 1)),
        );
        return Effect.timed(attempt).pipe(
          Effect.tap(([duration]) =>
            Metric.update(
              Metric.withAttributes(Telemetry.webhookDeliveryDuration, { apiId }),
              duration,
            ),
          ),
          Effect.tap(() =>
            Metric.update(Metric.withAttributes(Telemetry.webhookDelivered, { apiId }), 1),
          ),
          Effect.asVoid,
          // A failing subscriber must never stall the control loop, so the
          // failure is recorded and swallowed rather than propagated.
          //
          // Two records, and they are not redundant. The in-memory list is a
          // diagnostic: it answers "what did this instance fail to send", it
          // is bounded, and it dies with the process. The outbox is the
          // authoritative one: it survives the process, it is what gets
          // replayed, and it is the reason the per-API guarantee now reaches
          // the subscriber rather than stopping at the aggregator's edge.
          Effect.catchCause((cause) =>
            Effect.all(
              [
                Ref.update(dead, (xs) =>
                  [
                    ...xs,
                    new DeliveryFailed({ sink: "webhook", apiId, cause: String(cause) }),
                  ].slice(-DEAD_LETTER_BUFFER),
                ),
                Metric.update(Metric.withAttributes(Telemetry.webhookDeadLettered, { apiId }), 1),
                outbox.append(event).pipe(
                  Effect.flatMap((dropped) =>
                    dropped > 0
                      ? Effect.all([
                          Metric.update(
                            Metric.withAttributes(Telemetry.outboxDropped, { apiId }),
                            dropped,
                          ),
                          Effect.logWarning(
                            `outbox for ${apiId} is full — dropped ${dropped} of the oldest ` +
                              `undelivered events; the subscriber will see a gap`,
                          ),
                        ], { discard: true })
                      : Effect.void,
                  ),
                  // An unreachable outbox degrades to what this did before it
                  // existed: the event is lost and counted. It must not turn a
                  // failed delivery into a failed tick.
                  Effect.catchCause(() =>
                    Effect.logWarning(`could not persist an undelivered event for ${apiId}`),
                  ),
                ),
              ],
              { discard: true },
            ),
          ),
          // Delivery is off the hot path by construction: the loop forks it and
          // never awaits it.
          Effect.forkChild,
          Effect.asVoid,
        );
      };

      /**
       * One pass over the outbox, oldest first, per API.
       *
       * Stops that API at its first failure rather than skipping ahead: delivering
       * 8 while 7 is stuck hands the subscriber a gap that never closes. Nothing
       * is committed until delivered, so a crash mid-pass replays rather than
       * loses. Bounded per pass, so a subscriber coming back is not met with
       * everything at once.
       */
      const draining = yield* Ref.make(false);
      const drainPass = Effect.gen(function* () {
        const apis = yield* outbox.apis;
        let replayed = 0;
        for (const apiId of apis) {
          const pending = yield* outbox.peek(apiId, OUTBOX_DRAIN_LIMIT);
          // `consumed` is what the commit trims, and counts entries this pass
          // is done with — delivered or unreadable. `delivered` is what the
          // subscriber actually took. They differ only when an entry no longer
          // decodes, and conflating them is what leaves a delivered event in
          // place to be sent twice.
          let consumed = 0;
          let delivered = 0;
          let unreadable = 0;
          for (const entry of pending) {
            if (O.isNone(entry)) {
              consumed++;
              unreadable++;
              continue;
            }
            const ok = yield* post(entry.value).pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(false)),
            );
            if (!ok) break;
            consumed++;
            delivered++;
          }
          if (unreadable > 0) {
            yield* Effect.logWarning(
              `dropped ${unreadable} undeliverable outbox entr(ies) for ${apiId}: no longer decodable`,
            );
          }
          if (consumed > 0) {
            yield* outbox.commit(apiId, consumed);
          }
          if (delivered > 0) {
            yield* Metric.update(
              Metric.withAttributes(Telemetry.outboxReplayed, { apiId }),
              delivered,
            );
            replayed += delivered;
          }
          yield* outbox
            .depth(apiId)
            .pipe(
              Effect.flatMap((d) =>
                Metric.update(Metric.withAttributes(Telemetry.outboxDepth, { apiId }), d),
              ),
            );
        }
        if (replayed > 0) {
          yield* Effect.logInfo(`replayed ${replayed} event(s) from the outbox`);
        }
        return replayed;
      });

      /**
       * One pass at a time. The caller forks this, and a subscriber that hangs
       * rather than refusing costs a full timeout per pass — without the
       * guard, a tick every 250ms against a subscriber timing out at 2s would
       * stack passes until they outnumber the events they are trying to
       * deliver.
       *
       * The flag is released only by the pass that took it, which is why this
       * is not a plain `ensuring` around the whole thing.
       */
      const drainOutbox = Ref.getAndSet(draining, true).pipe(
        Effect.flatMap((busy) =>
          busy
            ? Effect.succeed(0)
            : drainPass.pipe(Effect.ensuring(Ref.set(draining, false))),
        ),
        // The outbox being unreachable is a reason to try again next tick, not
        // a reason to end the loop that is trying.
        Effect.catchCause(() => Effect.succeed(0)),
      );

      return { name: "webhook", deliver, deadLetters: Ref.get(dead), drainOutbox };
  });

/**
 * Fans one event out to every given sink and forks each delivery
 * independently, so a slow or unreachable one (e.g. RabbitMQ down while the
 * webhook is fine) never delays the others. Dead letters from all sinks are
 * pooled — a subscriber checking the delivery contract does not need to
 * know how many sinks are mounted.
 */
export const combineSinks = (sinks: ReadonlyArray<SinkImpl>): SinkImpl => ({
  name: sinks.map((s) => s.name).join("+"),
  deliver: (event) => Effect.all(sinks.map((s) => s.deliver(event)), { discard: true }),
  deadLetters: Effect.all(sinks.map((s) => s.deadLetters)).pipe(Effect.map((xs) => xs.flat())),
  // Each sink owns its own outbox, because "the webhook subscriber is down"
  // and "RabbitMQ is down" are different failures with different backlogs —
  // replaying one into the other would duplicate events on the sink that was
  // healthy all along.
  drainOutbox: Effect.all(sinks.map((s) => s.drainOutbox)).pipe(
    Effect.map((counts) => counts.reduce((a, b) => a + b, 0)),
  ),
});

/** Used by tests and by --no-webhook runs. */
export const NoopSinkLayer = Layer.succeed(EventSink, {
  name: "noop",
  deliver: () => Effect.void,
  deadLetters: Effect.succeed([]),
  drainOutbox: Effect.succeed(0),
});
