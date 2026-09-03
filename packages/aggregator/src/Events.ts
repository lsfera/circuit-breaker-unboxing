import {
  Context,
  Duration,
  Effect,
  Layer,
  Metric,
  PubSub,
  Ref,
  Schedule,
  Stream,
} from "effect";
import { DeliveryFailed } from "@egress/domain/Model.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSnapshot, CircuitEvent, State } from "@egress/domain/Model.ts";

export const SOURCE = "egress-proxy/control-plane";

const build = (
  type: CircuitEvent["type"],
  snap: ApiSnapshot,
  previousState: State | null,
): CircuitEvent => ({
  specversion: "1.0",
  type,
  source: SOURCE,
  subject: `api://${snap.apiId}`,
  id: crypto.randomUUID(),
  time: new Date().toISOString(),
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

export const stateChanged = (snap: ApiSnapshot, previous: State | null) =>
  build("egress.circuit.state_changed", snap, previous);

export const snapshotEvent = (snap: ApiSnapshot) =>
  build("egress.circuit.snapshot", snap, null);

// ---------------------------------------------------------------------------
// EventBus — one PubSub, many subscribers.
//
// In v1 this was an ad-hoc Set of callbacks plus a hand-rolled ring buffer.
// PubSub gives backpressure and per-subscriber cursors for free, and each SSE
// client becomes a Stream rather than a registered listener that must be
// remembered and cleaned up.
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
  }
>()("EventSink") {}

/** The shape every sink builds — split out from the Layer wrapper so main.ts can compose several before mounting the one EventSink tag. */
export type SinkImpl = {
  readonly name: string;
  readonly deliver: (event: CircuitEvent) => Effect.Effect<void>;
  readonly deadLetters: Effect.Effect<ReadonlyArray<DeliveryFailed>>;
};

/**
 * Stand-in for the real middleware hop. In production this is a produce() to
 * Kafka or NATS keyed by apiId; HTTP keeps the demo broker-free.
 *
 * The retry policy is the reason this is worth doing in Effect: v1 hand-rolled
 * a loop with a counter, a sleep and a try/catch. Here the policy is a value —
 * exponential backoff, capped attempts — and it composes.
 */
export const makeWebhookSink = (url: string): Effect.Effect<SinkImpl> =>
  Effect.gen(function* () {
      const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);

      const post = (event: CircuitEvent) =>
        Effect.tryPromise({
          try: (signal) =>
            fetch(url, {
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
            }),
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
          Effect.retry({
            schedule: Schedule.exponential(Duration.millis(100)),
            times: 3,
          }),
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
          Effect.catchCause((cause) =>
            Effect.all(
              [
                Ref.update(dead, (xs) => [
                  ...xs,
                  new DeliveryFailed({ sink: "webhook", apiId, cause: String(cause) }),
                ]),
                Metric.update(Metric.withAttributes(Telemetry.webhookDeadLettered, { apiId }), 1),
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

      return { name: "webhook", deliver, deadLetters: Ref.get(dead) };
  });

export const WebhookSinkLayer = (url: string) => Layer.effect(EventSink, makeWebhookSink(url));

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
});

/** Used by tests and by --no-webhook runs. */
export const NoopSinkLayer = Layer.succeed(EventSink, {
  name: "noop",
  deliver: () => Effect.void,
  deadLetters: Effect.succeed([]),
});
