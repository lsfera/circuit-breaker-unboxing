import {
  Array as Arr,
  Context,
  DateTime,
  Deferred,
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
import { Outbox, OUTBOX_DRAIN_LIMIT } from "@egress/coordination/Outbox.ts";
import type { CoordinationUnavailable } from "@egress/coordination/Coordination.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSnapshot, CircuitEvent, EventType, State } from "@egress/domain/Model.ts";

export const SOURCE = "egress-proxy/control-plane";

/** A diagnostic buffer, not a ledger; the exact total is the metric. */
export const DEAD_LETTER_BUFFER = 200;

/** Shared by both sinks. Low: the webhook's outbox, not retrying, is the answer to a subscriber that stays down. */
export const DELIVERY_RETRY = {
  schedule: Schedule.exponential(Duration.millis(100)),
  times: 3,
} as const;

/** `now` from the caller's Effect clock, so envelope and payload share one clock. */
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
  time: DateTime.formatIso(DateTime.makeUnsafe(now)),
  datacontenttype: "application/json",
  data: {
    apiId: snap.apiId,
    sequence: snap.sequence,
    previousState,
    state: snap.state,
    reason: snap.reason,
    healthyEndpoints: snap.healthyEndpoints,
    totalEndpoints: snap.totalEndpoints,
    observedSince: DateTime.formatIso(DateTime.makeUnsafe(snap.observedSince)),
    reportingReplicas: snap.reportingReplicas,
  },
});

// `null`: the published schema is `NullOr`, and the first event has no predecessor.
export const stateChanged = (snap: ApiSnapshot, previous: State | null, now: number) =>
  build(SEQUENCED_EVENT, snap, previous, now);

export const snapshotEvent = (snap: ApiSnapshot, now: number) =>
  build(SNAPSHOT_EVENT, snap, null, now);

export class EventBus extends Context.Service<
  EventBus,
  {
    readonly publish: (event: CircuitEvent) => Effect.Effect<void>;
    readonly subscribe: Stream.Stream<CircuitEvent>;
    readonly recent: Effect.Effect<ReadonlyArray<CircuitEvent>>;
  }
>()("@egress/aggregator/Events/EventBus") {
  static readonly layer = Layer.effect(
    EventBus,
    Effect.gen(function* () {
      const pubsub = yield* PubSub.sliding<CircuitEvent>(256);
      const ring = yield* Ref.make<ReadonlyArray<CircuitEvent>>([]);
      return EventBus.of({
        publish: (event) =>
          Ref.update(ring, (xs) => [...xs, event].slice(-200)).pipe(
            Effect.andThen(PubSub.publish(pubsub, event)),
            Effect.asVoid,
          ),
        subscribe: Stream.fromPubSub(pubsub),
        recent: Ref.get(ring),
      });
    }),
  );
}

export class EventSink extends Context.Service<
  EventSink,
  {
    readonly name: string;
    /** See SinkImpl: completes once delivery is leadership-relevant-confirmed, or fails. */
    readonly deliver: (event: CircuitEvent) => Effect.Effect<void, DeliveryFailed>;
    readonly deadLetters: Effect.Effect<ReadonlyArray<DeliveryFailed>>;
    /** See SinkImpl: replay what an earlier attempt could not deliver, leader-only. */
    readonly drainOutbox: Effect.Effect<number>;
    /** See SinkImpl: whether this sink can currently deliver. */
    readonly ready: Effect.Effect<boolean>;
    /** See SinkImpl: fence off buffered, unconfirmed publishes on step-down or demotion. */
    readonly resetConnection: Effect.Effect<void>;
  }
>()("@egress/aggregator/Events/EventSink") {}

/** The shape every sink builds — split out from the Layer wrapper so main.ts can compose several before mounting the one EventSink tag. */
export type SinkImpl = {
  readonly name: string;
  /**
   * Fails if the event did not reach what leadership depends on; the tick
   * checkpoints only on success. The AMQP sink awaits its confirm; the webhook
   * sink forks and returns, since a subscriber's outage is not a leadership question.
   */
  readonly deliver: (event: CircuitEvent) => Effect.Effect<void, DeliveryFailed>;
  readonly deadLetters: Effect.Effect<ReadonlyArray<DeliveryFailed>>;
  /** Replay what earlier attempts could not deliver. Leader-only, or every event goes out twice. */
  readonly drainOutbox: Effect.Effect<number>;
  /** A leader whose sink is not ready steps down rather than publish into nothing. */
  readonly ready: Effect.Effect<boolean>;
  /** On demotion: stop anything written but unconfirmed from landing after a new leader moved on. */
  readonly resetConnection: Effect.Effect<void>;
};

/** Stand-in for a real middleware hop (Kafka or NATS keyed by apiId). */
export const makeWebhookSink = Effect.fnUntraced(function* (url: string): Effect.fn.Return<SinkImpl, never, Outbox> {
    const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
    const outbox = yield* Outbox;

    const post = (event: CircuitEvent) =>
      Effect.tryPromise({
        try: async (signal) => {
          const res = await fetch(url, {
            method: "POST",
            headers: {
              "content-type": "application/cloudevents+json",
              // Per-API ordering is the only ordering subscribers need.
              "ce-partitionkey": event.data.apiId,
              "idempotency-key": `${event.data.apiId}:${event.data.sequence}`,
            },
            body: JSON.stringify(event),
            signal,
          });
          // An unconsumed body keeps its connection out of the pool.
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

    /**
     * Everything that reaches the subscriber for one API goes through here, in the
     * order it was registered: each delivery waits for the one before it. Forked
     * POSTs used to race, and an event posted while an earlier one sat in the
     * outbox overtook it.
     */
    const tails = new Map<string, Effect.Effect<void>>();
    const inOrder = <A, E>(apiId: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E> => {
      const done = Deferred.makeUnsafe<void>();
      const previous = tails.get(apiId) ?? Effect.void;
      tails.set(apiId, Deferred.await(done));
      return previous.pipe(Effect.andThen(effect), Effect.ensuring(Deferred.succeed(done, undefined)));
    };

    const keep = (event: CircuitEvent) => {
      const apiId = event.data.apiId;
      return outbox.append(event).pipe(
        Effect.flatMap((dropped) =>
          dropped > 0
            ? Effect.all([
                Metric.update(Metric.withAttributes(Telemetry.outboxDropped, { apiId }), dropped),
                Effect.logWarning(
                  `outbox for ${apiId} is full — dropped ${dropped} of the oldest ` +
                    `undelivered events; the subscriber will see a gap`,
                ),
              ], { discard: true })
            : Effect.void,
        ),
        // An unreachable outbox loses the event, counted; never a failed tick.
        Effect.catchCause((cause) =>
          Effect.logWarning(`could not persist an undelivered event for ${apiId}`, cause),
        ),
      );
    };

    const send = (event: CircuitEvent) => {
      const apiId = event.data.apiId;
      const attempt = post(event).pipe(
        Effect.retry(DELIVERY_RETRY),
        Effect.tapError(() => Metric.update(Metric.withAttributes(Telemetry.webhookFailed, { apiId }), 1)),
      );
      const direct = Effect.timed(attempt).pipe(
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
        // Swallowed: a failing subscriber must not stall the loop. The outbox is
        // what gets replayed; the list is a bounded diagnostic.
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
              keep(event),
            ],
            { discard: true },
          ),
        ),
      );
      // Behind anything already waiting in the outbox, or it would arrive first.
      return outbox.depth(apiId).pipe(
        Effect.orElseSucceed(() => 0),
        Effect.flatMap((waiting) => (waiting > 0 ? keep(event) : direct)),
      );
    };

    const deliver = (event: CircuitEvent) =>
      Effect.suspend(() => Effect.forkChild(inOrder(event.data.apiId, send(event)))).pipe(Effect.asVoid);

    /**
     * Oldest first per API, stopping at the first failure: delivering 8 while 7
     * is stuck is a gap that never closes. Committed only after delivery.
     */
    const draining = yield* Ref.make(false);
    /**
     * `consumed` (delivered or unreadable) is what the commit trims; `delivered`
     * is what the subscriber took. Conflating them sent events twice.
     */
    type Pass = {
      readonly consumed: number;
      readonly delivered: number;
      readonly unreadable: number;
      readonly stopped: boolean;
    };

    const drainApi = Effect.fnUntraced(function* (apiId: string) {
      const { from, entries } = yield* outbox.peek(apiId, OUTBOX_DRAIN_LIMIT);
      const { consumed, delivered, unreadable } = yield* Effect.reduce(
        entries,
        (): Pass => ({ consumed: 0, delivered: 0, unreadable: 0, stopped: false }),
        (acc, entry): Effect.Effect<Pass, CoordinationUnavailable> =>
          acc.stopped
            ? Effect.succeed(acc)
            : O.match(entry, {
                onNone: () =>
                  Effect.succeed({
                    ...acc,
                    consumed: acc.consumed + 1,
                    unreadable: acc.unreadable + 1,
                  }),
                onSome: (event) =>
                  post(event).pipe(
                    Effect.as({
                      ...acc,
                      consumed: acc.consumed + 1,
                      delivered: acc.delivered + 1,
                    }),
                    Effect.catchCause(() => Effect.succeed({ ...acc, stopped: true })),
                  ),
              }),
      );

      yield* unreadable > 0
        ? Effect.logWarning(
            `dropped ${unreadable} undeliverable outbox entr(ies) for ${apiId}: no longer decodable`,
          )
        : Effect.void;
      yield* consumed > 0 ? outbox.commit(apiId, from + consumed) : Effect.void;
      yield* delivered > 0
        ? Metric.update(Metric.withAttributes(Telemetry.outboxReplayed, { apiId }), delivered)
        : Effect.void;
      yield* outbox
        .depth(apiId)
        .pipe(
          Effect.flatMap((d) =>
            Metric.update(Metric.withAttributes(Telemetry.outboxDepth, { apiId }), d),
          ),
        );
      return delivered;
    });

    const drainPass = Effect.gen(function* () {
      const apis = yield* outbox.apis;
      const replayed = Arr.reduce(
        yield* Effect.forEach(apis, (apiId) => inOrder(apiId, drainApi(apiId))),
        0,
        (a, b) => a + b,
      );
      yield* replayed > 0
        ? Effect.logInfo(`replayed ${replayed} event(s) from the outbox`)
        : Effect.void;
      return replayed;
    });

    /**
     * One pass at a time: a hanging subscriber costs a 2s timeout per pass, and
     * a 250ms tick would stack them.
     */
    const drainable = yield* Ref.make(true);
    const drainOutbox = Ref.getAndSet(draining, true).pipe(
      Effect.flatMap((busy) =>
        busy
          ? Effect.succeed(0)
          : drainPass.pipe(
              Effect.ensuring(Ref.set(draining, false)),
              Effect.tap(() =>
                Ref.getAndSet(drainable, true).pipe(
                  Effect.flatMap((was) =>
                    was ? Effect.void : Effect.logInfo("the outbox is readable again"),
                  ),
                ),
              ),
            ),
      ),
      // Retried next tick, and logged on the edge (once down, once back).
      Effect.catchCause((cause) =>
        Ref.getAndSet(drainable, false).pipe(
          Effect.flatMap((was) =>
            was
              ? Effect.logWarning("could not replay the outbox", cause)
              : Effect.void,
          ),
          Effect.as(0),
        ),
      ),
    );

    // Always ready, nothing to fence: the outbox carries a subscriber's outage.
    return {
      name: "webhook",
      deliver,
      deadLetters: Ref.get(dead),
      drainOutbox,
      ready: Effect.succeed(true),
      resetConnection: Effect.void,
    };
});

/** Fans out to every sink; fails, or is unready, if any one is. */
export const combineSinks = (sinks: ReadonlyArray<SinkImpl>): SinkImpl => ({
  name: sinks.map((s) => s.name).join("+"),
  deliver: (event) => Effect.all(sinks.map((s) => s.deliver(event)), { discard: true }),
  deadLetters: Effect.all(sinks.map((s) => s.deadLetters)).pipe(Effect.map((xs) => xs.flat())),
  drainOutbox: Effect.all(sinks.map((s) => s.drainOutbox)).pipe(
    Effect.map((counts) => counts.reduce((a, b) => a + b, 0)),
  ),
  ready: Effect.all(sinks.map((s) => s.ready)).pipe(Effect.map((rs) => rs.every((r) => r))),
  resetConnection: Effect.all(sinks.map((s) => s.resetConnection), { discard: true }),
});

/** Used by tests and by --no-webhook runs. */
export const NoopSinkLayer = Layer.succeed(EventSink, {
  name: "noop",
  deliver: () => Effect.void,
  deadLetters: Effect.succeed([]),
  drainOutbox: Effect.succeed(0),
  ready: Effect.succeed(true),
  resetConnection: Effect.void,
});
