import { Duration, Effect, Layer, Ref, Schedule, Semaphore } from "effect";
import { Rmq, RmqError } from "@egress/rmq/Client.ts";
import { CONTROL_EXCHANGE, encodeCircuitEvent, routingKeyFor } from "@egress/rmq/ControlPlane.ts";
import { DeliveryFailed } from "@egress/domain/Model.ts";
import { EventSink } from "./Events.ts";
import type { SinkImpl } from "./Events.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";
import type { Publisher } from "@egress/rmq/Client.ts";

/**
 * A peer to WebhookSink, publishing the same CircuitEvent to
 * `circuit.control` instead of POSTing a webhook — the transport
 * `docs/rmq-control-plane.md`'s daemon fleet actually subscribes to. Same
 * `EventSink` contract: bounded retry, dead-letter on exhaustion, delivery
 * forked off the hot path so a slow or unreachable broker never stalls the
 * tick loop.
 *
 * One publisher per apiId, declared lazily on first delivery and cached —
 * this library's publisher binds a fixed (exchange, routingKey) at creation
 * (see Client.ts's module doc), so a publisher per apiId is the natural
 * shape, not a workaround.
 *
 * Every RMQ operation here runs under one semaphore permit, serializing them
 * on the shared connection. This is not defensive boilerplate: verified live
 * that `rabbitmq-amqp-js-client` mis-routes messages when
 * `createPublisher` is called concurrently for different targets on one
 * connection — e.g. three events firing on the aggregator's first tick, one
 * per apiId, each independently forked, all landed on the *first* publisher
 * created, silently. The library has no documented guarantee about
 * concurrent link creation either way, so `send` is serialized too rather
 * than assuming only `createPublisher` is affected. See
 * `docs/rmq-control-plane.md` for the reproduction.
 */
export const makeAmqpControlPlaneSink: Effect.Effect<SinkImpl, RmqError, Rmq> = Effect.gen(
  function* () {
    const rmq = yield* Rmq;
    yield* rmq.declareTopicExchange(CONTROL_EXCHANGE);

    const dead = yield* Ref.make<ReadonlyArray<DeliveryFailed>>([]);
    const publishers = yield* Ref.make(new Map<string, Publisher>());
    const gate = yield* Semaphore.make(1);

    const publisherFor = (apiId: string) =>
      Ref.get(publishers).pipe(
        Effect.flatMap((map) => {
          const existing = map.get(apiId);
          if (existing) return Effect.succeed(existing);
          return rmq.publisherToExchange(CONTROL_EXCHANGE, routingKeyFor(apiId)).pipe(
            Effect.tap((pub) => Ref.update(publishers, (m) => new Map(m).set(apiId, pub))),
          );
        }),
      );

    const publish = (event: CircuitEvent) =>
      gate
        .withPermit(
          publisherFor(event.data.apiId).pipe(
            Effect.flatMap((pub) => rmq.send(pub, encodeCircuitEvent(event))),
          ),
        )
        .pipe(
          Effect.mapError(
            (e: RmqError) =>
              new DeliveryFailed({ sink: "amqp", apiId: event.data.apiId, cause: String(e.cause) }),
          ),
        );

    const deliver = (event: CircuitEvent) => {
      const apiId = event.data.apiId;
      return publish(event).pipe(
        Effect.retry({ schedule: Schedule.exponential(Duration.millis(100)), times: 3 }),
        Effect.catchCause((cause) =>
          Ref.update(dead, (xs) => [
            ...xs,
            new DeliveryFailed({ sink: "amqp", apiId, cause: String(cause) }),
          ]),
        ),
        // Delivery is off the hot path by construction, same as WebhookSink.
        Effect.forkChild,
        Effect.asVoid,
      );
    };

    return { name: "amqp", deliver, deadLetters: Ref.get(dead) };
  },
);

export const AmqpControlPlaneSinkLayer = Layer.effect(EventSink, makeAmqpControlPlaneSink);
