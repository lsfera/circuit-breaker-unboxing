import { randomUUID } from "node:crypto";
import { Duration, Effect, Schedule } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  encodeWorkMessage,
  WORK_CONTENT_TYPE,
  WORK_MESSAGE_TYPE,
  workMessageId,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";

/**
 * The load half of the scenario: a steady stream onto `<apiId>.work`. Fixed rate, and it never reacts to the
 * third party: arrivals do not stop when it degrades, and a producer that backed off would hide the backlog
 * the fleet has to survive.
 */

type ProducerConfig = {
  readonly apiId: string;
  readonly ratePerSecond: number;
};

export const runProducer = Effect.fnUntraced(function* (cfg: ProducerConfig) {
  const rmq = yield* Rmq;
  const queue = workQueueFor(cfg.apiId);
  // Same arguments the daemons declare — whichever container starts first creates the queue.
  yield* rmq.declareQueue(deadLetterQueueFor(cfg.apiId), deadLetterQueueOptions());
  yield* rmq.declareQueue(queue, workQueueOptions(cfg.apiId));
  const publisher = yield* rmq.publisherToQueue(queue, { contentType: WORK_CONTENT_TYPE, type: WORK_MESSAGE_TYPE });

  // One batch per 100ms rather than a timer per message; nothing downstream can tell the difference.
  const perTick = Math.max(1, Math.round(cfg.ratePerSecond / 10));
  const TICK = Duration.millis(100);
  // Names this run's messages — `sent` alone would reissue a prior run's idempotency keys after a restart.
  const run = randomUUID().slice(0, 8);
  let sent = 0;

  yield* Effect.log(`${cfg.apiId}/producer: up — ${cfg.ratePerSecond}/s onto ${queue}`);

  yield* Effect.gen(function* () {
    // Concurrent: `send` waits for the broker's confirm, and a sequential batch would pay that round trip per
    // message. `messageId` is stamped once here and is the idempotency key a redelivery reuses.
    const batch = Array.from({ length: perTick }, () => {
      const n = sent++;
      return { body: encodeWorkMessage({ apiId: cfg.apiId, n }), messageId: workMessageId(run, n) };
    });
    yield* Effect.forEach(
      batch,
      ({ body, messageId }) =>
        // The root of every trace: the sampler decides here alone, since downstream spans are ParentBased.
        rmq.send(publisher, body, { messageId }).pipe(
          Effect.withSpan("work.publish", {
            attributes: {
              "messaging.system": "rabbitmq",
              "messaging.operation.name": "publish",
              "messaging.destination.name": queue,
              "egress.api_id": cfg.apiId,
            },
          }),
        ),
      { concurrency: "unbounded", discard: true },
    );
    // Publish rate is read from RabbitMQ's own metrics, not republished here.
    if (sent % (cfg.ratePerSecond * 10) < perTick) {
      yield* Effect.log(`${cfg.apiId}/producer: ${sent} messages published`);
    }
    // `fixed`, not `spaced`: spaced would add the broker's confirm time to the period, backing off exactly
    // when the queue is deepest.
  }).pipe(Effect.repeat(Schedule.fixed(TICK)));
});
