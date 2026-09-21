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
} from "@egress/rmq/WorkQueue.ts";

/**
 * The load half of the scenario: a steady stream onto `<apiId>.work`.
 *
 * Fixed rate, and it never looks at the circuit state. The problem this repo is
 * about only exists because arrivals do not stop when a third party degrades; a
 * producer that backed off would hide the backlog the fleet has to survive.
 */

type ProducerConfig = {
  readonly apiId: string;
  readonly ratePerSecond: number;
};

export const runProducer = Effect.fnUntraced(function* (cfg: ProducerConfig) {
  const rmq = yield* Rmq;
  const queue = workQueueFor(cfg.apiId);
  // Same arguments the daemons declare, because whichever container starts
  // first is what actually creates the queue and a mismatched redeclare is
  // a hard error, not a merge.
  yield* rmq.declareQueue(deadLetterQueueFor(cfg.apiId), deadLetterQueueOptions());
  yield* rmq.declareQueue(queue, workQueueOptions(cfg.apiId));
  const publisher = yield* rmq.publisherToQueue(queue, { contentType: WORK_CONTENT_TYPE, type: WORK_MESSAGE_TYPE });

  // One batch per 100ms rather than one timer per message: at a few hundred
  // messages a second the scheduling overhead of the latter dominates, and
  // nothing downstream can tell the difference.
  const perTick = Math.max(1, Math.round(cfg.ratePerSecond / 10));
  const TICK = Duration.millis(100);
  // Names this run's messages. `sent` restarts at zero with the process, so on its
  // own it would reissue the idempotency key of different work after a restart.
  const run = randomUUID().slice(0, 8);
  let sent = 0;

  yield* Effect.log(`${cfg.apiId}/producer: up — ${cfg.ratePerSecond}/s onto ${queue}`);

  yield* Effect.gen(function* () {
    // Concurrently, because `send` waits for the broker to confirm each
    // message and a sequential batch would pay that round trip twenty times
    // inside a 100ms tick — measured at 165/s against a target of 200. AMQP
    // pipelines confirms, so having the whole batch in flight at once is the
    // ordinary way to use them. Nothing downstream cares in what order these
    // particular messages arrive: they are independent units of work.
    // No idempotency key here: it is not a producer concern. `n` is what
    // makes one, downstream — a broker requeue redelivers this exact body, so
    // whoever calls the third party can derive a stable key straight from it
    // without minting or carrying one forward. See packages/consumer/src/consumer.ts.
    const batch = Array.from({ length: perTick }, () => {
      const n = sent++;
      return { body: encodeWorkMessage({ apiId: cfg.apiId, n }), messageId: workMessageId(run, n) };
    });
    yield* Effect.forEach(
      batch,
      ({ body, messageId }) =>
        // The root of every trace in this repo. The sampler decides here and
        // nowhere else — `@egress/rmq` stamps a traceparent only when a span
        // is active, and every span downstream is ParentBased, so a message
        // is either followed the whole way or not at all.
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
    // Publish rate onto the work queue is read from RabbitMQ's own metrics
    // now, not republished here — see rabbitmq_detailed_queue_*.
    if (sent % (cfg.ratePerSecond * 10) < perTick) {
      yield* Effect.log(`${cfg.apiId}/producer: ${sent} messages published`);
    }
    // `fixed`, not `spaced`: spaced waits the interval *after* each batch
    // finishes, so the period becomes 100ms plus however long the broker took
    // to confirm, and the configured rate is never the rate produced. Worse,
    // the shortfall grows with broker latency — the producer would quietly
    // back off exactly when the queue is deepest, which is the one thing the
    // comment above says it must not do. `fixed` keeps the cadence and skips
    // a tick if one ever overruns.
  }).pipe(Effect.repeat(Schedule.fixed(TICK)));
});
