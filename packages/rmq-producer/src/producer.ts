import { randomUUID } from "node:crypto";
import { Duration, Effect, Schedule } from "effect";
import protobuf from "protobufjs";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  encodeWorkMessage,
  workMessageId,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/WorkQueue.ts";
import type { WorkMessage } from "@egress/rmq/WorkQueue.ts";

/**
 * The load half of the scenario: a steady stream onto `<apiId>.work`. Fixed rate, and it never reacts to the
 * third party: arrivals do not stop when it degrades, and a producer that backed off would hide the backlog
 * the fleet has to survive.
 */

/** `message Work { string api_id = 1; int64 n = 2; }`, what a consumer reading `application/x-protobuf` expects. */
const WorkProto = protobuf.Type.fromJSON("Work", {
  fields: { apiId: { type: "string", id: 1 }, n: { type: "int64", id: 2 } },
});

/**
 * How a body is written, and the `content_type` that says so. RabbitMQ neither validates nor uses it, nor `type`;
 * what a consumer reads is its own negotiation (`accept` in `@egress/rmq-consumer`).
 */
const FORMATS = {
  json: { contentType: "application/json", encode: encodeWorkMessage },
  protobuf: { contentType: "application/x-protobuf", encode: (m: WorkMessage) => WorkProto.encode(m).finish() },
} as const;

type ProducerConfig = {
  readonly apiId: string;
  readonly ratePerSecond: number;
  readonly format: keyof typeof FORMATS;
};

export const runProducer = Effect.fnUntraced(function* (cfg: ProducerConfig) {
  const rmq = yield* Rmq;
  const queue = workQueueFor(cfg.apiId);
  // Same arguments the daemons declare: whichever container starts first creates the queue, and a mismatched
  // redeclare is a hard error, not a merge.
  yield* rmq.declareQueue(deadLetterQueueFor(cfg.apiId), deadLetterQueueOptions());
  yield* rmq.declareQueue(queue, workQueueOptions(cfg.apiId));
  const { contentType, encode } = FORMATS[cfg.format];
  const publisher = yield* rmq.publisherToQueue(queue, { contentType, type: "egress.work" });

  // One batch per 100ms rather than a timer per message; nothing downstream can tell the difference.
  const perTick = Math.max(1, Math.round(cfg.ratePerSecond / 10));
  const TICK = Duration.millis(100);
  // Names this run's messages. `sent` restarts at zero with the process, so on its
  // own it would reissue the idempotency key of different work after a restart.
  const run = randomUUID().slice(0, 8);
  let sent = 0;
  // A failed batch is skipped, not fatal: one nack or a publish channel closing under
  // an unconfirmed batch would otherwise end the loop and the process. Logged on the
  // edges only; `lost` still ends the process.
  let failing = false;

  yield* Effect.log(`${cfg.apiId}/producer: up — ${cfg.ratePerSecond}/s of ${cfg.format} onto ${queue}`);

  yield* Effect.gen(function* () {
    // One batch per tick: published back to back on the confirm channel and confirmed together, one round trip
    // for the tick rather than one per message. `messageId` is stamped once here and is the idempotency key the
    // consumer sends: a redelivery is the same message with the same id.
    const batch = Array.from({ length: perTick }, () => {
      const n = sent++;
      return { body: encode({ apiId: cfg.apiId, n }), messageId: workMessageId(run, n) };
    });
    const published = yield* rmq.sendBatch(publisher, batch).pipe(
      // The root of every trace: the sampler decides here alone (`@egress/rmq` stamps a traceparent only when a span
      // is active and downstream spans are ParentBased), so a tick's messages are followed the whole way or not at all.
      Effect.withSpan("work.publish", {
        attributes: {
          "messaging.system": "rabbitmq",
          "messaging.operation.name": "publish",
          "messaging.destination.name": queue,
          "messaging.batch.message_count": batch.length,
          "egress.api_id": cfg.apiId,
        },
      }),
      Effect.as(true),
      Effect.catch((error) =>
        failing
          ? Effect.succeed(false)
          : Effect.as(Effect.logWarning(`${cfg.apiId}/producer: publishing failed, still trying — ${error.message}`), false),
      ),
    );
    yield* published && failing ? Effect.log(`${cfg.apiId}/producer: publishing again`) : Effect.void;
    failing = !published;
    // Publish rate is read from RabbitMQ's own metrics, not republished here.
    yield* Effect.when(
      Effect.log(`${cfg.apiId}/producer: ${sent} messages published`),
      Effect.sync(() => sent % (cfg.ratePerSecond * 10) < perTick),
    );
    // `fixed`, not `spaced`: spaced waits after each batch, so the period becomes 100ms plus the broker's confirm
    // time and the producer would quietly back off exactly when the queue is deepest. `fixed` keeps the cadence and
    // skips a tick that overruns.
  }).pipe(Effect.repeat(Schedule.fixed(TICK)));
});
