import { Duration, Effect, Schedule } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import { workQueueFor } from "@egress/rmq/ControlPlane.ts";

/**
 * The "high throughput of requests" half of the scenario: a steady stream of
 * work onto `<apiId>.work` for the daemon fleet to drain, one call to the
 * flaky third party per message.
 *
 * It publishes at a fixed rate and never looks at the circuit state — that
 * is deliberate. The whole problem this repo is about only exists because
 * the arrival rate does *not* drop when the third party degrades; a producer
 * that backed off on its own would hide the backlog the fleet has to survive.
 * Watch the queue depth climb through OPEN and drain back down as the ramp
 * brings daemons back — that gap is the thundering herd, made visible.
 */

export type ProducerConfig = {
  readonly apiId: string;
  readonly ratePerSecond: number;
};

export const runProducer = (cfg: ProducerConfig) =>
  Effect.gen(function* () {
    const rmq = yield* Rmq;
    const queue = workQueueFor(cfg.apiId);
    yield* rmq.declareQueue(queue);
    const publisher = yield* rmq.publisherToQueue(queue);

    // One batch per 100ms rather than one timer per message: at a few hundred
    // messages a second the scheduling overhead of the latter dominates, and
    // nothing downstream can tell the difference.
    const perTick = Math.max(1, Math.round(cfg.ratePerSecond / 10));
    let sent = 0;

    yield* Effect.log(`${cfg.apiId}/producer: up — ${cfg.ratePerSecond}/s onto ${queue}`);

    yield* Effect.gen(function* () {
      for (let i = 0; i < perTick; i++) {
        yield* rmq.send(publisher, JSON.stringify({ apiId: cfg.apiId, n: sent++ }));
      }
      if (sent % (cfg.ratePerSecond * 10) < perTick) {
        yield* Effect.log(`${cfg.apiId}/producer: ${sent} messages published`);
      }
    }).pipe(Effect.repeat(Schedule.spaced(Duration.millis(100))));
  });
