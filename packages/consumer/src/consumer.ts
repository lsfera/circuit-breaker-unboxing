import { Effect, Metric } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  IDEMPOTENCY_KEY_HEADER,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import * as Telemetry from "./Telemetry.ts";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * The base scenario: one competing-consumer daemon, no circuit awareness at
 * all. It knows nothing about the other daemons in its own fleet and nothing
 * about whether the third party is degrading — only whether *its own* last
 * call succeeded. A failed call is handed back to the broker with `requeue`,
 * and the broker's own `x-delivery-limit` (see `workQueueOptions`) is what
 * eventually dead-letters it. Nothing here backs off, coordinates, or stops.
 * That absence is the point: it is what the rest of this article series is
 * about adding, one piece at a time.
 */

export type ConsumerConfig = {
  readonly apiId: string;
  /** The one address a real client would be given — no replica names, no LB it can see through. */
  readonly egressAddr: string;
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
};

/** Body shape the producer publishes: `{ apiId, n }`. `n` is what makes the idempotency key stable across a broker redelivery of the same message. */
type WorkMessage = { readonly apiId: string; readonly n: number };

const parse = (body: string): WorkMessage | undefined => {
  try {
    const value: unknown = JSON.parse(body);
    if (
      typeof value === "object" &&
      value !== null &&
      "apiId" in value &&
      "n" in value &&
      typeof (value as { n: unknown }).n === "number"
    ) {
      return value as WorkMessage;
    }
    return undefined;
  } catch {
    return undefined;
  }
};

/**
 * Whether a call outcome should be accepted or handed back to the broker.
 * Pulled out as a total function of the one thing that matters — pure,
 * exhaustively testable, no broker or fetch involved.
 */
export type CallOutcome = "ok" | "failed";
export const decide = (outcome: CallOutcome): Settlement =>
  outcome === "ok" ? "accept" : "requeue";

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handler below (amqplib's own callback, not an
  // Effect fiber) can still update metrics through this process's services —
  // see rmq-consumer/src/daemon.ts's identical comment on why the bare
  // `Effect.run*` entry points are wrong here.
  const services = yield* Effect.context<never>();
  const runInContext = Effect.runPromiseWith(services);

  let inFlight = 0;
  const track = (outcome: CallOutcome) =>
    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome }), 1));

  const call = async (body: string, _delivery: DeliveryInfo): Promise<Settlement> => {
    const message = parse(body);
    if (message === undefined) return "discard";

    inFlight++;
    runInContext(Metric.update(Telemetry.inFlight, inFlight));
    let outcome: CallOutcome;
    try {
      const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
        signal: AbortSignal.timeout(2000),
        headers: { [IDEMPOTENCY_KEY_HEADER]: `${cfg.apiId}:${message.n}` },
      });
      // Drain the body even though nothing wants it: an unconsumed response
      // holds its connection out of the pool.
      await res.text().catch(() => {});
      outcome = res.ok ? "ok" : "failed";
    } catch {
      // Timeout or connection refused — the ordinary shape of a third party
      // that is down. Nothing here distinguishes it from any other failure:
      // that distinction is exactly what a breaker exists to make.
      outcome = "failed";
    } finally {
      inFlight--;
      runInContext(Metric.update(Telemetry.inFlight, inFlight));
    }

    track(outcome);
    return decide(outcome);
  };

  yield* rmq.consume(workQueue, (body, delivery) => call(body, delivery), {
    prefetch: cfg.maxInFlight,
  });

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue}`,
  );
});
