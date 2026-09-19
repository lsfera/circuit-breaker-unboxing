import { Effect, Metric } from "effect";
import { randomUUID } from "node:crypto";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  deadLetterQueueFor,
  deadLetterQueueOptions,
  IDEMPOTENCY_KEY_HEADER,
  routingKeyFor,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import * as Breaker from "./Breaker.ts";
import * as Telemetry from "./Telemetry.ts";
import { CircuitState } from "cockatiel";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * One competing-consumer daemon with an in-process circuit breaker (see
 * Breaker.ts). Its breaker's decision is still entirely private to this
 * process, formed from only the calls this process itself has made — that
 * hasn't changed, and this branch doesn't change it: each replica keeps
 * protecting itself exactly as article 2/3 built it. What's new is a
 * second, independent channel — every transition also goes out on
 * `circuit.control` for `@egress/aggregator` to fold into one published,
 * fleet-wide verdict. Publishing that event and acting on this replica's
 * own breaker are unrelated: the verdict is for telling the rest of the
 * system about an outage, not for this replica's own protection. See
 * README.md for why those are kept as different problems.
 */

export type ConsumerConfig = {
  readonly apiId: string;
  /** The one address a real client would be given — no replica names, no LB it can see through. */
  readonly egressAddr: string;
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
  readonly breaker: Breaker.BreakerConfig;
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

/** A non-2xx is thrown, not returned — cockatiel's `handleAll` policy classifies by thrown errors. */
class UpstreamCallFailed extends Error {}

/**
 * Whether a call outcome should be accepted or handed back to the broker.
 * Pulled out as a total function of the one thing that matters — pure,
 * exhaustively testable, no broker, breaker, or fetch involved.
 *
 * `"open"` and `"failed"` both requeue: the difference between them is
 * whether a call was actually attempted, which is a telemetry fact, not a
 * settlement fact. What differs operationally is upstream of this function
 * — see `OPEN_REQUEUE_DELAY_*` in `call` below.
 */
export type CallOutcome = "ok" | "failed" | "open";
export const decide = (outcome: CallOutcome): Settlement =>
  outcome === "ok" ? "accept" : "requeue";

/**
 * Held before releasing a breaker-open rejection back to the broker. Without
 * this, a message rejected instantly by an open local breaker (no call made,
 * no wait) goes straight back onto the queue and straight back to this same
 * consumer, which can spin against its own in-memory breaker at whatever
 * rate the broker will redeliver — hammering the *broker* even though the
 * third party is no longer being hammered. Same constants, same reasoning as
 * the shed-`429` hold the article series' predecessor daemon used before it
 * was removed: jittered so a fleet whose breakers open together doesn't
 * requeue in lockstep either.
 */
const OPEN_REQUEUE_DELAY_MIN_MS = 100;
const OPEN_REQUEUE_DELAY_MAX_MS = 400;

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handler below (amqplib's own callback, not an
  // Effect fiber) can still update metrics through this process's services —
  // see rmq-consumer/src/daemon.ts's identical comment on why the bare
  // `Effect.run*` entry points are wrong here. `Rmq` itself is in the
  // capture now too: `Breaker.withPermit` needs it to run the probe-permit
  // queue's own `get`/`nack`.
  const services = yield* Effect.context<Rmq>();
  const runInContext = Effect.runPromiseWith(services);

  // One breaker for the process's whole life, shared across every message —
  // see Breaker.ts for why that sharing is load-bearing, not incidental.
  const breaker = Breaker.make(cfg.breaker);

  // Every replica seeds the same permit queue; RabbitMQ's own
  // x-max-length/x-overflow keeps exactly one token regardless of how many
  // replicas race this on startup — see Breaker.ts's module doc.
  yield* Breaker.seedPermit(cfg.apiId);

  // This replica's identity on `circuit.control` only — Prometheus tells
  // replicas apart by scrape IP already, but an AMQP event has no IP to
  // reuse, and nothing needs this id to be anything but unique per process.
  const instance = randomUUID();
  // Durable: a control-plane exchange should survive a broker restart the
  // same way every queue here already does — declareTopicExchange's own
  // default is false, sized for a throwaway exchange, not this one.
  yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, { durable: true });
  const controlPub = yield* rmq.publisherToExchange(CONTROL_EXCHANGE, routingKeyFor(cfg.apiId));

  breaker.onStateChange((state: CircuitState) => {
    runInContext(Metric.update(Telemetry.breakerState, Breaker.STATE_CODE[state]));
    // Off the hot path — transitions are rare, never per-message — so a
    // fire-and-forget publish costs nothing here the way it would inside
    // `call`. Logged and dropped on failure rather than retried: this
    // branch stays single-instance and notification-only on purpose, see
    // README.md's "what this still doesn't fix."
    runInContext(
      rmq.send(
        controlPub,
        JSON.stringify({
          apiId: cfg.apiId,
          instance,
          state: Breaker.STATE_NAME[state],
          at: Date.now(),
        }),
      ),
    ).catch((err: unknown) => {
      runInContext(
        Effect.logWarning(`${cfg.apiId}/consumer: circuit.control publish failed`, err),
      ).catch(() => {});
    });
  });
  breaker.onBreak(() => {
    runInContext(Metric.update(Telemetry.breakerTrips, 1));
    runInContext(Effect.log(`${cfg.apiId}/consumer: breaker opened`));
  });
  breaker.onReset(() => runInContext(Effect.log(`${cfg.apiId}/consumer: breaker closed`)));

  let inFlight = 0;
  const track = (outcome: CallOutcome) =>
    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome }), 1));

  const callUpstream = async (n: number): Promise<void> => {
    inFlight++;
    runInContext(Metric.update(Telemetry.inFlight, inFlight));
    try {
      const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
        signal: AbortSignal.timeout(2000),
        headers: { [IDEMPOTENCY_KEY_HEADER]: `${cfg.apiId}:${n}` },
      });
      // Drain the body even though nothing wants it: an unconsumed response
      // holds its connection out of the pool.
      await res.text().catch(() => {});
      if (!res.ok) throw new UpstreamCallFailed(`status ${res.status}`);
    } finally {
      inFlight--;
      runInContext(Metric.update(Telemetry.inFlight, inFlight));
    }
  };

  const call = async (body: string, _delivery: DeliveryInfo): Promise<Settlement> => {
    const message = parse(body);
    if (message === undefined) return "discard";

    // Checked before execute() rather than inside the wrapped function:
    // cockatiel decides Closed/Open/HalfOpen itself when execute() actually
    // runs, so a race between this read and that decision is possible but
    // harmless — worst case one call takes the wrong branch below for a
    // state that flipped in the last few microseconds, and cockatiel's own
    // switch still applies the real rule regardless of which function it
    // was handed. Only HalfOpen changes behavior: Closed and Open are
    // unaffected by which function this passes to execute().
    const attemptUpstream =
      breaker.state === CircuitState.HalfOpen
        ? () => runInContext(Breaker.withPermit(cfg.apiId, () => callUpstream(message.n)))
        : () => callUpstream(message.n);

    let outcome: CallOutcome;
    try {
      await breaker.execute(attemptUpstream);
      outcome = "ok";
    } catch (err) {
      // Breaker.isBrokenCircuitError: rejected locally, no call attempted —
      // this replica's own breaker is open. Breaker.NoPermit: a half-open
      // probe this replica wanted to make, but lost the fleet-wide permit
      // race for — also no call attempted, same telemetry story as being
      // open. Anything else is a real call that failed (timeout, connection
      // refused, or UpstreamCallFailed).
      outcome =
        Breaker.isBrokenCircuitError(err) || err instanceof Breaker.NoPermit ? "open" : "failed";
    }

    track(outcome);
    if (outcome === "open") {
      const jitter =
        OPEN_REQUEUE_DELAY_MIN_MS + Math.random() * (OPEN_REQUEUE_DELAY_MAX_MS - OPEN_REQUEUE_DELAY_MIN_MS);
      await new Promise((resolve) => setTimeout(resolve, jitter));
    }
    return decide(outcome);
  };

  yield* rmq.consume(workQueue, (body, delivery) => call(body, delivery), {
    prefetch: cfg.maxInFlight,
  });

  // Set at startup so the series exists before the first state change.
  yield* Metric.update(Telemetry.breakerState, Breaker.INITIAL_STATE_CODE);

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} ` +
      `breaker=${cfg.breaker.consecutiveFailures}consecutive/${cfg.breaker.initialDelayMs}-${cfg.breaker.maxDelayMs}ms`,
  );
});
