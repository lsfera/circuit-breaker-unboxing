import { Effect, Metric, Option as O, Queue } from "effect";
import type { HttpClient } from "effect/unstable/http";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  decodeWorkMessage,
  readsWorkFormat,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import * as Delay from "@egress/rmq/DelayedDelivery.ts";
import * as Breaker from "./Breaker.ts";
import * as Telemetry from "./Telemetry.ts";
import * as Upstream from "./Upstream.ts";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * One competing-consumer daemon behind a circuit breaker that lives in the
 * broker (see Breaker.ts): tripping cancels this replica's consumer, and a
 * message sent through the delay chain is what brings it back. It still knows
 * nothing about the other daemons — five replicas are five breakers, each with
 * its own token — but none of them holds a timer, a state machine's clock, or
 * an open breaker's rejected messages: an open replica is simply not consuming.
 */

export type ConsumerConfig = {
  readonly apiId: string;
  /** The one address a real client would be given — no replica names, no LB it can see through. */
  readonly egressAddr: string;
  readonly apiPath: string;
  /** Concurrent third-party calls, applied as the work consumer's prefetch. */
  readonly maxInFlight: number;
  /** Names this replica's wake queue, so the token finds only this process. */
  readonly replicaId: string;
  readonly breaker: Breaker.BreakerConfig;
};

/**
 * Whether a call outcome should be accepted or handed back to the broker.
 * Pulled out as a total function of what matters — pure, exhaustively
 * testable, no broker, breaker, or fetch involved.
 *
 * A failure is charged to the message (`requeue` counts toward the queue's
 * delivery budget) only when it stands alone. One that follows another failure
 * on the same replica — `streak` above 1 — or that is a probe is evidence about
 * the third party, not about the message, and is `release`d: handed back with
 * no strike. The breaker needs `consecutiveFailures` calls to open and its
 * consumer takes a round trip to stop, and in that window the same few messages
 * are redelivered to it again and again: charged, a message dead-letters with
 * nothing wrong with it, which the chaos run found at 2–3 messages per outage
 * under a 1,000/s spike. A message that fails between successes — a poison
 * message on a healthy third party — is still charged, and still parked.
 */
export type Role = "work" | "probe";
export const decide = (outcome: Upstream.CallOutcome, role: Role = "work", streak = 1): Settlement =>
  outcome === "ok" ? "accept" : role === "probe" || streak > 1 ? "release" : "requeue";

/**
 * Longer than the client's own connection-recovery budget (about five
 * minutes), so a wake queue survives a reconnect and is collected only once
 * its replica is really gone. The queue only expires while it has no consumer.
 */
const WAKE_QUEUE_EXPIRES_MS = 600_000;

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
  const services = yield* Effect.context<HttpClient.HttpClient>();
  const runInContext = Effect.runPromiseWith(services);

  // The breaker's open state is a message in the delay chain addressed to this
  // replica, so the queue it comes back on belongs to this replica: named by
  // it, and collected by the broker when the replica is gone for good.
  const wakeQueue = `${cfg.apiId}.breaker.wake.${cfg.replicaId}`;
  yield* Delay.declare();
  yield* rmq.declareQueue(wakeQueue, { args: { "x-expires": WAKE_QUEUE_EXPIRES_MS } });
  yield* Delay.receive(wakeQueue);
  const wakes = yield* Queue.unbounded<number>();
  yield* rmq.consume(
    wakeQueue,
    (_body, delivery) => {
      Queue.offerUnsafe(wakes, Number(delivery.properties.attempt));
      return "accept";
    },
    { prefetch: 1 },
  );

  let inFlight = 0;
  const setInFlight = (delta: 1 | -1) =>
    Effect.suspend(() => Metric.update(Telemetry.inFlight, (inFlight += delta)));

  const callUpstream = (key: string): Promise<Upstream.CallOutcome> =>
    runInContext(
      setInFlight(1).pipe(
        Effect.andThen(Upstream.call(`${cfg.egressAddr}${cfg.apiPath}`, key)),
        Effect.ensuring(setInFlight(-1)),
      ),
    );

  const attempt = async (key: string, role: Role, report: Breaker.Report): Promise<Settlement> => {
    const outcome = await callUpstream(key);
    const streak = report(outcome === "ok");
    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome }), 1));
    return decide(outcome, role, streak);
  };

  // A body that declares a content type, encoding or message type this daemon
  // cannot read, does not decode, or carries no `message_id` to use as its
  // idempotency key, was never published by this fleet: discard
  // it rather than spend the delivery budget on something no retry can fix.
  //
  // Said out loud, because RabbitMQ's own guidance for a consumer handed a
  // delivery it cannot handle is to log it, and a publisher that starts sending
  // `gzip` by mistake would otherwise empty the queue into the dead-letter queue
  // without a trace. The counter carries the volume; the log carries what was
  // declared, at most once a second so a flood does not become the incident.
  let lastLoggedAt = 0;
  const discard = (reason: "format" | "malformed" | "keyless", delivery: DeliveryInfo): Promise<Settlement> => {
    runInContext(Metric.update(Metric.withAttributes(Telemetry.discarded, { reason }), 1));
    O.map(
      O.liftPredicate(Date.now(), (now) => now - lastLoggedAt >= 1000),
      (now) => {
        lastLoggedAt = now;
        const declared = (o: O.Option<string>) => O.getOrElse(o, () => "none");
        return runInContext(
          Effect.logWarning(
            `${cfg.apiId}/consumer: discarding a ${reason} delivery — message_id ${declared(delivery.messageId)}, ` +
              `type ${declared(delivery.type)}, content-type ${declared(delivery.contentType)}, ` +
              `content-encoding ${declared(delivery.contentEncoding)}`,
          ),
        );
      },
    );
    return Promise.resolve<Settlement>("discard");
  };

  const call = (
    body: string,
    delivery: DeliveryInfo,
    role: Role,
    report: Breaker.Report,
  ): Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("malformed", delivery),
          // The key is the message's own `message_id`, assigned once by the
          // producer: no id means no safe retry, so no call.
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("keyless", delivery),
              onSome: (key) => attempt(key, role, report),
            }),
        })
      : discard("format", delivery);

  const supervisor = Breaker.supervise(cfg.breaker, {
    // A probe is one message: prefetch 1 is the whole mechanism.
    subscribe: (role, report) =>
      rmq.consume(workQueue, (body, delivery) => call(body, delivery, role, report), {
        prefetch: role === "probe" ? 1 : cfg.maxInFlight,
      }),
    retire: rmq.drainConsumer,
    hold: (seconds, attempt) =>
      Delay.sendDelayed(wakeQueue, seconds, "wake", { headers: { attempt: String(attempt) } }).pipe(
        Effect.provideService(Rmq, rmq),
        Effect.andThen(Effect.log(`${cfg.apiId}/consumer: breaker open for ${seconds}s (attempt ${attempt})`)),
        Effect.andThen(Queue.take(wakes)),
      ),
    onPhase: (phase) =>
      Metric.update(Telemetry.breakerState, Breaker.PHASE_CODE[phase]).pipe(
        Effect.andThen(
          phase === "open" ? Metric.update(Telemetry.breakerTrips, 1) : Effect.void,
        ),
        Effect.andThen(Effect.log(`${cfg.apiId}/consumer: breaker ${phase}`)),
      ),
  });

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} ` +
      `wake=${wakeQueue} breaker=${cfg.breaker.consecutiveFailures}consecutive/${cfg.breaker.initialDelaySeconds}-${cfg.breaker.maxDelaySeconds}s`,
  );

  // Runs for the process's life: the phases repeat, and it ends only if the
  // broker fails an operation the breaker cannot do without.
  yield* supervisor;
});
