import { Effect, Metric, Option as O, Queue, Ref, Schedule } from "effect";
import type { HttpClient } from "effect/unstable/http";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  deadLetterQueueFor,
  deadLetterQueueOptions,
  decodeWorkMessage,
  parkedQueueFor,
  parkedQueueOptions,
  readsWorkFormat,
  redriveTriggerQueueFor,
  redriveTriggerQueueOptions,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/WorkQueue.ts";
import * as Delay from "@egress/rmq/DelayedDelivery.ts";
import * as Breaker from "./Breaker.ts";
import * as Permit from "./Permit.ts";
import * as Redrive from "./Redrive.ts";
import * as Telemetry from "./Telemetry.ts";
import * as Upstream from "./Upstream.ts";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * One competing-consumer daemon behind a circuit breaker that lives in the broker (see Breaker.ts): tripping
 * cancels this replica's consumer and a message through the delay chain brings it back. It still knows nothing
 * about the other daemons: five replicas are five breakers, each with its own token; an open replica is simply
 * not consuming. Two things are shared through the broker: the one probe permit (Permit.ts), so the fleet
 * probes one call at a time, and the redrive of `<api>.work.dead`, run by whichever replica RabbitMQ elects on
 * the redrive-trigger queue (Redrive.ts).
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
 * Whether a call outcome is accepted, handed back to the broker or dead-lettered: a total function of what
 * matters, so it is testable without a broker, breaker or fetch. `client_error` skips the delivery budget and
 * the release/requeue split below entirely: it is discarded at once, on this replica or the next, because a
 * retry gets the same answer.
 *
 * A `failed` is charged to the message (`requeue` counts toward the queue's delivery budget) only when it stands
 * alone. One that follows another failure on the same replica (`streak` above 1) or is a probe is evidence about
 * the third party, not the message, and is `release`d with no strike: the breaker needs `consecutiveFailures`
 * calls to open and its consumer takes a round trip to stop, and in that window the same few messages are
 * redelivered again and again, so charging them would dead-letter healthy messages. A message that fails between
 * successes (a poison message on a healthy third party) is still charged, and still parked.
 */
export type Role = "work" | "probe";
export const decide = (outcome: Breaker.CallOutcome, role: Role = "work", streak = 1): Settlement =>
  outcome === "ok"
    ? "accept"
    : outcome === "client_error"
      ? "discard"
      : role === "probe" || streak > 1
        ? "release"
        : "requeue";

/**
 * Longer than the client's connection-recovery budget (about five minutes), so a wake queue survives a reconnect
 * and is collected only once its replica is really gone. The queue only expires while it has no consumer.
 */
const WAKE_QUEUE_EXPIRES_MS = 600_000;

/** A clock-driven redrive trigger: a message can dead-letter while every breaker stays closed. */
const REDRIVE_SWEEP = "30 seconds";

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handlers below (amqplib's callbacks, not Effect fibers) can still reach this
  // process's services: metrics, and the broker for the permit and the redrive.
  const services = yield* Effect.context<HttpClient.HttpClient | Rmq>();
  const runInContext = Effect.runPromiseWith(services);

  // The open state is a message addressed to this replica, so the queue it comes back on belongs to it: named
  // by it, and collected by the broker when the replica is gone for good.
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

  const phase = yield* Ref.make<Breaker.Phase>("closed");
  const isClosed = Effect.map(Ref.get(phase), (p) => p === "closed");

  yield* Permit.seed(cfg.apiId);

  // Every replica declares the parked queue, even those never elected: every process that might touch a
  // queue has to agree on its arguments.
  yield* rmq.declareQueue(parkedQueueFor(cfg.apiId), parkedQueueOptions());
  const redriveQueue = redriveTriggerQueueFor(cfg.apiId);
  yield* rmq.declareQueue(redriveQueue, redriveTriggerQueueOptions());
  const redriveTriggerPub = yield* rmq.publisherToQueue(redriveQueue);
  const triggerRedrive = rmq.send(redriveTriggerPub, "redrive").pipe(
    Effect.catch((err) => Effect.logWarning(`${cfg.apiId}/consumer: redrive trigger publish failed`, err)),
  );

  // Two triggers close together must not start overlapping passes; one that arrives mid-pass is dropped.
  const redriving = yield* Ref.make(false);
  const redrivePass = Ref.modify(redriving, (running) => [running, true] as const).pipe(
    Effect.flatMap((running) =>
      running
        ? Effect.void
        : Redrive.runPass({
            apiId: cfg.apiId,
            // This replica's own view, not the fleet's: see README.md's "what this still doesn't fix".
            isClosed,
            onOutcome: (outcome) =>
              void runInContext(Metric.update(Metric.withAttributes(Telemetry.redrives, { outcome }), 1)),
          }).pipe(Effect.ensuring(Ref.set(redriving, false))),
    ),
    Effect.catch((err) => Effect.logWarning(`${cfg.apiId}/consumer: redrive pass failed`, err)),
  );
  // Only the consumer RabbitMQ has made active (`x-single-active-consumer`) receives anything here.
  yield* rmq.consume(redriveQueue, () => {
    Effect.runForkWith(services)(redrivePass);
    return "accept";
  });
  yield* Effect.forkChild(
    Effect.repeat(Effect.when(triggerRedrive, isClosed), Schedule.spaced(REDRIVE_SWEEP)),
  );

  let inFlight = 0;
  const setInFlight = (delta: 1 | -1) =>
    Effect.suspend(() => Metric.update(Telemetry.inFlight, (inFlight += delta)));

  const callUpstream = (key: string): Promise<Upstream.CallStatus> =>
    runInContext(
      setInFlight(1).pipe(
        Effect.andThen(Upstream.call(`${cfg.egressAddr}${cfg.apiPath}`, key)),
        Effect.ensuring(setInFlight(-1)),
      ),
    );

  // Logged at most once a second: a publisher that starts sending `gzip` by mistake, or a third party that
  // starts refusing every request, would otherwise empty the queue into the dead-letter queue without a trace.
  // The counters carry the volume.
  let lastLoggedAt = 0;
  const warnAtMostOncePerSecond = (message: () => string): void => {
    O.map(
      O.liftPredicate(Date.now(), (now) => now - lastLoggedAt >= 1000),
      (now) => {
        lastLoggedAt = now;
        return runInContext(Effect.logWarning(message()));
      },
    );
  };

  const permitLost = (): Settlement => {
    runInContext(Metric.update(Telemetry.permitLost, 1));
    return "release";
  };

  // The permit is held for the call alone, not while the probe consumer waits for a message.
  const probeCall = (key: string, verdict: (v: Breaker.ProbeVerdict) => void): Promise<Settlement> =>
    runInContext(Permit.take(cfg.apiId).pipe(Effect.orElseSucceed(() => O.none<Effect.Effect<void>>()))).then(
      O.match({
        onNone: () => {
          verdict("no-permit");
          return permitLost();
        },
        onSome: (giveBack) =>
          attempt(key, "probe", (ok) => {
            verdict(ok ? "ok" : "failed");
            return ok ? 0 : 1;
          }).finally(() => runInContext(giveBack)),
      }),
    );

  const attempt = async (key: string, role: Role, report: Breaker.Report): Promise<Settlement> => {
    const status = await callUpstream(key);
    const outcome = Breaker.classify(status);
    const streak = report(outcome !== "failed");
    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome, status: String(status) }), 1));
    if (outcome === "client_error") {
      warnAtMostOncePerSecond(
        () => `${cfg.apiId}/consumer: third party refused message_id ${key} with ${status}, dead-lettering it`,
      );
    }
    return decide(outcome, role, streak);
  };

  // A body that declares a content type, encoding or message type this daemon cannot read, does not decode, or
  // carries no `message_id` to use as its idempotency key was never published by this fleet: discard it rather
  // than spend the delivery budget on something no retry can fix.
  const discard = (reason: "format" | "malformed" | "keyless", delivery: DeliveryInfo): Promise<Settlement> => {
    runInContext(Metric.update(Metric.withAttributes(Telemetry.discarded, { reason }), 1));
    warnAtMostOncePerSecond(() => {
      const declared = (o: O.Option<string>) => O.getOrElse(o, () => "none");
      return (
        `${cfg.apiId}/consumer: discarding a ${reason} delivery — message_id ${declared(delivery.messageId)}, ` +
        `type ${declared(delivery.type)}, content-type ${declared(delivery.contentType)}, ` +
        `content-encoding ${declared(delivery.contentEncoding)}`
      );
    });
    return Promise.resolve<Settlement>("discard");
  };

  const call = (body: string, delivery: DeliveryInfo, run: (key: string) => Promise<Settlement>): Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("malformed", delivery),
          // The key is the message's own `message_id`, assigned once by the
          // producer: no id means no safe retry, so no call.
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("keyless", delivery),
              onSome: run,
            }),
        })
      : discard("format", delivery);

  const supervisor = Breaker.supervise(cfg.breaker, {
    subscribe: (report) =>
      rmq.consume(workQueue, (body, delivery) => call(body, delivery, (key) => attempt(key, "work", report)), {
        prefetch: cfg.maxInFlight,
      }),
    // A probe is one message: prefetch 1 is the whole mechanism.
    probe: (verdict) =>
      rmq.consume(workQueue, (body, delivery) => call(body, delivery, (key) => probeCall(key, verdict)), {
        prefetch: 1,
      }),
    retire: rmq.drainConsumer,
    hold: (seconds, attempt) =>
      Delay.sendDelayed(wakeQueue, seconds, "wake", { headers: { attempt: String(attempt) } }).pipe(
        Effect.provideService(Rmq, rmq),
        Effect.andThen(Effect.log(`${cfg.apiId}/consumer: breaker open for ${seconds}s (attempt ${attempt})`)),
        Effect.andThen(Queue.take(wakes)),
      ),
    onPhase: (next) =>
      Ref.set(phase, next).pipe(
        Effect.andThen(Metric.update(Telemetry.breakerState, Breaker.PHASE_CODE[next])),
        Effect.andThen(next === "open" ? Metric.update(Telemetry.breakerTrips, 1) : Effect.void),
        Effect.andThen(Effect.log(`${cfg.apiId}/consumer: breaker ${next}`)),
        // Closing (startup included) is when "the outage may be over" first becomes true here.
        Effect.andThen(next === "closed" ? triggerRedrive : Effect.void),
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
