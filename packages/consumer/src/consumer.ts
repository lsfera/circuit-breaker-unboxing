import { Effect, Match, Metric, Option as O, Ref } from "effect";
import { setTimeout as sleep } from "node:timers/promises";
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
} from "@egress/rmq/ControlPlane.ts";
import * as Breaker from "./Breaker.ts";
import * as Redrive from "./Redrive.ts";
import * as Telemetry from "./Telemetry.ts";
import * as Upstream from "./Upstream.ts";
import { CircuitState } from "cockatiel";
import type { DeliveryInfo, Settlement } from "@egress/rmq/Client.ts";

/**
 * One competing-consumer daemon with its own in-process circuit breaker, plus the fleet-wide probe permit
 * (both Breaker.ts). Its breaker's decision stays entirely private to this process.
 *
 * A second, unrelated concern on top: `<api>.work.dead` used to be a one-way trip. RabbitMQ's
 * `x-single-active-consumer` elects exactly one replica per API to redrive it — see Redrive.ts — gated on
 * that one elected replica's own breaker, the same local-view tradeoff the probe permit already made. A pass
 * starts either on a transition into Closed or on a fixed clock (`REDRIVE_SWEEP_MS`), since a message can
 * dead-letter without any transition happening at all.
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

/**
 * A total function of the one thing that matters, testable with no broker, breaker or fetch. `"open"` means no
 * call was attempted, so it releases: RabbitMQ 4.3 doesn't count a release toward `x-delivery-limit`, and three
 * redeliveries onto open breakers would otherwise dead-letter work the third party never saw. `"failed"`
 * requeues, spending the budget on a real call. `"client_error"` skips straight to the dead-letter queue: a
 * retry gets the same answer.
 */
export type CallOutcome = Breaker.CallOutcome | "open";
export const decide = (outcome: CallOutcome): Settlement =>
  Match.value(outcome).pipe(
    Match.when("ok", (): Settlement => "accept"),
    Match.when("client_error", (): Settlement => "discard"),
    Match.when("failed", (): Settlement => "requeue"),
    Match.when("open", (): Settlement => "release"),
    Match.exhaustive,
  );

/** What one attempt came to. `status` is `none` when no call reached the third party. */
type Attempt = { readonly outcome: CallOutcome; readonly status: string };
const TURNED_AWAY: Attempt = { outcome: "open", status: "none" };

/**
 * Held before releasing a breaker-open rejection, so it doesn't spin straight back to this same consumer at
 * whatever rate the broker redelivers. Jittered so breakers that open together don't release in lockstep.
 */
const OPEN_REQUEUE_DELAY_MIN_MS = 100;
const OPEN_REQUEUE_DELAY_MAX_MS = 400;

/** How often the elected replica re-triggers a redrive pass even without a fresh breaker transition — see the sweep's own comment in `runConsumer`. */
const REDRIVE_SWEEP_MS = 30_000;

export const runConsumer = Effect.fnUntraced(function* (cfg: ConsumerConfig) {
  const rmq = yield* Rmq;
  const workQueue = workQueueFor(cfg.apiId);
  const deadQueue = deadLetterQueueFor(cfg.apiId);

  yield* rmq.declareQueue(deadQueue, deadLetterQueueOptions());
  yield* rmq.declareQueue(workQueue, workQueueOptions(cfg.apiId));

  // Captured so the plain-async handler below (amqplib's callback, not an Effect fiber) can still reach these
  // services — `Rmq` is needed for `Breaker.withPermit`'s own `get`/`nack`.
  const services = yield* Effect.context<HttpClient.HttpClient | Rmq>();
  const runInContext = Effect.runPromiseWith(services);

  // One breaker per process, shared across every message: a fresh one per call would never accumulate a failure count.
  const breaker = Breaker.make(cfg.breaker);

  // Every replica seeds the same permit queue; RabbitMQ's own x-max-length/x-overflow keeps exactly one token
  // regardless of how many replicas race this on startup — see Breaker.ts's module doc.
  yield* Breaker.seedPermit(cfg.apiId);

  // Recovering `<api>.work.dead`. `parkedQueue` needs declaring even on the four replicas that will never
  // redrive into it — every process that might touch a queue has to agree on its arguments.
  yield* rmq.declareQueue(parkedQueueFor(cfg.apiId), parkedQueueOptions());
  const redriveQueue = redriveTriggerQueueFor(cfg.apiId);
  yield* rmq.declareQueue(redriveQueue, redriveTriggerQueueOptions());
  const redriveTriggerPub = yield* rmq.publisherToQueue(redriveQueue);

  const triggerRedrive = () =>
    runInContext(rmq.send(redriveTriggerPub, "redrive")).catch((err: unknown) => {
      runInContext(
        Effect.logWarning(`${cfg.apiId}/consumer: redrive trigger publish failed`, err),
      ).catch(() => {});
    });

  // Guards against two triggers arriving close together starting two overlapping passes on whichever replica
  // the broker has elected active — `Ref.modify` reads and marks the claim in one synchronous step.
  const redriving = yield* Ref.make(false);

  // Never more than one bound consumer here actually receives anything: `redriveTriggerQueueOptions`'s
  // `x-single-active-consumer` is the whole election, promoted automatically if the active replica
  // disconnects — the same broker guarantee the probe permit already leans on.
  yield* rmq.consume(redriveQueue, () => {
    runInContext(
      Ref.modify(redriving, (running) => [running, true] as const).pipe(
        Effect.flatMap((alreadyRunning) =>
          alreadyRunning
            ? Effect.void
            : Redrive.runPass({
                apiId: cfg.apiId,
                // The elected replica's own view — the same tradeoff the probe permit already made, not a
                // new one. See README.md's "what this still doesn't fix."
                isClosed: Effect.sync(() => breaker.state === CircuitState.Closed),
                onOutcome: (outcome) => {
                  runInContext(
                    Metric.update(Metric.withAttributes(Telemetry.redrives, { outcome }), 1),
                  );
                },
              }).pipe(Effect.ensuring(Ref.set(redriving, false))),
        ),
      ),
    ).catch((err: unknown) => {
      runInContext(Ref.set(redriving, false));
      runInContext(Effect.logWarning(`${cfg.apiId}/consumer: redrive pass failed`, err)).catch(
        () => {},
      );
    });
    return "accept";
  });

  breaker.onStateChange((state: CircuitState) => {
    runInContext(Metric.update(Telemetry.breakerState, Breaker.STATE_CODE[state]));
  });
  breaker.onBreak(() => {
    runInContext(Metric.update(Telemetry.breakerTrips, 1));
    runInContext(Effect.log(`${cfg.apiId}/consumer: breaker opened`));
  });
  breaker.onReset(() => {
    runInContext(Effect.log(`${cfg.apiId}/consumer: breaker closed`));
    // The moment this replica's own breaker closes is the moment "the outage might be over" first becomes
    // true for it — worth a trigger even though only the elected replica will ever act on it.
    triggerRedrive();
  });

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

  // Rate-limited: a misbehaving publisher or third party would otherwise fill the log at message rate. The
  // counters carry the volume.
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

  const attempt = async (key: string): Promise<Settlement> => {
    // Checked before execute() rather than inside the wrapped function: a race with cockatiel's own
    // Closed/Open/HalfOpen decision is possible but harmless, since only HalfOpen changes which function runs.
    const attemptUpstream =
      breaker.state === CircuitState.HalfOpen
        ? () => runInContext(Breaker.withPermit(cfg.apiId, () => callUpstream(key)))
        : () => callUpstream(key);

    const { outcome, status } = await breaker.execute(attemptUpstream).then(
      (answer): Attempt => ({ outcome: Breaker.classify(answer), status: String(answer) }),
      // isBrokenCircuitError (breaker open) and NoPermit (lost the permit race) both mean no call was
      // attempted — same "open" telemetry. Anything else threw, which counts as failed.
      (err): Attempt =>
        Breaker.isBrokenCircuitError(err) || err instanceof Breaker.NoPermit
          ? TURNED_AWAY
          : { outcome: "failed", status: "error" },
    );

    runInContext(Metric.update(Metric.withAttributes(Telemetry.calls, { outcome, status }), 1));
    await Match.value(outcome).pipe(
      Match.when("open", () => sleep(OPEN_REQUEUE_DELAY_MIN_MS + Math.random() * (OPEN_REQUEUE_DELAY_MAX_MS - OPEN_REQUEUE_DELAY_MIN_MS))),
      Match.when("client_error", () =>
        warnAtMostOncePerSecond(() => `${cfg.apiId}/consumer: third party refused message_id ${key} with ${status}, dead-lettering it`),
      ),
      Match.orElse(() => Promise.resolve()),
    );
    return decide(outcome);
  };

  // A delivery in a format this daemon can't read, that doesn't decode, or with no message_id to use as its
  // idempotency key was never published by this fleet — discard rather than spend a retry no fix helps.
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

  const call = (body: string, delivery: DeliveryInfo): Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("malformed", delivery),
          // The key is the message's own `message_id`, assigned once by the
          // producer: no id means no safe retry, so no call.
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("keyless", delivery),
              onSome: attempt,
            }),
        })
      : discard("format", delivery);

  yield* rmq.consume(workQueue, (body, delivery) => call(body, delivery), {
    prefetch: cfg.maxInFlight,
  });

  // Set at startup so the series exists before the first state change.
  yield* Metric.update(Telemetry.breakerState, Breaker.INITIAL_STATE_CODE);

  // A backlog already sitting in the dead-letter queue when this replica starts — from a redrive-eligible
  // outage that ended before any restart — would otherwise wait for a fresh breaker trip and reset.
  triggerRedrive();

  // A message can dead-letter while this replica's breaker is already Closed (a redelivery exhausting
  // `x-delivery-limit` needs no breaker transition at all), and `onReset`/startup above only trigger on one.
  // A clock-driven sweep, independent of transitions, is what closes that gap.
  setInterval(() => {
    if (breaker.state === CircuitState.Closed) triggerRedrive();
  }, REDRIVE_SWEEP_MS).unref();

  yield* Effect.log(
    `${cfg.apiId}/consumer: up — maxInFlight=${cfg.maxInFlight} egress=${cfg.egressAddr}${cfg.apiPath} work=${workQueue} ` +
      `breaker=${cfg.breaker.consecutiveFailures}consecutive/${cfg.breaker.initialDelayMs}-${cfg.breaker.maxDelayMs}ms`,
  );
});
