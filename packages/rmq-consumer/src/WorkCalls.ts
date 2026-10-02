import { Effect, Match, Metric, Option as O, Semaphore } from "effect";
import type { Context } from "effect";
import {
  ATTEMPTS_HEADER,
  decodeWorkMessage,
  ORIGIN_QUEUE_HEADER,
  ORIGIN_REASON_HEADER,
  readsWorkFormat,
  REDRIVE_COUNT_HEADER,
} from "@egress/rmq/ControlPlane.ts";
import type { DeliveryInfo, Publisher, RmqService, Settlement } from "@egress/rmq/Client.ts";
import * as Attempts from "./Attempts.ts";
import * as Limiter from "./Limiter.ts";
import * as Telemetry from "./Telemetry.ts";
import type { Counts } from "./Tally.ts";

/**
 * The idempotency key as the third party receives it. On the broker it is the
 * `message_id`, assigned once by the producer; every republish must pass
 * `messageId` forward or `send` stamps a new one.
 */
export const IDEMPOTENCY_KEY_HTTP_HEADER = "x-idempotency-key";

/**
 * The work path: one delivery, one call through Envoy, one settlement (the table
 * in docs/architecture.md). Nothing here reads the circuit — whether this daemon
 * consumes at all is the control path's decision, in daemon.ts.
 */
export type WorkCallsOptions = {
  readonly cfg: {
    readonly apiId: string;
    readonly instanceId: string;
    readonly egressAddr: string;
    readonly apiPath: string;
    readonly maxInFlight: number;
    readonly limit: O.Option<Limiter.LimiterConfig>;
  };
  readonly rmq: RmqService;
  readonly counts: Counts;
  readonly attrs: Record<string, string>;
  readonly queues: { readonly work: string; readonly dead: string };
  readonly publishers: { readonly work: Publisher; readonly dead: Publisher; readonly parked: Publisher };
  /** The daemon's context, so the callbacks' effects keep its tracer and logger. */
  readonly services: Context.Context<never>;
};

export const makeWorkCalls = Effect.fnUntraced(function* (options: WorkCallsOptions) {
  const { cfg, rmq: control, counts, attrs } = options;
  const { work: workQueue, dead: deadQueue } = options.queues;
  const { work: workPublisher, dead: deadPublisher, parked: parkedPublisher } = options.publishers;
  const runInContext = Effect.runPromiseWith(options.services);

  /** A gauge, not a limit: prefetch bounds it. */
  let inFlight = 0;

  /** Jitter before releasing a 429, so a fleet shed together does not return together. Not tuned. */
  const SHED_BACKOFF_MIN_MS = 100;
  const SHED_BACKOFF_MAX_MS = 400;

  const limit = O.map(cfg.limit, (c) => new Limiter.AdaptiveLimit(c));
  const initialSlots = O.match(limit, { onNone: () => cfg.maxInFlight, onSome: (l) => l.slots });
  const slots = Semaphore.makeUnsafe(initialSlots);
  yield* Metric.update(Metric.withAttributes(Telemetry.concurrencyLimit, attrs), initialSlots);
  const resize = (to: number) =>
    Semaphore.resize(slots, to).pipe(
      Effect.andThen(Metric.update(Metric.withAttributes(Telemetry.concurrencyLimit, attrs), to)),
    );

  /** The key is the delivery's `message_id`, carried by every redelivery, retry and redrive. */
  const fetchStatus = async (key: string): Promise<number | "error"> => {
    inFlight++;
    try {
      const res = await fetch(`${cfg.egressAddr}${cfg.apiPath}`, {
        signal: AbortSignal.timeout(2000),
        headers: { [IDEMPOTENCY_KEY_HTTP_HEADER]: key },
      });
      // An unconsumed body holds its connection out of the pool.
      await res.text().catch(() => {});
      return res.status;
    } catch {
      // The shape of an outage, not an error: the aggregator judges health from Envoy.
      return "error";
    } finally {
      inFlight--;
    }
  };

  /**
   * A delivery waiting for a slot stays unacked, so the limit only narrows what
   * prefetch allows. Moved once per round trip (Limiter.ts's `epoch`).
   */
  const limitedStatus = (key: string): Promise<number | "error"> =>
    O.match(limit, {
      onNone: () => fetchStatus(key),
      onSome: (l) =>
        runInContext(
          Semaphore.withPermit(
            slots,
            Effect.gen(function* () {
              const startedIn = l.epoch;
              const status = yield* Effect.promise(() => fetchStatus(key));
              const before = l.slots;
              yield* Effect.sync(() =>
                Match.value(Attempts.classify(status)).pipe(
                  Match.when("ok", () => l.succeeded()),
                  Match.when("shed", () => l.throttled(startedIn)),
                  Match.orElse(() => {}),
                ),
              );
              yield* Effect.when(resize(l.slots), Effect.sync(() => l.slots !== before));
              return status;
            }),
          ),
        ),
    });

  /**
   * Poison goes straight to the parked queue: through the dead-letter queue it came from the work queue, so the
   * redrive would replay it `MAX_REDRIVES` times for the same answer. `otherwise` is the settlement if the park fails.
   */
  const park = (body: string, messageId: O.Option<string>, reason: string, otherwise: Settlement): Promise<Settlement> =>
    runInContext(
      control.send(parkedPublisher, body, {
        messageId: O.getOrUndefined(messageId),
        headers: { [ORIGIN_QUEUE_HEADER]: workQueue, [ORIGIN_REASON_HEADER]: reason },
      }),
    ).then(
      (): Settlement => "accept",
      (): Settlement => otherwise,
    );

  const attempt = async (body: string, delivery: DeliveryInfo, key: string): Promise<Settlement> => {
    const status = await limitedStatus(key);
    const outcome = Attempts.classify(status);

    // Headers are only materialized when the call failed: the success path runs thousands of times a second.
    const decision = Attempts.nextAttempt(
      outcome,
      outcome === "failed" ? delivery.properties[ATTEMPTS_HEADER] : undefined,
    );
    return Match.valueTags(decision, {
      accept: async (): Promise<Settlement> => {
        counts.ok++;
        return "accept";
      },

      release: async (): Promise<Settlement> => {
        // Backpressure, not a failure. `release` does not spend x-delivery-limit;
        // `requeue` would, and a burst would dead-letter healthy work.
        counts.shed++;
        const jitter =
          SHED_BACKOFF_MIN_MS + Math.random() * (SHED_BACKOFF_MAX_MS - SHED_BACKOFF_MIN_MS);
        await new Promise((resolve) => setTimeout(resolve, jitter));
        return "release";
      },

      park: async (): Promise<Settlement> => {
        // Refused (4xx other than 408/429): a retry gets the same answer. Parked for a human.
        counts.refused++;
        return park(body, O.some(key), `refused-${status}`, "requeue");
      },

      republish: async (decision): Promise<Settlement> => {
        // Republished, never requeued: a requeue cannot carry the attempt count.
        counts.failed++;
        const toDead = decision.destination === "dead";
        const target = toDead ? deadPublisher : workPublisher;
        const destinationQueue = toDead ? deadQueue : workQueue;
        const headers: Record<string, string> = {
          [ATTEMPTS_HEADER]: String(decision.attempts),
          // Carried forward, or a redriven poison message would reset its count on every failure and never park.
          ...O.match(O.fromNullishOr(delivery.properties[REDRIVE_COUNT_HEADER]), {
            onNone: () => ({}),
            onSome: (count) => ({ [REDRIVE_COUNT_HEADER]: count }),
          }),
          // A direct publish has no x-first-death-*; the redrive attributes it by these.
          ...(toDead
            ? { [ORIGIN_QUEUE_HEADER]: workQueue, [ORIGIN_REASON_HEADER]: "attempts-exhausted" }
            : {}),
        };
        try {
          const send = control.send(target, body, { messageId: key, headers });
          await runInContext(
            O.map(delivery.parent, (span) =>
              send.pipe(
                Effect.withSpan("work.retry", {
                  attributes: {
                    "messaging.system": "rabbitmq",
                    "messaging.operation.name": "retry",
                    "messaging.destination.name": destinationQueue,
                    "egress.attempts": decision.attempts,
                  },
                }),
                Effect.withParentSpan(span),
              ),
            ).pipe(O.getOrElse(() => send)),
          );
        } catch {
          // x-delivery-limit is the backstop when a retry cannot be republished.
          return "requeue";
        }
        return "accept";
      },
    });
  };

  /** Never published by this fleet: parked unread, not retried or redriven. */
  const discard = (reason: "Format" | "Malformed" | "Keyless", body: string, delivery: DeliveryInfo): Promise<Settlement> => {
    counts[`discarded${reason}`]++;
    return park(body, delivery.messageId, `unreadable-${reason.toLowerCase()}`, "discard");
  };

  const call = (body: string, delivery: DeliveryInfo): Settlement | Promise<Settlement> =>
    readsWorkFormat(delivery)
      ? O.match(decodeWorkMessage(body), {
          onNone: () => discard("Malformed", body, delivery),
          onSome: () =>
            O.match(delivery.messageId, {
              onNone: () => discard("Keyless", body, delivery),
              onSome: (key) => attempt(body, delivery, key),
            }),
        })
      : discard("Format", body, delivery);

  /** Traced only when the message carried a parent; the common case stays a plain call. */
  const callEgress = (body: string, delivery: DeliveryInfo): Settlement | Promise<Settlement> =>
    O.map(delivery.parent, (span) =>
      runInContext(
        Effect.promise(async () => call(body, delivery)).pipe(
          Effect.tap((outcome) =>
            Effect.annotateCurrentSpan({ "egress.settlement": outcome }),
          ),
          Effect.withSpan("work.call", {
            attributes: {
              "egress.api_id": cfg.apiId,
              "egress.path": cfg.apiPath,
              "egress.daemon": cfg.instanceId,
            },
          }),
          Effect.withParentSpan(span),
        ),
      ),
    ).pipe(O.getOrElse(() => call(body, delivery)));


  return {
    /** The work consumer's handler. */
    callEgress,
    inFlight: () => inFlight,
  };
});
