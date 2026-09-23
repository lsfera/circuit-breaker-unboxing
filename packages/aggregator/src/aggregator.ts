import { Effect, Metric, Option as O, Ref } from "effect";
import { Rmq } from "@egress/rmq/Client.ts";
import { CONTROL_EXCHANGE, decodeControlEvent, readsControlFormat } from "@egress/rmq/ControlPlane.ts";
import type { DeliveryInfo } from "@egress/rmq/Client.ts";
import * as Telemetry from "./Telemetry.ts";
import * as Verdict from "./Verdict.ts";

/**
 * One process, one in-memory registry, no persistence. A restart starts from
 * "no data yet" and repopulates within a few replicas' worth of transitions
 * — an explicit trade for staying single-instance this branch, not an
 * oversight. See README.md's "what this still doesn't fix" for the SPOF
 * this is.
 */

export type AggregatorConfig = {
  /** Share of the known fleet reporting open/half_open before the published verdict itself opens. */
  readonly verdictThreshold: number;
  /** How long a replica may go unheard-from before its last vote stops counting. */
  readonly stalenessMs: number;
};

/** Every replica binds and publishes on this; a single-instance aggregator needs exactly one queue for all of it. */
const QUEUE = "circuit.control.aggregator";

export const runAggregator = Effect.fnUntraced(function* (cfg: AggregatorConfig) {
  const rmq = yield* Rmq;

  // Durable, matching consumer.ts's own declare — a redeclare whose
  // durability disagrees is a broker error (406 PRECONDITION-FAILED), so
  // whichever process starts second must ask for the same thing.
  yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, { durable: true });
  yield* rmq.declareQueue(QUEUE);
  // Every apiId this deployment ever runs — this process never hardcodes one.
  yield* rmq.bind("circuit.*", CONTROL_EXCHANGE, QUEUE);

  const registries = yield* Ref.make<ReadonlyMap<string, Verdict.ApiRegistry>>(new Map());
  const published = yield* Ref.make<ReadonlyMap<string, Verdict.Verdict>>(new Map());

  const onEvent = (event: Verdict.ReplicaEvent) =>
    Effect.gen(function* () {
      const now = Date.now();
      const perApi = yield* Ref.get(registries);
      const previous = perApi.get(event.apiId) ?? new Map();
      const pruned = Verdict.prune(previous, now, cfg.stalenessMs);

      // The registry's size is the fraction's denominator (ADR 009): an
      // instance dropped here changes what the same fraction *means* at the
      // same number, silently, unless something says so.
      for (const instance of previous.keys()) {
        if (!pruned.has(instance)) {
          yield* Effect.logWarning(
            `aggregator: ${event.apiId} dropped stale replica ${instance} (silent for > ${cfg.stalenessMs}ms)`,
          );
        }
      }

      const updated = Verdict.shouldAccept(pruned.get(event.instance), event)
        ? new Map(pruned).set(event.instance, { state: event.state, at: event.at })
        : pruned;
      if (updated === pruned) {
        yield* Effect.logDebug(
          `aggregator: ${event.apiId} dropped stale/reordered event from ${event.instance} (at=${event.at})`,
        );
      }
      yield* Ref.update(registries, (m) => new Map(m).set(event.apiId, updated));

      const fraction = Verdict.openFraction(updated);
      const verdict = Verdict.verdictFor(fraction, cfg.verdictThreshold);

      yield* Metric.update(Metric.withAttributes(Telemetry.openFraction, { apiId: event.apiId }), fraction);
      yield* Metric.update(
        Metric.withAttributes(Telemetry.knownReplicas, { apiId: event.apiId }),
        updated.size,
      );
      yield* Metric.update(
        Metric.withAttributes(Telemetry.verdictState, { apiId: event.apiId }),
        verdict === "open" ? 1 : 0,
      );

      const prev = yield* Ref.get(published);
      if (prev.get(event.apiId) !== verdict) {
        yield* Ref.update(published, (m) => new Map(m).set(event.apiId, verdict));
        yield* Effect.log(
          `aggregator: ${event.apiId} fleet verdict -> ${verdict} ` +
            `(${updated.size} known, ${Math.round(fraction * 100)}% open/half-open)`,
        );
      }
    });

  // Bridges amqplib's plain-callback world into this process's services —
  // same reasoning as @egress/consumer's identical capture in consumer.ts.
  const services = yield* Effect.context<never>();
  const runInContext = Effect.runPromiseWith(services);

  // Accepted, not dead-lettered: nothing redelivers a transition, and the replica's next one supersedes it.
  const decline = (reason: "format" | "malformed", delivery: DeliveryInfo) =>
    runInContext(
      Effect.logWarning(
        `aggregator: dropping a ${reason} circuit.control event — type ${O.getOrElse(delivery.type, () => "none")}, ` +
          `content-type ${O.getOrElse(delivery.contentType, () => "none")}`,
      ),
    );

  yield* rmq.consume(
    QUEUE,
    (body, delivery) =>
      readsControlFormat(delivery)
        ? O.match(decodeControlEvent(body), {
            onNone: () => decline("malformed", delivery),
            onSome: (event) => runInContext(onEvent(event)),
          })
        : decline("format", delivery),
    { prefetch: 50 },
  );

  yield* Effect.log(
    `aggregator: up — exchange=${CONTROL_EXCHANGE} threshold=${cfg.verdictThreshold} staleness=${cfg.stalenessMs}ms`,
  );
});
