import { Context, Duration, Effect, Layer, Metric, Ref, Schedule } from "effect";
import * as Breaker from "./domain/Breaker.ts";
import { Config } from "./domain/Model.ts";
import { EventBus, EventSink, snapshotEvent, stateChanged } from "./Events.ts";
import { FleetSource } from "./FleetSource.ts";
import * as Telemetry from "./Telemetry.ts";
import type { ApiSnapshot, CircuitEvent, State } from "./domain/Model.ts";

type Registry = {
  readonly breakers: ReadonlyMap<string, Breaker.BreakerState>;
  readonly lastSnapshotAt: ReadonlyMap<string, number>;
};

export class Aggregator extends Context.Service<
  Aggregator,
  {
    /** One pass: poll the fleet, advance every breaker, publish what changed. */
    readonly tick: Effect.Effect<ReadonlyArray<CircuitEvent>>;
    readonly snapshots: Effect.Effect<ReadonlyArray<ApiSnapshot>>;
    readonly stateOf: (apiId: string) => Effect.Effect<State | null>;
    /**
     * The tick loop. Runs until interrupted — the caller decides how to fork
     * it, so its lifetime is tied to a scope the caller owns.
     */
    readonly run: Effect.Effect<void>;
  }
>()("Aggregator") {}

export const AggregatorLayer = Layer.effect(
  Aggregator,
  Effect.gen(function* () {
    const cfg = yield* Config;
    const fleet = yield* FleetSource;
    const bus = yield* EventBus;
    const sink = yield* EventSink;

    const registry = yield* Ref.make<Registry>({
      breakers: new Map(),
      lastSnapshotAt: new Map(),
    });

    const tick: Effect.Effect<ReadonlyArray<CircuitEvent>> = Effect.gen(
      function* () {
        const [pollDuration, reports] = yield* Effect.timed(fleet.poll);
        yield* Metric.update(Telemetry.fleetPollDuration, pollDuration);
        const now = yield* Effect.clockWith((c) => c.currentTimeMillis);

        const events = yield* Ref.modify(registry, (reg) => {
          const breakers = new Map(reg.breakers);
          const lastSnapshotAt = new Map(reg.lastSnapshotAt);

          for (const report of reports) {
            const current =
              breakers.get(report.apiId) ??
              Breaker.initial(report.apiId, cfg, now);
            breakers.set(report.apiId, Breaker.ingest(current, report));
          }

          const out: CircuitEvent[] = [];
          for (const [apiId, before] of breakers) {
            const [after, change] = Breaker.step(before, now, cfg);
            breakers.set(apiId, after);

            if (change) {
              out.push(stateChanged(Breaker.snapshot(after), change.from));
              lastSnapshotAt.set(apiId, now);
              continue;
            }
            const last = lastSnapshotAt.get(apiId) ?? 0;
            if (now - last >= cfg.snapshotMs) {
              lastSnapshotAt.set(apiId, now);
              out.push(snapshotEvent(Breaker.snapshot(after)));
            }
          }
          return [out as ReadonlyArray<CircuitEvent>, { breakers, lastSnapshotAt }];
        });

        // Gauges reflect the fleet's current view every tick, whether or not
        // anything published — a dashboard watching mid-dwell should not look
        // frozen just because no event crossed the publish threshold yet.
        yield* Ref.get(registry).pipe(
          Effect.flatMap((reg) =>
            Effect.forEach(
              [...reg.breakers.values()].map(Breaker.snapshot),
              (snap) =>
                Effect.all(
                  [
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitState, { apiId: snap.apiId }),
                      Telemetry.STATE_CODE[snap.state],
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitHealthyEndpoints, {
                        apiId: snap.apiId,
                      }),
                      snap.healthyEndpoints,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitTotalEndpoints, {
                        apiId: snap.apiId,
                      }),
                      snap.totalEndpoints,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitReportingReplicas, {
                        apiId: snap.apiId,
                      }),
                      snap.reportingReplicas,
                    ),
                    Metric.update(
                      Metric.withAttributes(Telemetry.circuitSequence, { apiId: snap.apiId }),
                      snap.sequence,
                    ),
                  ],
                  { discard: true },
                ),
              { discard: true },
            ),
          ),
        );

        for (const e of events) {
          if (e.type === "egress.circuit.state_changed") {
            yield* Metric.update(
              Metric.withAttributes(Telemetry.circuitTransitions, {
                apiId: e.data.apiId,
                reason: e.data.reason,
                state: e.data.state,
              }),
              1,
            );
          } else {
            yield* Metric.update(
              Metric.withAttributes(Telemetry.circuitSnapshots, { apiId: e.data.apiId }),
              1,
            );
          }
        }

        // Publish to the in-process bus first (the console), then hand to the
        // sink, which forks delivery so a slow subscriber cannot stall the loop.
        yield* Effect.forEach(events, (e) => bus.publish(e), {
          discard: true,
        });
        yield* Effect.forEach(events, (e) => sink.deliver(e), {
          discard: true,
        });
        return events;
      },
    );

    const snapshots = Ref.get(registry).pipe(
      Effect.map((reg) =>
        [...reg.breakers.values()]
          .map(Breaker.snapshot)
          .sort((a, b) => a.apiId.localeCompare(b.apiId)),
      ),
    );

    const stateOf = (apiId: string) =>
      Ref.get(registry).pipe(
        Effect.map((reg) => reg.breakers.get(apiId)?.state ?? null),
      );

    // The loop is a Schedule, not a setInterval. That is what lets TestClock
    // drive thousands of simulated seconds instantly and deterministically,
    // and what makes the loop interruptible as a value rather than via a
    // clearInterval handle someone has to remember to call.
    const run = tick.pipe(
      Effect.repeat(Schedule.spaced(Duration.millis(cfg.tickMs))),
      Effect.asVoid,
    );

    return { tick, snapshots, stateOf, run };
  }),
);
