import { PositiveInt, setting } from "@egress/config/Settings.ts";
import { mediaTypes, Payment, Refund } from "@egress/contracts/Work.ts";
import * as Producer from "@egress/rmq-producer";
import { run } from "@egress/rmq-producer/node";
import { Config, Duration, Effect, Schedule } from "effect";
import { Flag } from "effect/cli";

/**
 * The producer (`node src/main.ts`), the load half of the scenario: a steady stream onto its contract's exchange. Fixed rate,
 * and it never reacts to the third party: arrivals do not stop when it degrades, and a producer that backed off would
 * hide the backlog the fleet has to survive. It shares the contract with the consumer and nothing else.
 */

/**
 * Which contract a producer publishes, named for the consumer that reads it: `payments-provider` publishes payments,
 * `refunds-provider` refunds. Where they go is the contract's exchange; which queue holds them, the consumer's binding.
 */
const contracts = { "payments-provider": Payment, "refunds-provider": Refund } as const;
const apiIds = Object.keys(contracts) as Array<keyof typeof contracts>;

const flags = {
  apiId: Flag.Literals("api-id", apiIds).pipe(
    Flag.withFallbackConfig(Config.Literals(apiIds, "API_ID")),
    Flag.withDefault("payments-provider"),
    Flag.withDescription("Which work to publish: payments-provider publishes payments, refunds-provider refunds")
  ),
  /** Fixed, and deliberately never lowered in reaction to the third party: the backlog an outage builds is what the fleet has to survive. */
  ratePerSecond: setting(Flag.Int("rate"), PositiveInt, "RATE_PER_SECOND").pipe(
    Flag.withDefault(200),
    Flag.withDescription("Messages published per second, regardless of circuit state")
  ),
  format: Flag.Literals("format", ["json", "protobuf"]).pipe(
    Flag.withFallbackConfig(Config.Literals(["json", "protobuf"], "WORK_FORMAT")),
    Flag.withDefault("json"),
    Flag.withDescription(
      "How each body is written: JSON, or protobuf (`message Work { string api_id = 1; int64 n = 2; }`)"
    )
  )
};

const produce = Effect.fnUntraced(function*(settings: {
  readonly apiId: keyof typeof contracts;
  readonly ratePerSecond: number;
  readonly format: keyof typeof mediaTypes;
}) {
  const { apiId, ratePerSecond, format } = settings;
  const work = yield* Producer.publisher(contracts[apiId], { format: mediaTypes[format] });

  // One batch per 100ms rather than a timer per message; nothing downstream can tell the difference.
  const perTick = Math.max(1, Math.round(ratePerSecond / 10));
  const TICK = Duration.millis(100);
  let sent = 0;
  // A failed batch is skipped, not fatal: one nack or a publish channel closing under an unconfirmed batch would
  // otherwise end the loop and the process. Logged on the edges only; a lost broker still ends the process.
  let failing = false;

  yield* Effect.log(`${apiId}/producer: up — ${ratePerSecond}/s of ${format} onto ${work.exchange.name}`);

  yield* Effect.gen(function*() {
    const batch = Array.from({ length: perTick }, () => ({ apiId, n: sent++ }));
    const published = yield* work.publish(Producer.batch(batch)).pipe(
      Effect.as(true),
      Effect.catch((error) =>
        failing
          ? Effect.succeed(false)
          : Effect.as(Effect.logWarning(`${apiId}/producer: publishing failed, still trying — ${error.message}`), false)
      )
    );
    yield* published && failing ? Effect.log(`${apiId}/producer: publishing again`) : Effect.void;
    failing = !published;
    // Publish rate is read from RabbitMQ's own metrics and the SDK's counter, not logged per batch.
    yield* Effect.when(
      Effect.log(`${apiId}/producer: ${sent} messages published`),
      Effect.sync(() => sent % (ratePerSecond * 10) < perTick)
    );
    // `fixed`, not `spaced`: spaced waits after each batch, so the period becomes 100ms plus the broker's confirm
    // time and the producer would quietly back off exactly when the queue is deepest. `fixed` keeps the cadence and
    // skips a tick that overruns.
  }).pipe(Effect.repeat(Schedule.fixed(TICK)));
});

/** Failing setup (a broker that never comes up, a queue redeclared with different arguments) ends the process, for the container's restart policy. */
run({ flags, main: produce }, { name: "rmq-producer" });
