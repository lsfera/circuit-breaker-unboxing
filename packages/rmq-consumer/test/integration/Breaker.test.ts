import { Rmq } from "@egress/rmq/Client.ts";
import { deadLetterQueueFor, deadLetterQueueOptions, workQueueFor, workQueueOptions } from "@egress/rmq/WorkQueue.ts";
import { Effect, Option as O, Result, Schema } from "effect";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { broker, skipIfNoDocker, startBroker, stopBroker, waitFor } from "../../../rmq/test/integration/harness.ts";
import { runApplication } from "../../src/consumer.ts";
import * as Dependency from "../../src/Dependency.ts";
import * as Negotiation from "../../src/Negotiation.ts";

before(startBroker);
after(stopBroker);

const run = <A>(program: Effect.Effect<A, unknown, Rmq>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<A>
  );

test("a failed call opens the real consumer breaker until its delayed probe succeeds", async (t) => {
  if (skipIfNoDocker(t)) return;

  const key = "breaker-probe";
  const attempts: Array<{ body: string; deliveryCount: number; at: number; }> = [];
  const completed: string[] = [];
  const upstream = Dependency.make("upstream", {
    classify: Result.match({
      onSuccess: (status: number) =>
        status >= 500
          ? { outcome: "failed" as const, reason: String(status) }
          : { outcome: "ok" as const, reason: String(status) },
      onFailure: (error) => ({ outcome: "failed" as const, reason: String(error) })
    })
  });

  const app = {
    name: "breaker-integration",
    maxInFlight: 1,
    replicaId: "test",
    breaker: { consecutiveFailures: 1, initialDelaySeconds: 1, maxDelaySeconds: 1 },
    limit: O.none(),
    consumers: [{
      key,
      negotiate: Negotiation.accept({ "text/plain": Negotiation.text(Schema.String) }),
      decode: O.some,
      action: (payload: unknown, delivery: { readonly deliveryCount: number; }) =>
        upstream(
          Effect.sync(() => {
            attempts.push({
              body: String(payload),
              deliveryCount: delivery.deliveryCount,
              at: Date.now()
            });
            return attempts.length === 1 ? 503 : 200;
          })
        ).pipe(Effect.tap(() => Effect.sync(() => void completed.push(String(payload))))),
      dependencies: [upstream]
    }]
  };

  const observed = await run(
    Effect.gen(function*() {
      const rmq = yield* Rmq;
      yield* rmq.declareQueue(deadLetterQueueFor(key), deadLetterQueueOptions());
      yield* rmq.declareQueue(workQueueFor(key), workQueueOptions(key));
      const pub = yield* rmq.publisherToQueue(workQueueFor(key), { contentType: "text/plain" });
      yield* rmq.send(pub, "probe-me");
      yield* Effect.forkChild(runApplication(app));

      yield* waitFor(() => attempts.length >= 1);
      yield* Effect.sleep(250);
      const whileOpen = attempts.length;
      yield* waitFor(() => attempts.length >= 2);
      yield* waitFor(() => completed.length >= 1);
      yield* rmq.send(pub, "after-probe");
      yield* waitFor(() => attempts.length >= 3);
      yield* waitFor(() => completed.length >= 2);
      return { whileOpen, attempts, completed };
    })
  );

  assert.equal(observed.whileOpen, 1, "the consumer stays stopped during the breaker hold");
  assert.equal(observed.attempts.length, 3, "the successful probe completes and the closed consumer resumes");
  assert.deepEqual(observed.attempts.map(({ body }) => body), ["probe-me", "probe-me", "after-probe"]);
  assert.deepEqual(observed.completed, ["probe-me", "after-probe"], "both successful actions complete");
  assert.equal(observed.attempts[0]!.deliveryCount, 0);
  assert.equal(observed.attempts[1]!.deliveryCount, 1, "RabbitMQ counts the requeued delivery");
  assert.equal(observed.attempts[2]!.deliveryCount, 0, "the follow-up is a new message, not a redelivery");
  assert.ok(
    observed.attempts[1]!.at - observed.attempts[0]!.at >= 700,
    "the probe waits for the broker's one-second delay"
  );
});
