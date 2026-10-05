import { Effect } from "effect";
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import { Rmq } from "../../src/Client.ts";
import { broker, brokerExec, skipIfNoDocker, startBroker, stopBroker, text, waitFor } from "./harness.ts";

/**
 * The gap this guards: a reconnect runs `setup` (topology replay) on a connection nobody is listening to yet. A
 * broker that fails the connection during the replay must make that attempt fail and another be scheduled; under
 * amqplib it emitted `error` with no listener, which Node throws, and the process exited.
 *
 * The failure is a missed heartbeat. The broker closes every
 * connection, then suspends whichever connections open next, a few
 * milliseconds in, so the client's replay stalls and its 1s heartbeat (set
 * here, shorter than the default, to fit the window) fails
 * inside `setup`. Four seconds later they are resumed. Surviving it means
 * reconnecting afterwards: a publish goes through.
 */

beforeAll(startBroker);
afterAll(stopBroker);

const QUEUES = 400;

test("a connection the broker closes during topology replay is reconnected, not a crash", async (t) => {
  if (skipIfNoDocker(t)) return;

  const crashes: Array<unknown> = [];
  const onUncaught = (error: unknown) => void crashes.push(error);
  process.on("uncaughtException", onUncaught);

  const prefix = `replay.${Date.now()}`;
  const seen: Array<string> = [];
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.provide(
          Effect.gen(function*() {
            const rmq = yield* Rmq;
            yield* Effect.forEach(
              Array.from({ length: QUEUES }, (_, i) => `${prefix}.${i}`),
              (q) => rmq.declareQueue(q, { durable: true }),
              { discard: true }
            );
            const last = `${prefix}.${QUEUES - 1}`;
            yield* rmq.consume(last, (body) => void seen.push(text(body)));
            const pub = yield* rmq.publisherToQueue(last);

            yield* Effect.promise(() =>
              brokerExec([
                "rabbitmqctl",
                "eval",
                "Old = rabbit_networking:connections(), " +
                "rabbit_networking:close_all_connections(\"replay test\"), " +
                "Wait = fun W(N) when N > 0 -> case rabbit_networking:connections() -- Old of " +
                "[] -> timer:sleep(2), W(N - 1); New -> timer:sleep(15), [sys:suspend(P) || P <- New], " +
                "timer:sleep(4000), [sys:resume(P) || P <- New], length(New) end; W(_) -> 0 end, " +
                "Wait(2500)."
              ])
            );

            yield* waitFor(() => false, 3000);
            yield* rmq.send(pub, "after");
            yield* waitFor(() => seen.length >= 1, 30_000);
          }),
          Rmq.layer({ host: broker.host, port: broker.port, heartbeat: 1 })
        )
      ) as Effect.Effect<void>
    );
  } finally {
    process.off("uncaughtException", onUncaught);
  }

  assert.deepEqual(crashes.map(String), [], "no error escaped as an uncaught exception");
  assert.deepEqual(seen, ["after"], "the client recovered and delivered");
});
