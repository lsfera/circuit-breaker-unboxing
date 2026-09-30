import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { Effect, Fiber, Option as O } from "effect";
import { broker, skipIfNoDocker, startBroker, stopBroker, waitFor } from "../../../rmq/test/integration/harness.ts";
import { Rmq } from "@egress/rmq/Client.ts";
import {
  CONTROL_EXCHANGE,
  CONTROL_EXCHANGE_OPTIONS,
  encodeCircuitEvent,
  encodeWorkMessage,
  routingKeyFor,
  WORK_CONTENT_TYPE,
  WORK_MESSAGE_TYPE,
  workQueueFor,
  workQueueOptions,
} from "@egress/rmq/ControlPlane.ts";
import { SEQUENCED_EVENT, SNAPSHOT_EVENT, State } from "@egress/domain/Model.ts";
import type { CircuitEvent, EventType } from "@egress/domain/Model.ts";
import { runDaemon } from "../../src/daemon.ts";

/**
 * The daemon's shell against a real broker: what the reducer decides only matters
 * if the channels follow. One daemon, so it is the floor whenever there is one.
 */

before(startBroker);
after(stopBroker);

const API = "it-payments";
const LEASE = { epoch: "it", counter: 1 };

const event = (type: EventType, state: State, sequence: number): CircuitEvent => ({
  specversion: "1.0",
  type,
  source: "test",
  subject: `api://${API}`,
  id: `${type}-${sequence}`,
  time: new Date().toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId: API,
    sequence,
    previousState: null,
    state,
    reason: "HEALTHY",
    healthyEndpoints: 6,
    totalEndpoints: 6,
    observedSince: new Date().toISOString(),
    reportingReplicas: 3,
    lease: LEASE,
  },
});

test("idle until heard, working from the floor on CLOSED, stopped on OPEN", async (t) => {
  if (skipIfNoDocker(t)) return;

  // The third party: every call succeeds, and each is counted.
  let calls = 0;
  const upstream = createServer((_req, res) => {
    calls++;
    res.writeHead(200).end("ok");
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const { port } = upstream.address() as AddressInfo;

  const program = Effect.gen(function* () {
    const rmq = yield* Rmq;
    yield* rmq.declareTopicExchange(CONTROL_EXCHANGE, CONTROL_EXCHANGE_OPTIONS);
    yield* rmq.declareQueue(workQueueFor(API), workQueueOptions(API));
    const control = yield* rmq.publisherToExchange(CONTROL_EXCHANGE, routingKeyFor(API));
    const work = yield* rmq.publisherToQueue(workQueueFor(API), { contentType: WORK_CONTENT_TYPE, type: WORK_MESSAGE_TYPE });
    const enqueue = (from: number, count: number) =>
      Effect.forEach(
        Array.from({ length: count }, (_, i) => from + i),
        (n) => rmq.send(work, encodeWorkMessage({ apiId: API, n }), { messageId: `it:${n}` }),
        { discard: true },
      );

    const daemon = yield* Effect.forkChild(
      runDaemon({
        apiId: API,
        instanceId: "it-daemon",
        egressAddr: `http://127.0.0.1:${port}`,
        apiPath: "/pay",
        maxInFlight: 4,
        redriveOnClose: false,
        redriveMax: 100,
        limit: O.none(),
      }),
    );

    // Unheard: work waits, however long it has been queued.
    yield* enqueue(0, 20);
    yield* Effect.sleep("3 seconds");
    const whileUnheard = calls;

    // The first CLOSED elects the floor, which is this daemon, and it starts on its own.
    yield* rmq.send(control, encodeCircuitEvent(event(SNAPSHOT_EVENT, State.CLOSED, 1)));
    yield* waitFor(() => calls >= 20);
    const afterClosed = calls;

    // OPEN: the work consumer goes, and new work stays queued.
    yield* rmq.send(control, encodeCircuitEvent(event(SEQUENCED_EVENT, State.OPEN, 2)));
    yield* Effect.sleep("2 seconds");
    const atOpen = calls;
    yield* enqueue(100, 10);
    yield* Effect.sleep("3 seconds");
    const whileOpen = calls - atOpen;

    // CLOSED again: the ramp starts from the floor, which is still this daemon.
    yield* rmq.send(control, encodeCircuitEvent(event(SEQUENCED_EVENT, State.CLOSED, 3)));
    yield* waitFor(() => calls >= atOpen + 10);
    const afterRecovery = calls - atOpen;

    yield* Fiber.interrupt(daemon);
    return { whileUnheard, afterClosed, whileOpen, afterRecovery };
  });

  try {
    const result = await Effect.runPromise(
      Effect.scoped(Effect.provide(program, Rmq.layer({ host: broker.host, port: broker.port }))) as Effect.Effect<{
        whileUnheard: number;
        afterClosed: number;
        whileOpen: number;
        afterRecovery: number;
      }>,
    );
    assert.equal(result.whileUnheard, 0, "a daemon that has heard nothing calls nobody");
    assert.ok(result.afterClosed >= 20, `the floor drains the queue once CLOSED is heard (${result.afterClosed})`);
    assert.equal(result.whileOpen, 0, "OPEN stops the calls");
    assert.ok(result.afterRecovery >= 10, `the work held during OPEN is drained on CLOSED (${result.afterRecovery})`);
  } finally {
    upstream.close();
  }
});
