import { test } from "node:test";
import assert from "node:assert/strict";
import { Result } from "effect";
import {
  controlQueueOptions,
  deadLetterQueueOptions,
  decodeElectionTrigger,
  encodeCircuitEvent,
  encodeElectionTrigger,
  floorQueueOptions,
  sacQueueOptions,
  workQueueOptions,
} from "../src/ControlPlane.ts";
import { decodeCircuitEvent } from "@egress/domain/Model.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/** What this control plane will and will not read, with no broker. */

const event = (over: Partial<CircuitEvent["data"]> = {}): CircuitEvent => ({
  specversion: "1.0",
  type: "egress.circuit.state_changed",
  source: "egress-proxy/control-plane",
  subject: "api://payments-provider",
  id: "id-1",
  time: new Date(1_700_000_000_000).toISOString(),
  datacontenttype: "application/json",
  data: {
    apiId: "payments-provider",
    sequence: 7,
    previousState: "CLOSED",
    state: "OPEN",
    reason: "ALL_ENDPOINTS_EJECTED",
    healthyEndpoints: 0,
    totalEndpoints: 6,
    observedSince: new Date(1_700_000_000_000).toISOString(),
    reportingReplicas: 3,
    ...over,
  },
});

/** What a body decodes to, or the reason it did not — flattened for assertion. */
const readTrigger = (body: string) =>
  Result.match(decodeElectionTrigger(body), {
    onSuccess: (t) => t.sequence as number | string,
    onFailure: (f) => f as string,
  });

const readEvent = (body: string) =>
  Result.match(decodeCircuitEvent(body), {
    onSuccess: (e) => e.data.sequence as number | string,
    onFailure: (f) => f as string,
  });

test("a trigger round-trips through the pair that publishes and reads it", () => {
  assert.equal(readTrigger(encodeElectionTrigger({ sequence: 0 })), 0);
  assert.equal(readTrigger(encodeElectionTrigger({ sequence: 41 })), 41);
});

/**
 * The dedupe is `sequence <= probedSequence` and every comparison against `NaN`
 * is false, so a trigger that cannot be ordered would probe again — on the one
 * state whose contract is "exactly one call".
 */
test("a sequence that cannot be ordered does not decode", () => {
  for (const body of [
    '{"sequence":"7"}',
    '{"sequence":{}}',
    '{"sequence":[7]}',
    '{"sequence":true}',
    '{"sequence":1.5}',
    '{"sequence":-1}',
  ]) {
    assert.equal(readTrigger(body), "schema-mismatch", body);
  }
});

/** Neither acted on nor preserved is the one outcome this fleet refuses. */
test("a trigger with no sequence is a mismatch, not a silently ignored -1", () => {
  assert.equal(readTrigger("{}"), "schema-mismatch");
  assert.equal(readTrigger('{"sequence":null}'), "schema-mismatch");
  assert.equal(readTrigger("5"), "schema-mismatch");
  assert.equal(readTrigger("null"), "schema-mismatch");
});

/** Told apart because they call for different reactions. */
test("not being JSON is a different failure from not matching the schema", () => {
  assert.equal(readTrigger("not json"), "malformed-json");
  assert.equal(readTrigger(""), "malformed-json");
  assert.equal(readTrigger('{"sequence":'), "malformed-json");
});

test("a published event round-trips, and the two failures stay distinct", () => {
  assert.equal(readEvent(encodeCircuitEvent(event())), 7);
  assert.equal(readEvent("{oops"), "malformed-json");
  assert.equal(readEvent('{"specversion":"1.0"}'), "schema-mismatch");
});

/**
 * The checkpoint validator has always been strict, so a looser published contract
 * would let a reason be written and then fail to decode on rehydration — read as
 * "no checkpoint", cold-starting a new leader at CLOSED.
 */
test("a reason outside the vocabulary is not a valid event", () => {
  assert.equal(
    readEvent(encodeCircuitEvent(event({ reason: "SOMETHING_ELSE" as never }))),
    "schema-mismatch",
  );
  assert.equal(readEvent(encodeCircuitEvent(event({ reason: "PROBE_FAILED" }))), 7);
});

/** The same rule on the value the delivery contract orders by. */
test("an event whose sequence cannot be ordered is not a valid event", () => {
  for (const sequence of [-1, 2.5]) {
    assert.equal(
      readEvent(encodeCircuitEvent(event({ sequence }))),
      "schema-mismatch",
      String(sequence),
    );
  }
  assert.equal(readEvent(encodeCircuitEvent(event({ sequence: 0 }))), 0);
});

/**
 * RabbitMQ 4.3 closes the connection (541) on any transient queue that is not
 * exclusive, and the daemons declare every queue on connect — so one
 * `durable: false` here is a fleet that crash-loops on the upgraded broker.
 */
test("every queue the fleet declares is durable", () => {
  const declared = {
    work: workQueueOptions("api"),
    deadLetter: deadLetterQueueOptions(),
    election: sacQueueOptions("api"),
    control: controlQueueOptions("api"),
    floor: floorQueueOptions(),
  };
  assert.deepEqual(
    Object.entries(declared).filter(([, options]) => options.durable !== true).map(([name]) => name),
    [],
  );
});

test("the queues nobody reads after a daemon leaves still delete themselves", () => {
  assert.ok(Number(controlQueueOptions("api").args["x-expires"]) > 0);
  assert.ok(Number(floorQueueOptions().args["x-expires"]) > 0);
});
