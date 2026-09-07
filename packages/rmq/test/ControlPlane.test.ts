import { test } from "node:test";
import assert from "node:assert/strict";
import { Result } from "effect";
import {
  decodeCircuitEvent,
  decodeElectionTrigger,
  encodeCircuitEvent,
  encodeElectionTrigger,
} from "../src/ControlPlane.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * What this control plane will and will not read, with no broker.
 *
 * Both message kinds are declared here — the circuit event the aggregator
 * publishes, and the election trigger daemons publish to each other — so both
 * are readable the same way and testable the same way. The trigger only
 * recently became one of them: it was `JSON.stringify` at the publisher and
 * `Number(JSON.parse(body).sequence ?? -1)` at each of two consumers, which is
 * three places that had to agree and nothing making them.
 */

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
 * The hole this schema exists to close, and the same one the lease token had.
 *
 * The elected daemon dedupes triggers with `sequence <= probedSequence`, and
 * every comparison against `NaN` is false — so a trigger whose sequence could
 * not be ordered read as a new transition and probed again, on a state whose
 * entire contract is "exactly one call". `Number(...)` produced `NaN` from
 * every one of these.
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

/**
 * The quieter half. `?? -1` turned a trigger with no usable sequence into a
 * number every daemon silently ignores — neither acted on nor preserved,
 * which is the one outcome this fleet refuses everywhere else. It is an
 * unreadable message now, so it is dead-lettered and counted.
 */
test("a trigger with no sequence is a mismatch, not a silently ignored -1", () => {
  assert.equal(readTrigger("{}"), "schema-mismatch");
  assert.equal(readTrigger('{"sequence":null}'), "schema-mismatch");
  assert.equal(readTrigger("5"), "schema-mismatch");
  assert.equal(readTrigger("null"), "schema-mismatch");
});

/**
 * Told apart because they call for different reactions: a mismatch is a
 * version skew between whatever published and this fleet, and something that
 * is not JSON at all means the publisher is not who we think it is.
 */
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
 * `reason` was `Schema.String` while the vocabulary sat in the same file, and
 * the tell was the `as Reason` cast the aggregator needed to put a decoded
 * event's reason into a checkpoint. The checkpoint's own validator has always
 * been the strict one, so the write side was looser than the read side: a
 * reason outside the vocabulary could be checkpointed and then fail to decode
 * on rehydration, which reads as "no checkpoint" and cold-starts a new leader
 * at CLOSED.
 */
test("a reason outside the vocabulary is not a valid event", () => {
  assert.equal(
    readEvent(encodeCircuitEvent(event({ reason: "SOMETHING_ELSE" as never }))),
    "schema-mismatch",
  );
  assert.equal(readEvent(encodeCircuitEvent(event({ reason: "PROBE_FAILED" }))), 7);
});

/**
 * The same orderability rule as the trigger, on the value the whole delivery
 * contract is built from: `Contract.observe` counts gaps and duplicates by
 * comparing sequences, and a sequence that cannot be compared cannot be
 * checked.
 */
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
