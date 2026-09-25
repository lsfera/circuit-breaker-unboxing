import type { ContractState } from "./Contract.ts";
import type { EventType } from "@egress/domain/Model.ts";

/**
 * Plain numbers on the hot path, flushed as deltas once a second. Take one
 * `snapshot` and derive delta and high-water mark from it: re-reading after a
 * suspension marks as published what arrived during it.
 */

/** Bumped from AMQP callbacks and the egress call, which have no fiber to run an Effect in. */
type Counts = {
  ok: number;
  failed: number;
  /** Rejected by Envoy's adaptive-concurrency filter (429) before reaching the third party — backpressure, not a call failure. */
  shed: number;
  refused: number;
  probed: number;
  undecodable: number;
  stale: number;
  discardedFormat: number;
  discardedMalformed: number;
  discardedKeyless: number;
  /** For the heartbeat log line only; RabbitMQ already counts publishes per queue. */
  readonly byType: Map<EventType, number>;
};

export const zero = (): Counts => ({
  ok: 0,
  failed: 0,
  shed: 0,
  refused: 0,
  probed: 0,
  undecodable: 0,
  stale: 0,
  discardedFormat: 0,
  discardedMalformed: 0,
  discardedKeyless: 0,
  byType: new Map(),
});

export const observed = (counts: Counts, type: EventType): void => {
  counts.byType.set(type, (counts.byType.get(type) ?? 0) + 1);
};

type Snapshot = {
  readonly ok: number;
  readonly failed: number;
  readonly shed: number;
  readonly refused: number;
  readonly probed: number;
  readonly undecodable: number;
  readonly stale: number;
  readonly discardedFormat: number;
  readonly discardedMalformed: number;
  readonly discardedKeyless: number;
  readonly gaps: number;
  readonly duplicates: number;
};

export const nothing: Snapshot = {
  ok: 0,
  failed: 0,
  shed: 0,
  refused: 0,
  probed: 0,
  undecodable: 0,
  stale: 0,
  discardedFormat: 0,
  discardedMalformed: 0,
  discardedKeyless: 0,
  gaps: 0,
  duplicates: 0,
};

export const snapshot = (counts: Counts, contract: ContractState): Snapshot => ({
  ok: counts.ok,
  failed: counts.failed,
  shed: counts.shed,
  refused: counts.refused,
  probed: counts.probed,
  undecodable: counts.undecodable,
  stale: counts.stale,
  discardedFormat: counts.discardedFormat,
  discardedMalformed: counts.discardedMalformed,
  discardedKeyless: counts.discardedKeyless,
  gaps: contract.gaps,
  duplicates: contract.duplicates,
});

/** What the registry is owed since the last publication. Positive amounts only. */
type Delta = {
  readonly ok: number;
  readonly failed: number;
  readonly shed: number;
  readonly refused: number;
  readonly probed: number;
  readonly undecodable: number;
  readonly stale: number;
  readonly discardedFormat: number;
  readonly discardedMalformed: number;
  readonly discardedKeyless: number;
  readonly gaps: number;
  readonly duplicates: number;
};

export const since = (published: Snapshot, current: Snapshot): Delta => ({
  ok: current.ok - published.ok,
  failed: current.failed - published.failed,
  shed: current.shed - published.shed,
  refused: current.refused - published.refused,
  probed: current.probed - published.probed,
  undecodable: current.undecodable - published.undecodable,
  stale: current.stale - published.stale,
  discardedFormat: current.discardedFormat - published.discardedFormat,
  discardedMalformed: current.discardedMalformed - published.discardedMalformed,
  discardedKeyless: current.discardedKeyless - published.discardedKeyless,
  gaps: current.gaps - published.gaps,
  duplicates: current.duplicates - published.duplicates,
});
