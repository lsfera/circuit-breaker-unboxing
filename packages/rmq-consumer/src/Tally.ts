import type { ContractState } from "./Contract.ts";
import type { EventType } from "@egress/domain/Model.ts";

/**
 * Counting for the metrics registry: plain numbers on the hot path, flushed
 * as deltas once a second.
 *
 * The invariant is `snapshot`. Take one reading, derive both the delta and the
 * new high-water mark from it, and publishing may then suspend freely.
 * Re-reading the counters after a suspension marks as published whatever
 * arrived during it.
 */

/** Bumped from AMQP callbacks and the egress call, which have no fiber to run an Effect in. */
type Counts = {
  ok: number;
  failed: number;
  probed: number;
  redriven: number;
  undecodable: number;
  /** Control-plane events by CloudEvents type. */
  readonly byType: Map<EventType, number>;
};

export const zero = (): Counts => ({
  ok: 0,
  failed: 0,
  probed: 0,
  redriven: 0,
  undecodable: 0,
  byType: new Map(),
});

export const observed = (counts: Counts, type: EventType): void => {
  counts.byType.set(type, (counts.byType.get(type) ?? 0) + 1);
};

/**
 * One reading of everything, immutable afterwards — the by-type map included.
 * `gaps`/`duplicates` come from `ContractState` because `observe` derives them
 * rather than the daemon incrementing them.
 */
type Snapshot = {
  readonly ok: number;
  readonly failed: number;
  readonly probed: number;
  readonly redriven: number;
  readonly undecodable: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly byType: ReadonlyMap<EventType, number>;
};

export const nothing: Snapshot = {
  ok: 0,
  failed: 0,
  probed: 0,
  redriven: 0,
  undecodable: 0,
  gaps: 0,
  duplicates: 0,
  byType: new Map(),
};

export const snapshot = (counts: Counts, contract: ContractState): Snapshot => ({
  ok: counts.ok,
  failed: counts.failed,
  probed: counts.probed,
  redriven: counts.redriven,
  undecodable: counts.undecodable,
  gaps: contract.gaps,
  duplicates: contract.duplicates,
  byType: new Map(counts.byType),
});

/** What the registry is owed since the last publication. Positive amounts only. */
type Delta = {
  readonly ok: number;
  readonly failed: number;
  readonly probed: number;
  readonly redriven: number;
  readonly undecodable: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly byType: ReadonlyArray<readonly [type: EventType, count: number]>;
};

export const since = (published: Snapshot, current: Snapshot): Delta => {
  const byType: Array<readonly [EventType, number]> = [];
  for (const [type, count] of current.byType) {
    const seen = count - (published.byType.get(type) ?? 0);
    if (seen > 0) byType.push([type, seen] as const);
  }
  return {
    ok: current.ok - published.ok,
    failed: current.failed - published.failed,
    probed: current.probed - published.probed,
    redriven: current.redriven - published.redriven,
    undecodable: current.undecodable - published.undecodable,
    gaps: current.gaps - published.gaps,
    duplicates: current.duplicates - published.duplicates,
    byType,
  };
};
