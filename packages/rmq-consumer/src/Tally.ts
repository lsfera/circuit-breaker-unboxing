import type { ContractState } from "./Contract.ts";

/**
 * What this daemon has counted, and what the metrics registry is still owed.
 *
 * The counting itself has to be cheap and untyped-by-Effect: the work path is
 * a plain async function running a few hundred times a second, and forking a
 * fiber per metric write would be the most expensive thing in it. So the
 * daemon bumps plain numbers and a flush loop publishes the difference once a
 * second, which is precisely what a Prometheus counter is.
 *
 * The arithmetic around that lived inline in daemon.ts, in the same shape as
 * the delivery-contract check before it moved to Contract.ts: a handful of
 * subtractions nobody could test without five containers and an outage. It had
 * the bug that shape invites.
 *
 * ## The bug this file exists to make impossible
 *
 * Publishing suspends — `Metric.update` is an Effect — and the old flush read
 * its counters *twice*: once to compute what to publish, and again afterwards
 * to record what had been published.
 *
 *     for (const [type, count] of eventsByType) {
 *       const seen = count - (flushedEvents.get(type) ?? 0);
 *       if (seen > 0) yield* Metric.update(...);   // suspends here
 *     }
 *     flushedEvents = new Map(eventsByType);       // reads the live map again
 *
 * Anything counted during those suspensions landed in the second read but not
 * the first, so it was marked as published without ever being published — and
 * because the mark had moved, no later flush would ever pick it up. Reproduced
 * with one control event arriving mid-flush: observed 2, published 1, lost 1,
 * permanently.
 *
 * The numeric counters escaped it by accident. Their two reads sit in adjacent
 * *synchronous* statements, so nothing can interleave between them — the same
 * mistake, saved by where the semicolons happened to fall.
 *
 * The fix is structural rather than careful: take **one** immutable snapshot,
 * derive both the delta and the new high-water mark from that single reading,
 * and let publishing suspend as much as it likes. Nothing that happens during
 * a flush can be lost, because nothing is read a second time.
 */

/**
 * The mutable side, bumped from the AMQP callbacks and the egress call.
 *
 * A record of plain numbers rather than a `Ref`, and deliberately: these are
 * incremented from callbacks that are not running inside a fiber, and the
 * point of counting here rather than at the metric is to keep the hot path to
 * a property increment.
 */
export type Counts = {
  ok: number;
  failed: number;
  probed: number;
  redriven: number;
  undecodable: number;
  /** Control-plane events by CloudEvents type, so snapshots and transitions can be told apart. */
  readonly byType: Map<string, number>;
};

export const zero = (): Counts => ({
  ok: 0,
  failed: 0,
  probed: 0,
  redriven: 0,
  undecodable: 0,
  byType: new Map(),
});

export const observed = (counts: Counts, type: string): void => {
  counts.byType.set(type, (counts.byType.get(type) ?? 0) + 1);
};

/**
 * One reading of everything, taken at a single instant and immutable
 * afterwards — including a *copy* of the by-type map, which is the half the
 * old code got wrong.
 *
 * The delivery-contract counters come from `ContractState` rather than from
 * `Counts`, because the daemon does not increment them: `observe` derives them
 * from the sequence stream. Folding them in here is what makes one snapshot
 * cover everything a flush publishes.
 */
export type Snapshot = {
  readonly ok: number;
  readonly failed: number;
  readonly probed: number;
  readonly redriven: number;
  readonly undecodable: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly byType: ReadonlyMap<string, number>;
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

/**
 * What a Prometheus counter is owed since the last publication.
 *
 * Only positive amounts: a counter is not asked to go up by zero, and it
 * cannot go down. `byType` is a list rather than a map because that is what
 * the caller does with it — one `Metric.update` per entry — and because an
 * empty list says "nothing to publish" without a lookup.
 */
export type Delta = {
  readonly ok: number;
  readonly failed: number;
  readonly probed: number;
  readonly redriven: number;
  readonly undecodable: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly byType: ReadonlyArray<readonly [type: string, count: number]>;
};

export const since = (published: Snapshot, current: Snapshot): Delta => {
  const byType: Array<readonly [string, number]> = [];
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
