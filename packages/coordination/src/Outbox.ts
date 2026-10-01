import { Context, Effect, Option as O, Result } from "effect";
import { decodeCircuitEvent } from "@egress/domain/Model.ts";
import type { CoordinationUnavailable } from "./Coordination.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Where an event waits when the webhook subscriber will not take it. One consumer
 * (the leader's tick), ordered per API. Same port and shape as `CheckpointStore`.
 */

/** Overflow drops the oldest: the subscriber sees a detectable gap, not permanent staleness. */
export const OUTBOX_MAX_PER_API = 500;

/** How many entries one drain pass may replay per API. Bounded for the same reason the redrive is: a recovery must not become its own thundering herd. */
export const OUTBOX_DRAIN_LIMIT = 50;

/** `None`: no longer decodes. It keeps its position; filtering it once sent events twice (ADR 006). */
export type Entry = O.Option<CircuitEvent>;

/**
 * `from` is the absolute position of the first entry. Drains commit a position,
 * not a count: the bound can drop the oldest mid-drain, and a count would then
 * trim undelivered entries.
 */
export type Peeked = {
  readonly from: number;
  readonly entries: ReadonlyArray<Entry>;
};

export class Outbox extends Context.Service<
  Outbox,
  {
    /** Persist an undelivered event. Returns how many entries were dropped to stay inside the bound. */
    readonly append: (event: CircuitEvent) => Effect.Effect<number, CoordinationUnavailable>;
    /** The oldest `limit` entries for one API, in the order they were appended. */
    readonly peek: (apiId: string, limit: number) => Effect.Effect<Peeked, CoordinationUnavailable>;
    /**
     * Drop every entry before position `through` — called only once they have
     * been delivered, or found undeliverable. Entries already dropped by the
     * bound are not counted twice.
     */
    readonly commit: (apiId: string, through: number) => Effect.Effect<void, CoordinationUnavailable>;
    /** Which APIs currently have anything pending. */
    readonly apis: Effect.Effect<ReadonlyArray<string>, CoordinationUnavailable>;
    readonly depth: (apiId: string) => Effect.Effect<number, CoordinationUnavailable>;
  }
>()("@egress/coordination/Outbox") {}

/** One stored entry, read back: anything that does not decode keeps its place as `None`. */
export const decodeEntry = (raw: string): Entry => Result.getSuccess(decodeCircuitEvent(raw));
