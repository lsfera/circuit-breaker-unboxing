import { Clock, Effect, Layer, Option as O, Ref } from "effect";
import { randomUUID } from "node:crypto";
import {
  CheckpointFenced,
  CheckpointStore,
  isFenced,
  LeaderElection,
} from "../../src/Coordination.ts";
import type { Checkpoint, LeaseToken } from "../../src/Coordination.ts";
import { Outbox, OUTBOX_MAX_PER_API } from "../../src/Outbox.ts";
import type { Peeked } from "../../src/Outbox.ts";
import type { CircuitEvent } from "@egress/domain/Model.ts";

/**
 * Test doubles for the coordination ports, never a runtime backend: one
 * process's memory cannot exclude another process, so two instances would both
 * lead. They are the unit tests' reference for the rules the Redis scripts must
 * obey; test/integration/ checks the scripts against a real Redis.
 */

type Lock = {
  readonly holderId: string;
  readonly expiresAt: number;
};

type Lease = {
  readonly counter: number;
  readonly lock: O.Option<Lock>;
};

export const makeInMemoryCoordination = Effect.gen(function* () {
  // Never rotates: this store cannot lose its state without the process.
  const epoch = randomUUID();
  // The counter lives beside the lock, not in it, as Redis keeps the token key
  // apart from the holder key: a release must not restart the count inside a
  // live epoch, or a stale leader's higher counter outranks the next one.
  const lease = yield* Ref.make<Lease>({ counter: 0, lock: O.none() });
  const checkpoints = yield* Ref.make(new Map<string, Checkpoint>());

  const tryAcquireOrRenew = (holderId: string, ttlMs: number) =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        Ref.modify(lease, (current) => {
          type Outcome = readonly [O.Option<LeaseToken>, Lease];
          /** Someone else holds a live lease: no token, and the lease is left alone. */
          const deny: Outcome = [O.none(), current];
          /** Same holder, same token, extended TTL. */
          const renew: Outcome = [
            O.some({ epoch, counter: current.counter }),
            { counter: current.counter, lock: O.some({ holderId, expiresAt: now + ttlMs }) },
          ];
          /** Expired, released or never held: a genuine handoff, counter strictly increases. */
          const handOver: Outcome = [
            O.some({ epoch, counter: current.counter + 1 }),
            { counter: current.counter + 1, lock: O.some({ holderId, expiresAt: now + ttlMs }) },
          ];
          return O.match(current.lock, {
            onNone: () => handOver,
            onSome: (held) =>
              held.expiresAt <= now ? handOver : held.holderId === holderId ? renew : deny,
          });
        }),
      ),
    );

  const release = (holderId: string) =>
    Ref.update(lease, (current) => ({
      ...current,
      lock: O.filter(current.lock, (held) => held.holderId !== holderId),
    }));

  const save = (apiId: string, token: LeaseToken, checkpoint: Checkpoint) =>
    Ref.get(lease).pipe(
      Effect.map(({ counter }): LeaseToken => ({ epoch, counter })),
      Effect.flatMap((current) =>
        isFenced(token, current)
          ? Effect.fail(
              new CheckpointFenced({ apiId, attempted: token, current: O.some(current) }),
            )
          : Ref.update(checkpoints, (map) => new Map(map).set(apiId, checkpoint)),
      ),
    );

  const load = (apiId: string) =>
    Ref.get(checkpoints).pipe(Effect.map((map) => O.fromUndefinedOr(map.get(apiId))));

  return {
    leaderElection: { tryAcquireOrRenew, release },
    checkpointStore: { save, load },
  };
});

/** Both services, sharing the one token counter that makes fencing correct. */
export const InMemoryCoordinationLayer: Layer.Layer<LeaderElection | CheckpointStore> =
  Layer.unwrap(
    Effect.map(makeInMemoryCoordination, ({ leaderElection, checkpointStore }) =>
      Layer.mergeAll(
        Layer.succeed(LeaderElection, leaderElection),
        Layer.succeed(CheckpointStore, checkpointStore),
      ),
    ),
  );

export const makeInMemoryOutbox = Effect.gen(function* () {
  /** Per API: the pending events, and the absolute position of the first. */
  type Queue = { readonly head: number; readonly events: ReadonlyArray<CircuitEvent> };
  const queues = yield* Ref.make(new Map<string, Queue>());
  const queueOf = (map: Map<string, Queue>, apiId: string): Queue =>
    map.get(apiId) ?? { head: 0, events: [] };

  const append = (event: CircuitEvent) =>
    Ref.modify(queues, (map) => {
      const apiId = event.data.apiId;
      const q = queueOf(map, apiId);
      const next = [...q.events, event];
      const dropped = Math.max(0, next.length - OUTBOX_MAX_PER_API);
      return [dropped, new Map(map).set(apiId, { head: q.head + dropped, events: next.slice(dropped) })];
    });

  const peek = (apiId: string, limit: number) =>
    Ref.get(queues).pipe(
      Effect.map((map): Peeked => {
        const q = queueOf(map, apiId);
        return { from: q.head, entries: q.events.slice(0, limit).map(O.some) };
      }),
    );

  // The head is kept when the queue empties, so a commit from an older peek
  // can never trim entries appended after it.
  const commit = (apiId: string, through: number) =>
    Ref.update(queues, (map) => {
      const q = queueOf(map, apiId);
      const trim = Math.max(0, through - q.head);
      return new Map(map).set(apiId, { head: q.head + trim, events: q.events.slice(trim) });
    });

  const apis = Ref.get(queues).pipe(
    Effect.map((map) => [...map].filter(([, q]) => q.events.length > 0).map(([apiId]) => apiId)),
  );
  const depth = (apiId: string) =>
    Ref.get(queues).pipe(Effect.map((map) => queueOf(map, apiId).events.length));

  return { append, peek, commit, apis, depth } as const;
});

export const InMemoryOutboxLayer = Layer.effect(Outbox, makeInMemoryOutbox);
