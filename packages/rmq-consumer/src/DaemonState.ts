import { Match, Option as O } from "effect";
import { State, supersedes } from "@egress/domain/Model.ts";
import type { Applied, EventType, Lease } from "@egress/domain/Model.ts";
import { initial as initialPolicy, runsWork, silent, step } from "./DaemonPolicy.ts";
import type { DaemonPolicyState } from "./DaemonPolicy.ts";

/**
 * Everything a daemon decides, in one value moved by one pure function, so each
 * transition is atomic against concurrent AMQP callbacks. Channels stay outside:
 * they are the "actual" side `plan` compares against.
 */
export type DaemonState = {
  /**
   * How much `circuit` is worth: nothing yet (`unheard`), the last event (`heard`),
   * or stale because events stopped (`silent`) — see ADR 019.
   */
  readonly control: "unheard" | "heard" | "silent";
  /** When an event was last applied, or the daemon started: what silence is measured from. */
  readonly lastHeardAt: number;
  readonly circuit: State;
  /**
   * The event `circuit` came from. A stale one — a paused leader resuming, a
   * duplicate, an out-of-order delivery — is ignored rather than applied.
   */
  readonly applied: O.Option<Applied>;
  readonly policy: DaemonPolicyState;
  /** Every daemon publishes a trigger, and SAC hands them all to one: this turns them back into one probe. */
  readonly probedSequence: number;
  /** The same idea for the redrive election: one replay per recovery, not one per trigger message. */
  readonly redrivenSequence: number;
  /**
   * Until when this daemon holds the floor. A lease, not a flag: SAC promotes
   * silently, so a replacement learns it holds the floor from the next event,
   * and the lease stops a dead holder's claim outliving it (ADR 013).
   */
  readonly floorUntil: number;
};

/**
 * Four snapshot intervals: one lost snapshot is noise, four is a control plane
 * that has stopped. The floor lease is the same length, for the same reason.
 */
export const SILENCE_MS = 60_000;
export const FLOOR_LEASE_MS = 60_000;

export const initialState = (now: number): DaemonState => ({
  control: "unheard",
  lastHeardAt: now,
  // Worth nothing until an event says otherwise: the policy, not this, keeps the daemon idle.
  circuit: State.CLOSED,
  applied: O.none(),
  policy: initialPolicy(now),
  probedSequence: -1,
  redrivenSequence: -1,
  floorUntil: 0,
});

/** Whether the broker's election of this daemon as the floor is still current. */
export const isFloor = (state: DaemonState, now: number): boolean => now < state.floorUntil;

/** Everything that can move a daemon: two from the control plane, three from the broker's elections, two from its own clock. */
export type Command =
  | {
      readonly _tag: "CircuitChanged";
      readonly type: EventType;
      readonly lease: O.Option<Lease>;
      readonly state: State;
      readonly sequence: number;
      readonly at: number;
    }
  /** Once a second: advances the ramp, and notices a control plane gone quiet. */
  | { readonly _tag: "ClockTick"; readonly at: number }
  /** A delivery on the floor queue: the broker elected this daemon. */
  | { readonly _tag: "FloorElected"; readonly at: number }
  | { readonly _tag: "ProbeTriggered"; readonly sequence: number }
  | { readonly _tag: "RedriveTriggered"; readonly sequence: number }
  /** Un-marks the sequence so the redelivered trigger is acted on, not deduped. */
  | { readonly _tag: "TriggerFailed"; readonly election: "probe" | "redrive"; readonly sequence: number }
  /** On a timer: dead letters that arrive while CLOSED would otherwise never be replayed. */
  | { readonly _tag: "SweepTick"; readonly at: number };

/**
 * What the shell must do about a transition. Returned rather than performed,
 * so the decision is testable without a broker.
 */
export type Action =
  | { readonly _tag: "PublishProbeTrigger"; readonly sequence: number }
  | { readonly _tag: "PublishRedriveTrigger"; readonly sequence: number }
  | { readonly _tag: "Probe" }
  | { readonly _tag: "Redrive" };

type Transition = {
  readonly next: DaemonState;
  readonly actions: ReadonlyArray<Action>;
  /** The command was out-ranked or already handled, so nothing changed. */
  readonly ignored: boolean;
};

const ignore = (state: DaemonState): Transition => ({ next: state, actions: [], ignored: true });

/** Un-mark `sequence` only if nothing newer was marked since: a later transition's mark stands. */
const unmark = (marked: number, sequence: number): number => (marked === sequence ? sequence - 1 : marked);

export const reduce = (
  state: DaemonState,
  command: Command,
  redriveOnClose: boolean,
): Transition =>
  Match.valueTags(command, {
    CircuitChanged: (command): Transition =>
      supersedes(state.applied, command)
        ? {
          next: {
            ...state,
            control: "heard",
            lastHeardAt: command.at,
            applied: O.some({ lease: command.lease, sequence: command.sequence }),
            circuit: command.state,
            policy: step(state.policy, command.state, command.at),
          },
          actions: [
            ...(command.state === State.HALF_OPEN
              ? [{ _tag: "PublishProbeTrigger", sequence: command.sequence } as const]
              : []),
            // Only on the actual transition back into CLOSED. The aggregator's
            // periodic snapshots repeat the current state, and a redrive per snapshot
            // would replay the queue every fifteen seconds forever.
            ...(redriveOnClose && command.state === State.CLOSED && state.circuit !== State.CLOSED
              ? [{ _tag: "PublishRedriveTrigger", sequence: command.sequence } as const]
              : []),
          ],
          ignored: false,
        }
        : ignore(state),

    // Silence first: a ramp on stale knowledge is still stale. Only a heard CLOSED
    // ramps; every other state is a level.
    ClockTick: (command): Transition => ({
      next:
        state.control !== "silent" && command.at - state.lastHeardAt >= SILENCE_MS
          ? { ...state, control: "silent", policy: silent(command.at) }
          : state.control === "heard" && state.circuit === State.CLOSED
            ? { ...state, policy: step(state.policy, state.circuit, command.at) }
            : state,
      actions: [],
      ignored: false,
    }),

    FloorElected: (command): Transition => ({
      next: { ...state, floorUntil: command.at + FLOOR_LEASE_MS },
      actions: [],
      ignored: false,
    }),

    ProbeTriggered: (command): Transition =>
      command.sequence > state.probedSequence
        ? { next: { ...state, probedSequence: command.sequence }, actions: [{ _tag: "Probe" }], ignored: false }
        : ignore(state),

    RedriveTriggered: (command): Transition =>
      command.sequence > state.redrivenSequence
        ? { next: { ...state, redrivenSequence: command.sequence }, actions: [{ _tag: "Redrive" }], ignored: false }
        : ignore(state),

    TriggerFailed: (command): Transition => ({
      next:
        command.election === "probe"
          ? { ...state, probedSequence: unmark(state.probedSequence, command.sequence) }
          : { ...state, redrivenSequence: unmark(state.redrivenSequence, command.sequence) },
      actions: [],
      ignored: false,
    }),

    // No sequence, no dedupe, no state change. Not on a CLOSED that is stale or assumed.
    SweepTick: (command): Transition => ({
      next: state,
      actions:
        redriveOnClose && state.control === "heard" && state.circuit === State.CLOSED && isFloor(state, command.at)
          ? [{ _tag: "Redrive" }]
          : [],
      ignored: false,
    }),
  });

/**
 * Whether the event that installed `applied` is still the one `state` is on: a
 * trigger it owes is not worth publishing once a newer event has replaced it.
 * By value, so a snapshot repeating the same event keeps it current.
 */
export const isCurrent = (state: DaemonState, applied: O.Option<Applied>): boolean =>
  O.match(state.applied, {
    onNone: () => O.isNone(applied),
    onSome: (now) =>
      O.match(applied, {
        onNone: () => false,
        onSome: (then) =>
          now.sequence === then.sequence &&
          O.getOrUndefined(O.map(now.lease, (l) => l.epoch)) === O.getOrUndefined(O.map(then.lease, (l) => l.epoch)) &&
          O.getOrUndefined(O.map(now.lease, (l) => l.counter)) === O.getOrUndefined(O.map(then.lease, (l) => l.counter)),
      }),
  });

/** Which connections are allowed to exist in this state. */
type Connections = {
  readonly work: boolean;
  readonly probe: boolean;
  readonly redrive: boolean;
};

export const desired = (
  state: DaemonState,
  self: { readonly position: number; readonly isFloor: boolean },
): Connections => {
  // A silent control plane's circuit is not believed: the policy alone decides.
  const believed = state.control === "silent" ? O.none<State>() : O.some(state.circuit);
  return {
    // In HALF_OPEN only the elected prober calls; a low hash position must not race it.
    work: !O.contains(believed, State.HALF_OPEN) && runsWork(state.policy, self),
    // Leaving HALF_OPEN or CLOSED retires the probe or the redrive.
    probe: O.contains(believed, State.HALF_OPEN),
    redrive: O.contains(believed, State.CLOSED),
  };
};

type Plan = {
  readonly startWork: boolean;
  readonly stopWork: boolean;
  readonly stopProbe: boolean;
  readonly stopRedrive: boolean;
};

/** Opens only work channels: probe and redrive are opened by the elected daemon, and only retired here. */
export const plan = (want: Connections, have: Connections): Plan => ({
  startWork: want.work && !have.work,
  stopWork: !want.work && have.work,
  stopProbe: !want.probe && have.probe,
  stopRedrive: !want.redrive && have.redrive,
});
