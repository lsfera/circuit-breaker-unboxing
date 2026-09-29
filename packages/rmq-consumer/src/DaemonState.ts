import { Match, Option as O } from "effect";
import { State, supersedes } from "@egress/domain/Model.ts";
import type { Applied, EventType, Lease } from "@egress/domain/Model.ts";
import { initial as initialPolicy, runsWork, step } from "./DaemonPolicy.ts";
import type { DaemonPolicyState } from "./DaemonPolicy.ts";

/**
 * Everything a daemon decides, in one value moved by one pure function, so each
 * transition is atomic against concurrent AMQP callbacks. Channels stay outside:
 * they are the "actual" side `plan` compares against.
 */
export type DaemonState = {
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
};

export const initialState = (now: number): DaemonState => ({
  // CLOSED until told otherwise: a daemon that starts mid-incident learns the
  // real state from the aggregator's next snapshot.
  circuit: State.CLOSED,
  applied: O.none(),
  policy: initialPolicy(now),
  probedSequence: -1,
  redrivenSequence: -1,
});

/** Everything that can move a daemon: two from the control plane, two from the broker's elections, two from its own clock. */
export type Command =
  | {
      readonly _tag: "CircuitChanged";
      readonly type: EventType;
      readonly lease: O.Option<Lease>;
      readonly state: State;
      readonly sequence: number;
      readonly at: number;
    }
  | { readonly _tag: "RampTick"; readonly at: number }
  | { readonly _tag: "ProbeTriggered"; readonly sequence: number }
  | { readonly _tag: "RedriveTriggered"; readonly sequence: number }
  /** Un-marks the sequence so the redelivered trigger is acted on, not deduped. */
  | { readonly _tag: "TriggerFailed"; readonly election: "probe" | "redrive"; readonly sequence: number }
  /** On a timer: dead letters that arrive while CLOSED would otherwise never be replayed. */
  | { readonly _tag: "SweepTick"; readonly isFloor: boolean };

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

    // Only while CLOSED: every other state is a level, not a ramp.
    RampTick: (command): Transition => ({
      next:
        state.circuit === State.CLOSED
          ? { ...state, policy: step(state.policy, state.circuit, command.at) }
          : state,
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

    // No sequence, no dedupe, no state change.
    SweepTick: (command): Transition => ({
      next: state,
      actions:
        redriveOnClose && state.circuit === State.CLOSED && command.isFloor
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
): Connections => ({
  // In HALF_OPEN only the elected prober calls; a low hash position must not race it.
  work: state.circuit !== State.HALF_OPEN && runsWork(state.policy, self),
  // Leaving HALF_OPEN or CLOSED retires the probe or the redrive.
  probe: state.circuit === State.HALF_OPEN,
  redrive: state.circuit === State.CLOSED,
});

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
