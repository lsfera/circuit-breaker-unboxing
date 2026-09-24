import { Match, Option as O } from "effect";
import { State, supersedes } from "@egress/domain/Model.ts";
import type { Applied, EventType, Lease } from "@egress/domain/Model.ts";
import { initial as initialPolicy, runsWork, step } from "./DaemonPolicy.ts";
import type { DaemonPolicyState } from "./DaemonPolicy.ts";

/**
 * Everything one daemon decides, in one value, with one function that moves it:
 * which state it is in, which sequences it has already probed and redriven, and
 * which connections should therefore exist.
 *
 * One value rather than four `Ref`s because the invariants between them are real
 * and the handlers that touch them run concurrently from AMQP callbacks — here a
 * transition is atomic. The connections themselves stay out: they are resources,
 * and the "actual" side `plan` compares this against.
 */
export type DaemonState = {
  readonly circuit: State;
  /**
   * The event `circuit` came from. A stale one — a paused leader resuming, a
   * duplicate, an out-of-order delivery — is ignored rather than applied.
   */
  readonly applied: O.Option<Applied>;
  readonly policy: DaemonPolicyState;
  /**
   * Highest circuit sequence this daemon has already probed for.
   *
   * Every daemon publishes a probe trigger on entering HALF_OPEN so the trigger
   * still arrives when some are down; SAC delivers all of them to the one
   * elected consumer, and this is what turns "several triggers" back into "one
   * probe per transition".
   */
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
  /**
   * Fired on a timer, not a transition — the only command with no sequence to
   * dedupe on. Messages dead-letter while the circuit stays CLOSED too: a
   * broker restart advances `x-delivery-count` on outstanding deliveries, an
   * Envoy 503 counts as a failure, a rolling redeploy churns consumers. None
   * of that is a recovery, so nothing else would ever replay them.
   */
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
};

export const reduce = (
  state: DaemonState,
  command: Command,
  redriveOnClose: boolean,
): Transition =>
  Match.valueTags(command, {
    CircuitChanged: (command): Transition => {
      if (!supersedes(state.applied, command)) return { next: state, actions: [] };
      const next: DaemonState = {
        ...state,
        applied: O.some({ lease: command.lease, sequence: command.sequence }),
        circuit: command.state,
        policy: step(state.policy, command.state, command.at),
      };
      const actions: Action[] = [];
      if (command.state === State.HALF_OPEN) {
        actions.push({ _tag: "PublishProbeTrigger", sequence: command.sequence });
      }
      // Only on the actual transition back into CLOSED. The aggregator's
      // periodic snapshots repeat the current state, and a redrive per snapshot
      // would replay the queue every fifteen seconds forever.
      if (redriveOnClose && command.state === State.CLOSED && state.circuit !== State.CLOSED) {
        actions.push({ _tag: "PublishRedriveTrigger", sequence: command.sequence });
      }
      return { next, actions };
    },

    RampTick: (command): Transition => {
      // Only while CLOSED: every other state is a level, not a ramp.
      if (state.circuit !== State.CLOSED) return { next: state, actions: [] };
      return {
        next: { ...state, policy: step(state.policy, state.circuit, command.at) },
        actions: [],
      };
    },

    ProbeTriggered: (command): Transition => {
      if (command.sequence <= state.probedSequence) return { next: state, actions: [] };
      return {
        next: { ...state, probedSequence: command.sequence },
        actions: [{ _tag: "Probe" }],
      };
    },

    RedriveTriggered: (command): Transition => {
      if (command.sequence <= state.redrivenSequence) return { next: state, actions: [] };
      return {
        next: { ...state, redrivenSequence: command.sequence },
        actions: [{ _tag: "Redrive" }],
      };
    },

    // No sequence, so no dedupe and no state change — a sweep either finds the
    // circuit CLOSED and itself the floor right now, or it doesn't, and the next
    // one thirty seconds later decides fresh. `redriveOnce` is what makes a
    // sweep that finds nothing to do, or one that overlaps a pass already
    // running, harmless — see Redrive.ts.
    SweepTick: (command): Transition => ({
      next: state,
      actions:
        redriveOnClose && state.circuit === State.CLOSED && command.isFloor
          ? [{ _tag: "Redrive" }]
          : [],
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
  // HALF_OPEN is the one state where a daemon must not decide for itself. The
  // one call it permits belongs to whichever daemon the broker elected, and a
  // daemon low enough in the hash space would otherwise self-activate and race
  // it — two calls for a state whose entire contract is "exactly one".
  work: state.circuit !== State.HALF_OPEN && runsWork(state.policy, self),
  // A probe connection only ever belongs to HALF_OPEN, and a redrive only to
  // CLOSED. Leaving either state retires the connection: replaying a backlog
  // into an upstream that has just started failing again is the one thing this
  // whole design exists to prevent.
  probe: state.circuit === State.HALF_OPEN,
  redrive: state.circuit === State.CLOSED,
});

type Plan = {
  readonly startWork: boolean;
  readonly stopWork: boolean;
  readonly stopProbe: boolean;
  readonly stopRedrive: boolean;
};

/**
 * The reconciliation, as arithmetic on two records.
 *
 * Note the asymmetry, which is not an oversight: work connections are opened
 * here, probe and redrive connections never are. Those two are opened by
 * whichever daemon the broker elected, in response to a trigger, and this only
 * ever retires them when the state they belong to is gone.
 */
export const plan = (want: Connections, have: Connections): Plan => ({
  startWork: want.work && !have.work,
  stopWork: !want.work && have.work,
  stopProbe: !want.probe && have.probe,
  stopRedrive: !want.redrive && have.redrive,
});
