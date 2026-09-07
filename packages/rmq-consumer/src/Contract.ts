/**
 * The delivery contract, observed from the consumer's side of the broker.
 *
 * `/api/subscriber` checks the same property over HTTP from inside the
 * publishing process; this checks it over AMQP from five processes the publisher
 * does not control, which is what makes the two independent evidence.
 */

export type ContractState = {
  /**
   * `-1` until the first `state_changed` arrives. A daemon that starts
   * mid-incident legitimately joins the sequence part-way through, and
   * calling that a gap would make the metric lie on every restart.
   */
  readonly lastSequence: number;
  readonly gaps: number;
  readonly duplicates: number;
};

export const initialContract: ContractState = {
  lastSequence: -1,
  gaps: 0,
  duplicates: 0,
};

/** The event type that carries the guarantee. Snapshots deliberately repeat the current sequence, so they are exempt. */
export const SEQUENCED_EVENT = "egress.circuit.state_changed";

/**
 * Fold one control-plane event into the observation.
 *
 * `<=` rather than `===` for duplicates is deliberate: a sequence that goes
 * *backwards* reuses a number just as surely as one that repeats it, and a
 * leadership bug produces exactly that — an instance resuming from stale
 * in-memory state republishes numbers a later leader already used.
 */
export const observe = (
  self: ContractState,
  eventType: string,
  sequence: number,
): ContractState => {
  if (eventType !== SEQUENCED_EVENT) return self;
  if (self.lastSequence < 0) return { ...self, lastSequence: sequence };
  if (sequence <= self.lastSequence) {
    return { ...self, duplicates: self.duplicates + 1 };
  }
  return {
    ...self,
    lastSequence: sequence,
    gaps: sequence > self.lastSequence + 1 ? self.gaps + 1 : self.gaps,
  };
};
