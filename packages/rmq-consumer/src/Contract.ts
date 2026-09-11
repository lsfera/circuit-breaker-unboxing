import { Option as O } from "effect";
import { classifySequence, SEQUENCED_EVENT } from "@egress/domain/Model.ts";

/**
 * The delivery contract, observed from the consumer's side of the broker.
 *
 * `/api/subscriber` checks the same property over HTTP from inside the
 * publishing process; this checks it over AMQP from five processes the
 * publisher does not control. The vantage points are what make the two
 * independent — the rule itself is `classifySequence`, shared, because two
 * observers of one guarantee disagreeing about what a violation is would make
 * both readings worthless rather than corroborating.
 */

export type ContractState = {
  /**
   * `None` until the first `state_changed` arrives. A daemon that starts
   * mid-incident legitimately joins the sequence part-way through, and
   * calling that a gap would make the metric lie on every restart.
   */
  readonly lastSequence: O.Option<number>;
  readonly gaps: number;
  readonly duplicates: number;
};

export const initialContract: ContractState = {
  lastSequence: O.none(),
  gaps: 0,
  duplicates: 0,
};

/** Fold one control-plane event into the observation. */
export const observe = (
  self: ContractState,
  eventType: string,
  sequence: number,
): ContractState => {
  if (eventType !== SEQUENCED_EVENT) return self;
  switch (classifySequence(self.lastSequence, sequence)) {
    case "duplicate":
      // The only verdict that does not move the mark: a stale event must not
      // drag it backwards and turn the next live one into a gap.
      return { ...self, duplicates: self.duplicates + 1 };
    case "gap":
      return { ...self, lastSequence: O.some(sequence), gaps: self.gaps + 1 };
    case "first":
    case "next":
      return { ...self, lastSequence: O.some(sequence) };
  }
};
