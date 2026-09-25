import { Match, Option as O } from "effect";
import { classifySequence, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import type { EventType } from "@egress/domain/Model.ts";

/** The delivery contract, observed over AMQP; the rule is the shared `classifySequence`. */

export type ContractState = {
  /** `None` until the first `state_changed`: joining mid-incident is not a gap. */
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
  eventType: EventType,
  sequence: number,
): ContractState => {
  if (eventType !== SEQUENCED_EVENT) return self;
  return Match.value(classifySequence(self.lastSequence, sequence)).pipe(
    // The only verdict that does not move the mark: a stale event must not
    // drag it backwards and turn the next live one into a gap.
    Match.when("duplicate", () => ({ ...self, duplicates: self.duplicates + 1 })),
    Match.when("gap", () => ({ ...self, lastSequence: O.some(sequence), gaps: self.gaps + 1 })),
    Match.when(Match.is("first", "next"), () => ({ ...self, lastSequence: O.some(sequence) })),
    Match.exhaustive,
  );
};
