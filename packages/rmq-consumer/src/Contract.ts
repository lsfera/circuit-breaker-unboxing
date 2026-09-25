import { Match, Option as O } from "effect";
import { classifyEvent, SEQUENCED_EVENT } from "@egress/domain/Model.ts";
import type { Applied, EventType, Lease } from "@egress/domain/Model.ts";

/** The delivery contract, observed over AMQP; the rule is the shared `classifyEvent`. */

export type ContractState = {
  /** `None` until the first `state_changed`: joining mid-incident is not a gap. */
  readonly last: O.Option<Applied>;
  readonly gaps: number;
  readonly duplicates: number;
};

export const initialContract: ContractState = {
  last: O.none(),
  gaps: 0,
  duplicates: 0,
};

/** Fold one control-plane event into the observation. Snapshots repeat the current sequence by design, so only `state_changed` counts. */
export const observe = (
  self: ContractState,
  eventType: EventType,
  lease: O.Option<Lease>,
  sequence: number,
): ContractState => {
  const mark = { ...self, last: O.some({ lease, sequence }) };
  return eventType !== SEQUENCED_EVENT
    ? self
    : Match.value(classifyEvent(self.last, { lease, sequence })).pipe(
      // The only verdicts that do not move the mark: a stale event must not
      // drag it backwards and turn the next live one into a gap.
      Match.when(Match.is("duplicate", "stale"), () => ({ ...self, duplicates: self.duplicates + 1 })),
      Match.when("gap", () => ({ ...mark, gaps: self.gaps + 1 })),
      // A new epoch restarts the sequences: a fresh start, like joining.
      Match.when(Match.is("first", "next", "new-epoch"), () => mark),
      Match.exhaustive,
    );
};
