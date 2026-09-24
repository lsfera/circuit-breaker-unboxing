/**
 * How many calls this replica lets itself have open at once, learned from
 * the third party instead of configured: additive increase, multiplicative
 * decrease — the rule TCP uses to find a link's capacity without being told
 * it.
 *
 * A breaker answers "is it broken?" with two states, and a third party that
 * is merely *full* answers 429 to exactly the calls that exceeded what it can
 * serve while serving the rest. The breaker's only responses to that are to
 * keep pushing or to stop everything. This is the third one: push exactly as
 * hard as it will take. It reacts only to an explicit "slow down" (`429`) — a
 * 5xx says broken, which is the breaker's business, and shrinking the limit
 * on a coin-flip failure that has nothing to do with load would only throw
 * capacity away (see README.md).
 *
 * Pure state, no clock and no I/O: `consumer.ts` owns the semaphore this
 * sizes. The limit is a float so the increase can be a fraction of a slot.
 */

export type LimiterConfig = {
  /** Never below this, so a replica always makes some calls and can see the third party recover. */
  readonly min: number;
  /** Never above this — the configured `maxInFlight`, which is also where it starts. */
  readonly max: number;
  /** What the limit is multiplied by when the third party says slow down, in (0, 1). */
  readonly decrease: number;
};

export class AdaptiveLimit {
  private readonly cfg: LimiterConfig;
  private current: number;
  private era = 0;

  constructor(cfg: LimiterConfig) {
    this.cfg = cfg;
    this.current = cfg.max;
  }

  /** Whole slots the semaphore should hold. */
  get slots(): number {
    return Math.max(this.cfg.min, Math.floor(this.current));
  }

  /**
   * Which decrease a call was started under; hand it back to `throttled`.
   * A burst of calls that were all already in flight when the limit was
   * exceeded all get the same 429, and shrinking once per 429 would collapse
   * the limit to `min` in a single round trip. Only a call started after the
   * last decrease can vouch that the *new* limit is still too high.
   */
  get epoch(): number {
    return this.era;
  }

  /** One slot's worth of confidence per limit's-worth of successes: +1 per round trip. */
  succeeded(): void {
    this.current = Math.min(this.cfg.max, this.current + 1 / this.current);
  }

  throttled(startedIn: number): void {
    if (startedIn !== this.era) return;
    this.era++;
    this.current = Math.max(this.cfg.min, this.current * this.cfg.decrease);
  }
}
