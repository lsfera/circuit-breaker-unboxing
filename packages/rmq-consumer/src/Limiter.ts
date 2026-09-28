/**
 * How many calls this replica has open at once, learned rather than configured: additive increase, multiplicative
 * decrease, as TCP does. Only `throttled` ("full") shrinks it; a failure says broken, which is the breaker's business.
 * Pure state: `consumer.ts` owns the semaphore this sizes. A float, so an increase can be a fraction of a slot.
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
   * The decrease a call started under; hand it back to `throttled`. Calls already in flight when the limit was
   * exceeded all get the same 429, and shrinking once per 429 would collapse the limit in one round trip.
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
