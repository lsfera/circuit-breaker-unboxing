/**
 * The concurrent-call limit, learned by AIMD. A breaker can only push or stop;
 * a third party that is merely full answers 429 to the excess, and this pushes
 * exactly as hard as it takes. Only a 429 shrinks it: a 5xx says broken, which
 * is the breaker's business. Pure; daemon.ts owns the semaphore.
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
   * The decrease a call started under. Calls already in flight all get the same
   * 429; shrinking once per 429 would collapse the limit in one round trip.
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
