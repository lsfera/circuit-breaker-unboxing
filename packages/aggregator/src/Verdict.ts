/**
 * The pure heart of the aggregator: turn what's known about a fleet into one
 * verdict. No broker, no clock service, no Effect — a total function of a
 * registry and a threshold, the same "pull the decision out, test it alone"
 * shape as `packages/consumer/src/consumer.ts`'s `decide`.
 */

export type ReplicaState = "closed" | "open" | "half_open" | "isolated";

const REPLICA_STATES: ReadonlySet<string> = new Set<ReplicaState>([
  "closed",
  "open",
  "half_open",
  "isolated",
]);

/** A real membership check against the four states a replica can actually publish — not a bare `typeof`, which lets any string through as if it were a known state. */
export const isReplicaState = (x: string): x is ReplicaState => REPLICA_STATES.has(x);

/** The `circuit.control` wire shape every replica publishes on its own breaker's `onStateChange`. */
export type ReplicaEvent = {
  readonly apiId: string;
  readonly instance: string;
  readonly state: ReplicaState;
  readonly at: number;
};

/** What the aggregator currently believes about one apiId's fleet: one entry per instance heard from. */
export type ApiRegistry = ReadonlyMap<string, { readonly state: ReplicaState; readonly at: number }>;

export type Verdict = "open" | "closed";

/**
 * Instances not heard from inside `stalenessMs` are dropped rather than
 * counted. Without this, a replica that's scaled down or crashed keeps
 * casting its last vote forever — the fraction below would only ever grow
 * more open over the life of the process, never recover, once enough
 * replicas had come and gone.
 */
export const prune = (registry: ApiRegistry, now: number, stalenessMs: number): ApiRegistry =>
  new Map([...registry].filter(([, v]) => now - v.at <= stalenessMs));

/**
 * Share of a (already pruned) registry currently `open` or `half_open`.
 * Half-open counts as not-yet-healthy on purpose: a breaker that hasn't
 * closed again isn't serving normal traffic either, and folding it into
 * "closed" would make the published verdict recover before any replica
 * actually has.
 */
export const openFraction = (registry: ApiRegistry): number => {
  if (registry.size === 0) return 0;
  let open = 0;
  for (const v of registry.values()) if (v.state === "open" || v.state === "half_open") open++;
  return open / registry.size;
};

/** A fraction and a threshold in, one verdict out — the whole decision, isolated from how the fraction was computed. */
export const verdictFor = (fraction: number, threshold: number): Verdict =>
  fraction >= threshold ? "open" : "closed";

/**
 * Whether a newly-arrived event should overwrite what's already known about
 * its instance. A redelivered or delayed `circuit.control` event carries an
 * `at` no newer than one already folded in — accepting it anyway would
 * regress that instance's tracked state to something older than what the
 * registry already believed, purely because of redelivery timing rather than
 * anything that actually changed at the source.
 */
export const shouldAccept = (
  existing: { readonly at: number } | undefined,
  incoming: { readonly at: number },
): boolean => existing === undefined || incoming.at >= existing.at;
