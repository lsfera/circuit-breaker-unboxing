// A per-process breaker library against this repo's fleet shape.
// Five daemons on one API, each with its own breaker, sharing nothing.
// Parameters mirror docs/operations.md so the comparison is like-for-like:
// open for 4s, three consecutive successes to close.

const FLEET = 5, RATE = 10, SECONDS = 300, K = 5, OPEN_MS = 4000, PROBE_SUCCESSES = 3;

const rng = (seed) => () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

const run = (p, seed) => {
  const rand = rng(seed);
  const d = Array.from({ length: FLEET }, () => ({
    state: "CLOSED", consecutiveFails: 0, successStreak: 0, openedAt: 0,
  }));
  let reachedUpstream = 0, failedCalls = 0, probes = 0;
  const statesSeen = [];

  for (let t = 0; t < SECONDS * 1000; t += 1000 / RATE) {
    for (const b of d) {
      if (b.state === "OPEN") {
        if (t - b.openedAt < OPEN_MS) continue;      // short-circuited, no call
        b.state = "HALF_OPEN";
        probes++;                                     // every daemon probes alone
      }
      // A call is actually made.
      reachedUpstream++;
      const failed = rand() < p;
      if (failed) failedCalls++;

      if (b.state === "HALF_OPEN") {
        if (failed) { b.state = "OPEN"; b.openedAt = t; b.successStreak = 0; }
        else if (++b.successStreak >= PROBE_SUCCESSES) { b.state = "CLOSED"; b.consecutiveFails = 0; }
        continue;
      }
      if (failed) {
        if (++b.consecutiveFails >= K) { b.state = "OPEN"; b.openedAt = t; b.consecutiveFails = 0; }
      } else b.consecutiveFails = 0;
    }
    if (t % 1000 === 0) statesSeen.push(new Set(d.map((b) => b.state)).size);
  }

  const unanimous = statesSeen.filter((n) => n === 1).length / statesSeen.length;
  return { reachedUpstream, failedCalls, probes, unanimous };
};

console.log("failure  calls reaching   failed calls   fleet unanimous   probes");
console.log("  rate    the upstream                    (% of ticks)   in 5 min");
for (const p of [0.2, 0.45, 0.8, 1.0]) {
  const r = run(p, 42);
  console.log(
    `  ${String(Math.round(p * 100)).padStart(3)}%` +
    `${String(r.reachedUpstream).padStart(15)}` +
    `${String(r.failedCalls).padStart(15)}` +
    `${(r.unanimous * 100).toFixed(0).padStart(16)}%` +
    `${String(r.probes).padStart(11)}`,
  );
}
