import { createHash } from "node:crypto";
const h = (id) => parseInt(createHash("sha256").update(id).digest("hex").slice(0, 8), 16) / 0xffffffff;

const trials = 5000;
console.log("Fraction 0.5, sampled over " + trials + " random fleets\n");
console.log("  fleet   median   5th..95th   relative error (95th pct)   ran ZERO");
for (const n of [3, 5, 10, 20, 50, 100]) {
  const counts = [];
  let zero = 0;
  for (let t = 0; t < trials; t++) {
    const c = Array.from({ length: n }, (_, i) => h(`fleet${t}-daemon-${i}`)).filter((v) => v < 0.5).length;
    counts.push(c);
    if (c === 0) zero++;
  }
  counts.sort((a, b) => a - b);
  const lo = counts[Math.floor(trials * 0.05)], hi = counts[Math.floor(trials * 0.95)];
  const rel = Math.max(Math.abs(hi - n / 2), Math.abs(lo - n / 2)) / (n / 2);
  console.log(
    `  ${String(n).padStart(5)}${String(counts[trials >> 1]).padStart(9)}` +
    `${(lo + ".." + hi).padStart(12)}${(" ±" + (rel * 100).toFixed(0) + "%").padStart(28)}` +
    `${((zero / trials) * 100).toFixed(1).padStart(11)}%`,
  );
}

console.log("\nWith a floor of one — the SAC-elected daemon always runs when the target is non-zero\n");
console.log("  fleet   median   5th..95th   ran ZERO");
for (const n of [3, 5, 10]) {
  const counts = [];
  let zero = 0;
  for (let t = 0; t < trials; t++) {
    const vs = Array.from({ length: n }, (_, i) => h(`fleet${t}-daemon-${i}`));
    const c = Math.max(1, vs.filter((v) => v < 0.5).length);
    counts.push(c);
    if (c === 0) zero++;
  }
  counts.sort((a, b) => a - b);
  console.log(
    `  ${String(n).padStart(5)}${String(counts[trials >> 1]).padStart(9)}` +
    `${(counts[Math.floor(trials * 0.05)] + ".." + counts[Math.floor(trials * 0.95)]).padStart(12)}` +
    `${((zero / trials) * 100).toFixed(1).padStart(11)}%`,
  );
}
