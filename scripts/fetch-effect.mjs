/**
 * Puts Effect's source, at the version this repository runs, in repos/effect: the reference AGENTS.md sends every
 * question about an Effect API to. It is not committed — 4,000 files of someone else's repository, which the tag
 * alone is enough to get back.
 *
 *   node scripts/fetch-effect.mjs
 *
 * The version is the catalog's `effect` in pnpm-workspace.yaml. A copy already at that version is left alone; any
 * other is replaced by a shallow clone of the tag `effect@<version>`.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, rmSync } from "node:fs";

const TARGET = "repos/effect";

// The catalog is a flat `name: version` block; nothing else in the file is read.
const catalogBlock = /^catalog:\n((?:[ \t]+.+\n?)*)/m.exec(readFileSync("pnpm-workspace.yaml", "utf8"))?.[1] ?? "";
const version = [...catalogBlock.matchAll(/^[ \t]+["']?([^"':\s]+)["']?:\s*["']?([^"'\s#]+)/gm)].find(
  (m) => m[1] === "effect",
)?.[2];
if (!version) {
  console.error("pnpm-workspace.yaml's catalog has no entry for effect");
  process.exit(1);
}

const manifest = `${TARGET}/packages/effect/package.json`;
const have = existsSync(manifest) ? JSON.parse(readFileSync(manifest, "utf8")).version : undefined;
if (have === version) {
  console.log(`${TARGET} is effect@${version}`);
  process.exit(0);
}

console.log(have ? `${TARGET} is effect@${have}, moving it to effect@${version}` : `fetching effect@${version} into ${TARGET}`);
rmSync(TARGET, { recursive: true, force: true });
execFileSync(
  "git",
  ["clone", "--quiet", "--depth", "1", "--branch", `effect@${version}`, "https://github.com/Effect-TS/effect.git", TARGET],
  { stdio: "inherit" },
);
// A plain copy, not a nested repository an editor offers to commit to.
rmSync(`${TARGET}/.git`, { recursive: true, force: true });
console.log(`${TARGET} is effect@${version}`);
