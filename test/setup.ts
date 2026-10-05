import { inject } from "vitest";
import { runtime } from "./runtime.ts";

/**
 * Runs in every test worker before its test file. A runner started by one runtime can still put its workers on
 * another — `bunx vitest` without `--bun` runs them on node — and a suite that passes there says nothing about the
 * runtime it was meant for. Failing here makes a mismatched run fail rather than pass for the wrong runtime.
 */
const expected = inject("runtime");
if (runtime !== expected) {
  throw new Error(`test worker runs on ${runtime}, but the run is for ${expected}`);
}
