import { defineConfig } from "vitest/config";
import { runtime } from "./test/runtime.ts";

/**
 * One configuration for every runtime the SDK claims: the same suites run under node (`pnpm test`), Bun
 * (`pnpm test:bun`) and Deno (`pnpm test:deno`). Each project is named for the runtime that loaded this file, so a
 * report says which one it is from, and `test/setup.ts` fails a run whose test workers are not that runtime too.
 *
 * `unit` is what `pnpm run check` runs. `integration` needs Docker (`pnpm run test:rmq`): one RabbitMQ per test file,
 * started by `harness.ts`, and timeouts sized for broker restarts and real delays rather than Vitest's 5 s default.
 */
export default defineConfig({
  test: {
    provide: { runtime },
    setupFiles: ["./test/setup.ts"],
    projects: [
      {
        extends: true,
        test: {
          name: `unit:${runtime}`,
          include: ["packages/*/test/*.test.ts"]
        }
      },
      {
        extends: true,
        test: {
          name: `integration:${runtime}`,
          include: ["packages/*/test/integration/*.test.ts"],
          testTimeout: 180_000,
          hookTimeout: 120_000
        }
      }
    ]
  }
});
