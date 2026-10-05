/** The JavaScript runtime this process is: the ones the SDK is tested on, told apart by the globals only they define. */
export type Runtime = "node" | "bun" | "deno";

export const runtime: Runtime = "Deno" in globalThis ? "deno" : "Bun" in globalThis ? "bun" : "node";

declare module "vitest" {
  export interface ProvidedContext {
    /** The runtime that loaded the configuration, which every test worker must also be. */
    runtime: Runtime;
  }
}
