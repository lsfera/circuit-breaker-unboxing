import { NodeHttpServer, NodeRuntime, NodeServices } from "@effect/platform-node";
import { VERSION } from "@egress/config/Settings.ts";
import { Effect } from "effect";
import { Command } from "effect/cli";
import { createServer } from "node:http";
import { command } from "./index.ts";
import type { Application, Platform } from "./index.ts";

/**
 * The SDK on Node: the `Platform` it needs, and the process's main. Everything else is in `@egress/rmq-producer`,
 * which names no runtime.
 */

export const platform: Platform = {
  httpServer: (port) => NodeHttpServer.layer(createServer, { port })
};

/** Run a command built by `command`, extended or not, as this process's main. */
export const launch = (cmd: Command.Command<string, any, any, unknown, NodeServices.NodeServices>): void =>
  Command.run(cmd, { version: VERSION }).pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain);

/** Run one producer application in this process, until its `main` ends or fails or the broker is lost. */
export const run = <const F extends Command.Command.Config = {}>(
  app: Application<F>,
  options: { readonly name?: string; } = {}
): void => launch(command(app, platform, options));
