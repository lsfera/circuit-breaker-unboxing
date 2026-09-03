import { Effect, Layer } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { randomUUID } from "node:crypto";
import { RmqLive } from "@egress/rmq/Client.ts";
import { runDaemon } from "./daemon.ts";
import { runProducer } from "./producer.ts";

/**
 * Role dispatch for the RabbitMQ side of the scenario:
 *
 *   node --experimental-strip-types src/main.ts daemon
 *   node --experimental-strip-types src/main.ts producer
 *
 * Everything else comes from the environment rather than flags, because in
 * this repo these are containers, not commands someone types — see the
 * rmq-daemon-* / rmq-producer services in docker-compose.yml. DAEMON_INDEX
 * is the one value that differs between the otherwise identical daemon
 * containers.
 */

const role = process.argv[2] ?? "daemon";

const env = (name: string, fallback: string) => process.env[name] ?? fallback;

const rmqAddr = env("RMQ", "127.0.0.1:5672");
const [rmqHost, rmqPort] = rmqAddr.split(":");
const connect = { host: rmqHost ?? "127.0.0.1", port: Number(rmqPort ?? 5672) };

const apiId = env("API_ID", "payments-provider");

const program =
  role === "producer"
    ? runProducer({
        apiId,
        ratePerSecond: Number(env("RATE_PER_SECOND", "200")),
      })
    : runDaemon({
        apiId,
        index: Number(env("DAEMON_INDEX", "0")),
        fleetSize: Number(env("FLEET_SIZE", "5")),
        instanceId: env("INSTANCE_ID", randomUUID()),
        connect,
        // One address, no replica names — the same string a real client of
        // this API would be configured with.
        egressAddr: env("EGRESS_ADDR", "http://envoy:10000"),
        apiPath: env("API_PATH", "/payments"),
        maxInFlight: Number(env("MAX_IN_FLIGHT", "32")),
      });

if (role !== "producer" && role !== "daemon") {
  console.error(`unknown role "${role}" — expected "daemon" or "producer"`);
  process.exit(1);
}

/**
 * Containment for exactly one library-level race, and nothing else.
 *
 * rhea throws `transfer after detach` synchronously from inside a socket
 * data callback when the broker's frames arrive for a link that has just
 * gone away. `daemon.ts` avoids provoking it (see the two-close comment in
 * `probeOnce`), but a connection torn down while frames are in flight can
 * still hit it, and the throw is unreachable from here: the client creates
 * a private rhea container per connection and never exposes it, so there is
 * no `error` listener to attach. Left alone it kills the process.
 *
 * Every other uncaught exception is still fatal, on purpose — a daemon that
 * swallows its own bugs is worse than one that restarts.
 */
process.on("uncaughtException", (error) => {
  if (error instanceof Error && error.message === "transfer after detach") {
    console.warn(`[${role}] ignored rhea race: transfer after detach`);
    return;
  }
  throw error;
});

NodeRuntime.runMain(Effect.scoped(Effect.provide(program, RmqLive(connect))));
