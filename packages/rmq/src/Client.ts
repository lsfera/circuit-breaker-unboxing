import { Context, Data, Effect, Layer, Semaphore } from "effect";
import { createEnvironment } from "rabbitmq-amqp-js-client";
import type { Connection, Consumer, Publisher } from "rabbitmq-amqp-js-client";

/** Re-exported so downstream packages (@egress/aggregator, @egress/rmq-consumer) never need their own direct dependency on the underlying client library. */
export type { Consumer, Publisher };

/**
 * Opaque handles: rabbitmq-amqp-js-client's `Exchange`/`Queue` types exist
 * internally (src/exchange.ts, src/queue.ts) but are not part of its public
 * `dist/index.d.ts` export surface, so they can't be named here. Every
 * caller only ever passes these straight back into `bind`, never inspects
 * their shape, so opacity costs nothing.
 */
export type RmqExchange = unknown;
export type RmqQueue = unknown;

/**
 * Effect wrapper over rabbitmq-amqp-js-client (AMQP 1.0, RabbitMQ 4.x
 * native — not the AMQP 0-9-1 a library like amqplib speaks), in the same
 * Context.Service / Layer.effect / Data.TaggedError shape as
 * @egress/aggregator's FleetSource.ts and Coordination.ts. Every claim this
 * module leans on (x-single-active-consumer election and promotion, a
 * closed consumer stopping delivery without touching the connection, a
 * fixed exchange+routingKey publisher landing on the correctly topic-bound
 * queue) was verified against a real RabbitMQ 4.x container before being
 * written here — see docs/rmq-control-plane.md.
 *
 * ## Why every operation is serialized
 *
 * Opening links concurrently on one connection is broken in this client,
 * verified two ways against a real broker:
 *
 * - Three `createPublisher` calls in flight at once, each for a different
 *   (exchange, routingKey): every message published afterwards landed on
 *   the *first* publisher's queue.
 * - Three `createConsumer` calls in flight at once, each for a different
 *   queue: all three consumers received the *first* queue's messages.
 *
 * Sequential creation is correct in both cases; only concurrency breaks it,
 * which points at a race in link setup on the shared connection rather than
 * anything about addresses or routing. That is easy to hit by accident —
 * the aggregator's very first tick reports on every API at once, and a
 * daemon fleet starts N consumers at once — and it fails *silently*, with
 * plausible-looking traffic going to the wrong place.
 *
 * So the guard lives here, in the one place that owns the connection,
 * rather than at each call site: a single permit serializing every
 * operation that touches it. Callers get a connection that is safe to share
 * across concurrent fibers by construction, which is what lets the daemon
 * fleet run N consumers over one connection at all. The cost is nil at this
 * repo's volumes (a handful of control-plane events per incident), and
 * correctness here is not the place to trade for throughput.
 */

export class RmqError extends Data.TaggedError("RmqError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

export type QueueArgs = Record<string, unknown>;

export class Rmq extends Context.Service<
  Rmq,
  {
    /** durable: false, exclusive: false — every queue this repo declares is a demo fixture, not durable state. */
    readonly declareQueue: (name: string, args?: QueueArgs) => Effect.Effect<RmqQueue, RmqError>;
    readonly declareTopicExchange: (name: string) => Effect.Effect<RmqExchange, RmqError>;
    readonly bind: (
      routingKey: string,
      source: RmqExchange,
      destination: RmqQueue,
    ) => Effect.Effect<void, RmqError>;
    readonly consume: (
      queue: string,
      onMessage: (body: string) => void,
    ) => Effect.Effect<Consumer, RmqError>;
    /** One publisher per fixed (exchange, routingKey) or (queue) target — see Client.ts's module doc for why this is a publisher-per-target library, not per-message addressing. */
    readonly publisherToExchange: (
      exchange: string,
      routingKey: string,
    ) => Effect.Effect<Publisher, RmqError>;
    readonly publisherToQueue: (queue: string) => Effect.Effect<Publisher, RmqError>;
    readonly send: (pub: Publisher, body: string) => Effect.Effect<void, RmqError>;
    readonly closeConsumer: (c: Consumer) => Effect.Effect<void>;
  }
>()("Rmq") {}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

export type RmqConnectOptions = {
  readonly host: string;
  readonly port: number;
  readonly username?: string;
  readonly password?: string;
};

/** One real AMQP 1.0 connection per layer instance, closed when the layer's scope ends. */
export const RmqLive = (opts: RmqConnectOptions) =>
  Layer.effect(
    Rmq,
    Effect.gen(function* () {
      const env = createEnvironment({
        host: opts.host,
        port: opts.port,
        username: opts.username ?? "guest",
        password: opts.password ?? "guest",
      });
      const connection: Connection = yield* Effect.acquireRelease(
        wrap("connect", () => env.createConnection()),
        () => Effect.promise(() => env.close()),
      );
      const management = connection.management();

      // The one permit guarding this connection. See the module doc above:
      // concurrent link creation on a shared connection silently misroutes
      // in this client, for consumers as well as publishers.
      const gate = yield* Semaphore.make(1);
      const guarded = <A>(operation: string, promise: () => Promise<A>) =>
        gate.withPermit(wrap(operation, promise));

      return {
        declareQueue: (name, args = {}) =>
          guarded("declareQueue", () =>
            management.declareQueue(name, {
              exclusive: false,
              durable: false,
              // The library's own type narrows `arguments` to
              // Record<string,string>, but it only ever spreads this object
              // verbatim into the AMQP declare body — non-string values
              // (e.g. x-single-active-consumer: true) pass through fine at
              // runtime. Checked directly against a live broker, not
              // assumed — see docs/rmq-control-plane.md.
              arguments: args as Record<string, string>,
            } as never),
          ),
        declareTopicExchange: (name) =>
          guarded("declareExchange", () =>
            management.declareExchange(name, { type: "topic", durable: false }),
          ),
        bind: (routingKey, source, destination) =>
          guarded("bind", () => management.bind(routingKey, { source, destination } as never)).pipe(
            Effect.asVoid,
          ),
        consume: (queue, onMessage) =>
          guarded("consume", async () => {
            const consumer = await connection.createConsumer({
              queue: { name: queue },
              messageHandler: (ctx, message) => {
                onMessage(String(message.body));
                ctx.accept();
              },
            });
            consumer.start();
            return consumer;
          }),
        publisherToExchange: (exchange, routingKey) =>
          guarded("publisherToExchange", () =>
            connection.createPublisher({ exchange: { name: exchange, routingKey } }),
          ),
        publisherToQueue: (queue) =>
          guarded("publisherToQueue", () => connection.createPublisher({ queue: { name: queue } })),
        send: (pub, body) => guarded("send", () => pub.publish({ body } as never)).pipe(Effect.asVoid),
        closeConsumer: (c) => Effect.sync(() => c.close()),
      };
    }),
  );
