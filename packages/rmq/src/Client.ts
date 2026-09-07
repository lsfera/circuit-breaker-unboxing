import { Context, Data, Effect, Layer, Scope } from "effect";
import * as amqp from "amqplib";
import type { ChannelModel, ConfirmChannel, ConsumeMessage, Channel } from "amqplib";

/**
 * Opaque handles. `declareQueue`/`declareTopicExchange` hand these straight
 * back to `bind`, and nothing else ever inspects them, so their shape is not
 * part of the contract.
 */
export type RmqExchange = unknown;
export type RmqQueue = unknown;

/**
 * A consumer is a channel plus the tag the broker gave it. One channel per
 * consumer on purpose — see the module doc.
 */
export type Consumer = { readonly channel: Channel; readonly consumerTag: string };

/**
 * A publisher is an address, not a link. In AMQP 0-9-1 publishing takes the
 * exchange and routing key per call, so there is nothing to open, nothing to
 * fail halfway, and nothing to race.
 */
export type Publisher = { readonly exchange: string; readonly routingKey: string };

/**
 * Effect wrapper over amqplib (AMQP 0-9-1), in the same Context.Service /
 * Layer.effect / Data.TaggedError shape as @egress/aggregator's FleetSource.ts
 * and Coordination.ts.
 *
 * ## Why 0-9-1, having started on 1.0
 *
 * This was `rabbitmq-amqp-js-client` (AMQP 1.0, RabbitMQ 4 native) and the
 * decision to keep it was made on measurement — see
 * docs/decisions/001-amqp-client.md. What changed is the evidence, not the
 * taste:
 *
 *  - That client had no commit upstream after 2026-06-25, and its issue #96 —
 *    concurrent `createPublisher` calls resolving with crossed links, ~20,000
 *    misrouted messages for the reporter in production — was still open. It is
 *    the same defect this module used to serialize every operation to avoid.
 *  - Three separate workarounds here were calibrated to that exact build: a
 *    connection-wide semaphore, a two-connection topology per daemon, and an
 *    `uncaughtException` filter for a rhea throw with no reachable listener.
 *  - It exposed no way to bound a consumer's unsettled deliveries. rhea's
 *    default credit window is 1000, which is why closing a probe consumer
 *    stranded so much: the probe wanted *one* message.
 *
 * All three are gone here rather than worked around, which is the point of the
 * move. `prefetch` is a first-class argument, channels isolate failure, and
 * publishing has no link to race. What it costs is RabbitMQ 4's native
 * protocol; what it buys is a client with no dependencies, its own types, and
 * an actual maintainer.
 *
 * ## Channels, not connections
 *
 * The expensive lesson of the 1.0 client was that closing a consumer with
 * deliveries in flight stranded them, and enough strandings stalled *every*
 * link on that connection — so anything that churned consumers needed a
 * connection it could afford to destroy.
 *
 * 0-9-1 has the isolation built in. Every consumer here gets its own channel;
 * a channel that errors takes down nothing but itself, and cancelling a
 * consumer leaves the channel able to settle the delivery it is holding.
 * Declares get a throwaway channel each, so a `PRECONDITION_FAILED` from a
 * redeclare with different arguments — an ordinary thing to hit while
 * changing topology — cannot take the publish path with it.
 */

export class RmqError extends Data.TaggedError("RmqError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

export type QueueArgs = Record<string, unknown>;

/**
 * What a handler asks the broker to do with the delivery it was given.
 *
 * - `accept` — done with it, drop it from the queue (the default; a handler
 *   that returns nothing gets this).
 * - `requeue` — back onto the queue for another consumer or another attempt.
 *   Nothing about it is delayed, so an unbounded requeue on a failing
 *   dependency is a hot loop; bound it.
 * - `discard` — rejected without requeue. On a queue declared with
 *   `x-dead-letter-exchange` that routes the message to the dead-letter queue;
 *   on one without, it is dropped.
 */
export type Settlement = "accept" | "requeue" | "discard";

/** What the broker knows about this particular delivery. */
export type DeliveryInfo = {
  /**
   * How many times this message has been delivered and returned, as the broker
   * counts it — the `x-delivery-count` header a quorum queue stamps, 0 on a
   * first delivery.
   *
   * The 1.0 client reported 0 unconditionally, which is why the redelivery
   * budget had to be the queue's job (`x-delivery-limit`). It still is, and
   * should stay so: an in-process counter dies when the message moves to
   * another daemon. This is now honest for anything that wants to *look*.
   */
  readonly deliveryCount: number;
  /**
   * Where this message was dead-lettered from, when it was — RabbitMQ 4 stamps
   * `x-first-death-queue` and `x-first-death-reason` as headers. `null` for a
   * message that arrived normally.
   *
   * Computed on first access and cached: the high-rate handlers never read it.
   *
   * This is what makes one canonical dead-letter queue workable rather than a
   * bin of unrelated things — anything draining it can tell work that failed
   * its third-party call from a control message that failed to decode.
   */
  readonly deadLetter: { readonly queue: string; readonly reason: string } | null;
  /**
   * Headers carried on the message, as strings.
   *
   * The broker's death annotations are lost the moment anything republishes a
   * message, so a consumer that moves messages around inside the dead-letter
   * queue has to carry the provenance itself. This is where it puts it.
   */
  readonly properties: Readonly<Record<string, string>>;
};

/**
 * How many deliveries a consumer may hold unsettled.
 *
 * There was no equivalent lever on the 1.0 client: rhea's credit window was a
 * fixed 1000, so a consumer opened on a deep queue immediately owed the broker
 * an answer for a thousand messages. Every caller here wants far fewer, and
 * the probe wants exactly one.
 *
 * The default is deliberately above the daemon's `maxInFlight` (32) rather
 * than equal to it: the concurrency gate should be what limits calls, with
 * prefetch as the outer bound that keeps a stalled consumer from holding an
 * unbounded slice of the queue.
 */
export const DEFAULT_PREFETCH = 100;

export interface RmqService {
  /**
   * `durable` decides whether the queue and its contents survive a broker
   * restart, and it is a real decision rather than a default worth inheriting.
   * A transient queue is right for a live subscription that a restarting
   * consumer can rebuild from the next snapshot. It is badly wrong for a queue
   * whose entire purpose is holding work you promised to keep: measured on
   * this stack, a broker restart took a dead-letter queue from 24 preserved
   * messages to zero.
   */
  readonly declareQueue: (
    name: string,
    options?: { readonly args?: QueueArgs; readonly durable?: boolean },
  ) => Effect.Effect<RmqQueue, RmqError>;
  readonly declareTopicExchange: (
    name: string,
    options?: { readonly durable?: boolean },
  ) => Effect.Effect<RmqExchange, RmqError>;
  readonly bind: (
    routingKey: string,
    source: RmqExchange,
    destination: RmqQueue,
  ) => Effect.Effect<void, RmqError>;
  /**
   * The message is settled only once `onMessage` settles. Returning a promise
   * is therefore the flow-control lever: a handler that waits for its own work
   * holds its delivery unacked, and with `prefetch` bounding how many a
   * consumer may hold, the broker stops pushing before the consumer is
   * swamped. A synchronous handler settles immediately and gets no
   * backpressure at all.
   *
   * The returned value chooses the outcome, synchronously or from a promise;
   * returning nothing accepts, which is what every handler that cannot fail
   * wants. A handler that *throws* accepts anyway: the alternative is an
   * unbounded redelivery loop driven by a bug, which is worse than a lost
   * message and much harder to see.
   */
  readonly consume: (
    queue: string,
    onMessage: (
      body: string,
      delivery: DeliveryInfo,
    ) => void | Settlement | Promise<void | Settlement>,
    options?: { readonly prefetch?: number },
  ) => Effect.Effect<Consumer, RmqError>;
  /** One publisher per fixed (exchange, routingKey) or (queue) target. */
  readonly publisherToExchange: (
    exchange: string,
    routingKey: string,
  ) => Effect.Effect<Publisher, RmqError>;
  readonly publisherToQueue: (queue: string) => Effect.Effect<Publisher, RmqError>;
  /**
   * `properties` become message headers — see `DeliveryInfo.properties` for
   * why anything republishing needs them.
   *
   * Every message is published persistent. There is no flag for it because
   * there is no case here for publishing otherwise: on a transient queue the
   * broker ignores it, and on a durable one it is the difference between
   * keeping the message across a restart and only appearing to.
   */
  readonly send: (
    pub: Publisher,
    body: string,
    properties?: Record<string, string>,
  ) => Effect.Effect<void, RmqError>;
  /**
   * Stop delivery to this consumer, leaving its channel able to settle
   * whatever it is still holding.
   *
   * What the HALF_OPEN probe needs: it takes one message, stops the flow from
   * inside the handler, and then settles that message from the call's outcome.
   * Closing the channel instead would make the settlement moot and hand the
   * message back to the queue.
   */
  readonly cancelConsumer: (c: Consumer) => Effect.Effect<void>;
  /**
   * Retire the consumer and its channel outright.
   *
   * Everything the channel is still holding unacked goes back to the queue,
   * which is the point: this is how a daemon abandons work wholesale when the
   * circuit moves under it. It is the operation that used to require dropping
   * a whole connection, back when a stranded delivery could stall every link
   * sharing one.
   */
  readonly closeConsumer: (c: Consumer) => Effect.Effect<void>;
}

export class Rmq extends Context.Service<Rmq, RmqService>()("Rmq") {}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

/**
 * Settling a delivery whose channel has since closed throws
 * `IllegalOperationError`, and with a deferred ack that is not an edge case —
 * it is what happens whenever a consumer is retired while calls are still in
 * flight, which is exactly what `OPEN` does to @egress/rmq-consumer's daemons.
 * The settlement is genuinely moot at that point, because the broker requeues
 * every unacked delivery when the channel goes.
 */
const settle = (channel: Channel, message: ConsumeMessage, outcome: Settlement) => {
  try {
    if (outcome === "discard") channel.nack(message, false, false);
    else if (outcome === "requeue") channel.nack(message, false, true);
    else channel.ack(message);
  } catch {
    // channel already gone; the broker has the delivery back
  }
};

export type RmqConnectOptions = {
  readonly host: string;
  readonly port: number;
  readonly username?: string;
  readonly password?: string;
};

const describe = (delivery: ConsumeMessage): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  let deadLetter: DeliveryInfo["deadLetter"] | undefined;
  let properties: Readonly<Record<string, string>> | undefined;
  return {
    deliveryCount: Number(headers["x-delivery-count"] ?? 0),
    get deadLetter() {
      if (deadLetter === undefined) {
        const queue = headers["x-first-death-queue"];
        const reason = headers["x-first-death-reason"];
        deadLetter =
          typeof queue === "string"
            ? { queue, reason: typeof reason === "string" ? reason : "unknown" }
            : null;
      }
      return deadLetter;
    },
    get properties() {
      if (properties === undefined) {
        properties = Object.fromEntries(
          Object.entries(headers).map(([k, v]) => [k, String(v)]),
        );
      }
      return properties;
    },
  };
};

/**
 * One real AMQP connection, released when the surrounding scope closes.
 *
 * Exposed separately from `RmqLive` because a connection is not always a
 * process-lifetime thing here: @egress/rmq-consumer's daemon opens one per
 * redrive pass and per probe, so that work can be abandoned wholesale.
 */
export const makeRmq = (
  opts: RmqConnectOptions,
): Effect.Effect<RmqService, RmqError, Scope.Scope> =>
  Effect.gen(function* () {
    const connection: ChannelModel = yield* Effect.acquireRelease(
      wrap("connect", () =>
        amqp.connect({
          protocol: "amqp",
          hostname: opts.host,
          port: opts.port,
          username: opts.username ?? "guest",
          password: opts.password ?? "guest",
        }),
      ),
      // Swallowed deliberately: this runs on every scope close, and the daemon
      // closes connections constantly. A broker that has already gone makes
      // `close` reject, and a rejection here would fail the teardown rather
      // than complete it.
      (conn) => Effect.promise(() => conn.close().then(() => {}, () => {})),
    );

    // amqplib emits 'error' on the connection and on every channel. An
    // EventEmitter 'error' with no listener is rethrown by Node, from inside a
    // socket callback where nothing can catch it — which is precisely how the
    // previous client used to take the process down. One listener per emitter
    // is what makes a broken connection an observable event instead.
    connection.on("error", (error) => {
      console.warn(`[rmq] connection error: ${error.message}`);
    });

    /**
     * Publishing shares one channel. Ordering per (exchange, routingKey) is
     * what the event contract needs, and a single channel gives exactly that;
     * a channel per publisher would interleave.
     *
     * It is a *confirm* channel, which is what makes `send` mean "the broker
     * has this" rather than "this reached a socket". Without confirms an
     * amqplib publish resolves as soon as the frame is written, so a broker
     * that dropped the message would look identical to one that stored it —
     * and `Redrive.ts` leans on the difference: it publishes work back onto
     * the work queue and only then acks it off the dead-letter queue, which is
     * a guarantee about losses, not about socket writes.
     *
     * Recreated on demand, because one channel with no recovery is a single
     * point of failure. amqplib closes a channel on any channel-level error —
     * publishing to an exchange that does not exist is enough — so without
     * this, one such error ends publishing from this process for good while
     * every other health signal stays green, which is this repo's least
     * favourite shape of failure. Opened eagerly all the same, so a broker
     * that cannot give us a channel fails the layer at startup rather than at
     * the first publish.
     */
    let out: ConfirmChannel | null = yield* wrap("createChannel", () =>
      connection.createConfirmChannel(),
    );
    let opening: Promise<ConfirmChannel> | null = null;

    const watchPublishChannel = (ch: ConfirmChannel) => {
      ch.on("error", (error) => {
        console.warn(`[rmq] publish channel error: ${error.message}`);
      });
      // 'close' follows 'error', and also fires on a close nothing here asked
      // for. Dropping the reference is what makes the next publish reopen.
      ch.on("close", () => {
        if (out === ch) out = null;
      });
      return ch;
    };
    watchPublishChannel(out);

    /** The live publish channel, opening one if the last was closed under us. */
    const publishChannel = (): Promise<ConfirmChannel> => {
      if (out !== null) return Promise.resolve(out);
      // One reopen at a time. Several sends racing here must not each open a
      // channel and leave all but one orphaned on the broker.
      opening ??= connection.createConfirmChannel().then(
        (ch) => {
          out = watchPublishChannel(ch);
          opening = null;
          return ch;
        },
        (error) => {
          opening = null;
          throw error;
        },
      );
      return opening;
    };

    /**
     * Declares run on a throwaway channel each. They happen at startup, so the
     * extra round trip costs nothing, and it means a redeclare whose arguments
     * disagree with the existing queue — `PRECONDITION_FAILED`, which closes
     * the channel it arrives on — cannot take the publish path down with it.
     */
    const onFreshChannel = <A>(operation: string, use: (ch: Channel) => Promise<A>) =>
      wrap(operation, async () => {
        const ch = await connection.createChannel();
        ch.on("error", () => {});
        try {
          return await use(ch);
        } finally {
          await ch.close().catch(() => {});
        }
      });

    /**
     * Resolves when the broker has confirmed the message, and not before.
     *
     * This is also the backpressure: a caller that awaits its own confirm
     * cannot outrun the broker, which is a better bound than watching for
     * 'drain' and a good deal simpler — there is no wait to leave parked.
     *
     * The wait still has to end if the channel dies, because a channel that
     * closes will never confirm, and this promise is awaited from inside
     * message handlers: a send that never settles is a delivery that never
     * settles.
     */
    const publish = (pub: Publisher, content: Buffer, options: amqp.Options.Publish) =>
      publishChannel().then(
        (ch) =>
          new Promise<void>((resolve, reject) => {
            const finish = (outcome: () => void) => {
              ch.removeListener("close", onClose);
              outcome();
            };
            const onClose = () =>
              finish(() => reject(new Error("publish channel closed before the broker confirmed")));
            ch.once("close", onClose);
            ch.publish(pub.exchange, pub.routingKey, content, options, (error) =>
              error ? finish(() => reject(error)) : finish(resolve),
            );
          }),
      );

    return {
      declareQueue: (name, options = {}) =>
        onFreshChannel("declareQueue", async (ch) => {
          await ch.assertQueue(name, {
            durable: options.durable ?? false,
            exclusive: false,
            arguments: options.args ?? {},
          });
          return name as RmqQueue;
        }),
      declareTopicExchange: (name, options = {}) =>
        onFreshChannel("declareExchange", async (ch) => {
          await ch.assertExchange(name, "topic", { durable: options.durable ?? false });
          return name as RmqExchange;
        }),
      bind: (routingKey, source, destination) =>
        onFreshChannel("bind", async (ch) => {
          await ch.bindQueue(destination as string, source as string, routingKey);
        }).pipe(Effect.asVoid),
      consume: (queue, onMessage, options = {}) =>
        wrap("consume", async () => {
          // Its own channel: a consumer that errors, or one that is cancelled
          // with deliveries outstanding, must not touch any other.
          const ch = await connection.createChannel();
          ch.on("error", (error) => {
            console.warn(`[rmq] consumer channel error on ${queue}: ${error.message}`);
          });
          await ch.prefetch(options.prefetch ?? DEFAULT_PREFETCH);
          const { consumerTag } = await ch.consume(
            queue,
            (message) => {
              // null means the consumer was cancelled by the broker (the queue
              // was deleted underneath it). There is no delivery to settle.
              if (message === null) return;
              // A handler that throws synchronously would escape into
              // amqplib's delivery callback. Every handler in this repo is
              // careful, which is exactly the kind of thing that stops being
              // true later.
              let done: void | Settlement | Promise<void | Settlement>;
              try {
                done = onMessage(message.content.toString("utf8"), describe(message));
              } catch {
                return settle(ch, message, "accept");
              }
              if (done === undefined) return settle(ch, message, "accept");
              // A synchronous outcome is a string, not a thenable.
              if (typeof done === "string") return settle(ch, message, done);
              void done.then(
                (outcome) => settle(ch, message, outcome ?? "accept"),
                () => settle(ch, message, "accept"),
              );
            },
            { noAck: false },
          );
          return { channel: ch, consumerTag };
        }),
      publisherToExchange: (exchange, routingKey) =>
        Effect.succeed({ exchange, routingKey }),
      publisherToQueue: (queue) =>
        // The default exchange routes by queue name, which is the same path
        // `deadLetterArgs` uses for dead-lettering.
        Effect.succeed({ exchange: "", routingKey: queue }),
      send: (pub, body, properties) =>
        wrap("send", () =>
          publish(
            pub,
            Buffer.from(body, "utf8"),
            properties === undefined
              ? { persistent: true }
              : { persistent: true, headers: properties },
          ),
        ),
      cancelConsumer: (c) =>
        Effect.promise(() => c.channel.cancel(c.consumerTag).then(() => {}, () => {})),
      // Closing is enough on its own — the broker cancels the consumer and
      // requeues every unacked delivery on the channel. Swallowed because a
      // channel whose connection has already gone rejects here, and a teardown
      // that fails to tear down is worse than one that finds nothing to do.
      closeConsumer: (c) =>
        Effect.promise(() => c.channel.close().then(() => {}, () => {})),
    };
  });

/** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
export const RmqLive = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
