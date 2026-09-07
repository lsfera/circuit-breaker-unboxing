import { Context, Data, Effect, Layer, Option as O, Scope, Tracer } from "effect";
import * as amqp from "amqplib";
import { parentFrom, TRACEPARENT, traceparent } from "./Trace.ts";
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
 *
 * Deliberately mutable, and it is the one piece of mutability in this file
 * that earns its place: on a recovery the client rebuilds the consumer on a
 * new channel and points this same object at it, so a caller holding the
 * handle across an outage still holds a working one. @egress/rmq-consumer
 * keeps these in `Ref`s and compares them by identity, which only works
 * because the identity survives.
 */
export type Consumer = { channel: Channel; consumerTag: string };

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
   * `x-first-death-queue` and `x-first-death-reason` as headers. `None` for a
   * message that arrived normally.
   *
   * Computed on first access and cached: the high-rate handlers never read it.
   *
   * This is what makes one canonical dead-letter queue workable rather than a
   * bin of unrelated things — anything draining it can tell work that failed
   * its third-party call from a control message that failed to decode.
   */
  readonly deadLetter: O.Option<{ readonly queue: string; readonly reason: string }>;
  /**
   * Headers carried on the message, as strings.
   *
   * The broker's death annotations are lost the moment anything republishes a
   * message, so a consumer that moves messages around inside the dead-letter
   * queue has to carry the provenance itself. This is where it puts it.
   */
  readonly properties: Readonly<Record<string, string>>;
  /**
   * The span that published this message, when it was published inside one.
   *
   * `None` for the overwhelming majority, because tracing is sampled at the
   * root — so a handler that wraps its work in a span only pays for it on the
   * messages someone decided to follow. Handed to the caller rather than
   * applied here, because this client has no idea what the work around a
   * delivery is or what the span should be called.
   */
  readonly parent: O.Option<Tracer.ExternalSpan>;
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
  /**
   * What to do when the connection goes away without anyone here asking it to.
   *
   * The default is to exit, and that is the considered choice rather than
   * laziness — see `connectionLost`. Tests that deliberately take a broker away
   * override it.
   */
  readonly onLost?: (reason: string) => void;
};

/**
 * A lost connection is fatal by default.
 *
 * amqplib does not reconnect, and every consumer on a dead connection is
 * simply gone. Nothing in this process notices: the daemon's heartbeat reads
 * local state, so it goes on reporting `self=ACTIVE` while consuming nothing,
 * and the queue it was draining shows zero consumers. Measured, by restarting
 * the broker under the running fleet: five daemons up, five daemons idle, the
 * producer silent, and not one error line between them.
 *
 * That is the deaf-daemon failure this repo has an alert and a runbook for,
 * and surviving it quietly is worse than dying. Exiting hands the problem to
 * `restart: unless-stopped`, which is what the crash-fast stance in the
 * entrypoints has always assumed — see docker-compose.yml.
 */
const connectionLost = (reason: string) => {
  console.error(
    `[rmq] connection lost (${reason}) — exiting so the restart policy can rebuild it`,
  );
  process.exit(1);
};

const describe = (delivery: ConsumeMessage): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  let deadLetter: DeliveryInfo["deadLetter"] | undefined;
  let properties: Readonly<Record<string, string>> | undefined;
  let parent: DeliveryInfo["parent"] | undefined;
  return {
    deliveryCount: Number(headers["x-delivery-count"] ?? 0),
    get deadLetter() {
      if (deadLetter === undefined) {
        const queue = headers["x-first-death-queue"];
        const reason = headers["x-first-death-reason"];
        deadLetter =
          typeof queue === "string"
            ? O.some({ queue, reason: typeof reason === "string" ? reason : "unknown" })
            : O.none();
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
    get parent() {
      if (parent === undefined) {
        const header = headers[TRACEPARENT];
        parent = parentFrom(typeof header === "string" ? header : undefined);
      }
      return parent;
    },
  };
};

/**
 * One AMQP connection that repairs itself, released when the surrounding scope
 * closes.
 *
 * Exposed separately from `RmqLive` because a connection is not always a
 * process-lifetime thing here: @egress/rmq-consumer's daemon opens one per
 * redrive pass and per probe, so that work can be abandoned wholesale.
 *
 * ## Recovery, and why the client has to own it
 *
 * amqplib will reconnect for you (`recovery`), and that is all it does: it
 * reopens the socket and hands you a fresh connection. Channels are not
 * recreated, consumers are not re-registered, and a `Channel` you are holding
 * belongs to the connection that died. Left there, "recovery" would mean a
 * process that is connected and consuming nothing — which is the same zombie
 * as no recovery at all, only harder to see.
 *
 * So this records what it was asked to build — every queue, exchange and
 * binding, and every live consumer — and rebuilds it from the `setup` hook,
 * which amqplib runs after each successful connect and before it hands the
 * connection to anyone. Topology first, because a transient queue does not
 * survive a broker restart and its consumer would fail with NOT_FOUND;
 * publish channel next; consumers last.
 *
 * `Consumer` handles are mutated in place rather than replaced, so callers
 * holding one across a recovery keep a valid handle — `@egress/rmq-consumer`
 * stores them in `Ref`s and compares them by identity.
 *
 * Recovery is bounded. If it cannot get back within `maxRetries`, the process
 * exits and the restart policy takes over: a daemon that has been retrying for
 * five minutes has nothing a restart would lose, and something the platform
 * should know about.
 */
export const makeRmq = (
  opts: RmqConnectOptions,
): Effect.Effect<RmqService, RmqError, Scope.Scope> =>
  Effect.gen(function* () {
    type OnMessage = Parameters<RmqService["consume"]>[1];

    /** Everything this connection was told to create, so it can be created again. */
    type Topology =
      | { readonly kind: "queue"; readonly name: string; readonly durable: boolean; readonly args: QueueArgs }
      | { readonly kind: "exchange"; readonly name: string; readonly durable: boolean }
      | {
          readonly kind: "bind";
          readonly routingKey: string;
          readonly source: string;
          readonly destination: string;
        };
    const topology: Topology[] = [];
    const recorded = new Set<string>();
    const record = (key: string, entry: Topology) => {
      if (recorded.has(key)) return;
      recorded.add(key);
      topology.push(entry);
    };

    /** A live consumer, and enough about it to build it again. */
    type Live = {
      readonly handle: Consumer;
      readonly queue: string;
      readonly onMessage: OnMessage;
      readonly prefetch: number;
    };
    const live = new Set<Live>();

    let out: ConfirmChannel | null = null;
    let opening: Promise<ConfirmChannel> | null = null;

    /**
     * Publishes still waiting for a confirm, so one 'close' listener per
     * channel can fail all of them.
     *
     * A listener per in-flight publish is the obvious way to write this and
     * the wrong one: the producer sends its whole batch at once, so Node
     * started reporting a possible leak at eleven concurrent publishes. They
     * were not leaking — each was removed on confirm — but a warning that
     * cries leak in the logs of a system whose logs are the diagnostic is a
     * cost of its own.
     */
    const pending = new Set<(error: Error) => void>();

    const watchPublishChannel = (ch: ConfirmChannel) => {
      ch.on("error", (error) => {
        console.warn(`[rmq] publish channel error: ${error.message}`);
      });
      ch.on("close", () => {
        if (out === ch) out = null;
        const closed = new Error("publish channel closed before the broker confirmed");
        for (const fail of [...pending]) fail(closed);
      });
      return ch;
    };

    /** The delivery callback, shared by the first registration and every rebuild. */
    const deliver =
      (ch: Channel, onMessage: OnMessage) =>
      (message: ConsumeMessage | null): void => {
        // null means the broker cancelled the consumer — the queue was deleted
        // underneath it. There is no delivery to settle.
        if (message === null) return;
        // A handler that throws synchronously would escape into amqplib's
        // delivery callback. Every handler in this repo is careful, which is
        // exactly the kind of thing that stops being true later.
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
      };

    /** Register one consumer on its own channel, and point its handle at it. */
    const attach = async (open: () => Promise<Channel>, entry: Live) => {
      const ch = await open();
      ch.on("error", (error) => {
        console.warn(`[rmq] consumer channel error on ${entry.queue}: ${error.message}`);
      });
      await ch.prefetch(entry.prefetch);
      const { consumerTag } = await ch.consume(entry.queue, deliver(ch, entry.onMessage), {
        noAck: false,
      });
      entry.handle.channel = ch;
      entry.handle.consumerTag = consumerTag;
    };

    /** Replay every declare and binding, in the order they were first made. */
    const applyTopology = async (open: () => Promise<Channel>) => {
      if (topology.length === 0) return;
      const ch = await open();
      ch.on("error", () => {});
      try {
        for (const t of topology) {
          if (t.kind === "queue") {
            await ch.assertQueue(t.name, {
              durable: t.durable,
              exclusive: false,
              arguments: t.args,
            });
          } else if (t.kind === "exchange") {
            await ch.assertExchange(t.name, "topic", { durable: t.durable });
          } else {
            await ch.bindQueue(t.destination, t.source, t.routingKey);
          }
        }
      } finally {
        await ch.close().catch(() => {});
      }
    };

    /**
     * Run after every successful connect, including the first, and before the
     * connection is handed to anyone. On the first pass there is nothing
     * recorded and this only opens the publish channel; on a recovery it is
     * what puts the process back to work.
     *
     * It uses the model it is given rather than the recovering wrapper, which
     * is not serving connections yet — asking the wrapper here would wait for
     * the connection this function is part of establishing.
     */
    const setup = async (model: ChannelModel) => {
      await applyTopology(() => model.createChannel());
      out = watchPublishChannel(await model.createConfirmChannel());
      for (const entry of live) await attach(() => model.createChannel(), entry);
    };

    const connection = yield* Effect.acquireRelease(
      wrap("connect", () =>
        amqp.connect(
          {
            protocol: "amqp",
            hostname: opts.host,
            port: opts.port,
            username: opts.username ?? "guest",
            password: opts.password ?? "guest",
          },
          {
            recovery: {
              initialDelay: 200,
              maxDelay: 5000,
              // About five minutes of trying before the process gives up.
              maxRetries: 60,
              setup,
            },
          },
        ),
      ),
      // Swallowed deliberately: this runs on every scope close, and the daemon
      // closes connections constantly. A broker that has already gone makes
      // `close` reject, and a rejection here would fail the teardown rather
      // than complete it. Closing also stops recovery, which is what a
      // deliberate teardown should do.
      (conn) => Effect.promise(() => conn.close().then(() => {}, () => {})),
    );

    connection.on("error", (error) => {
      console.warn(`[rmq] connection error: ${error.message}`);
    });
    connection.on("disconnect", (error) => {
      console.warn(`[rmq] disconnected (${error?.message ?? "no reason given"}) — recovering`);
    });
    connection.on("reconnect-scheduled", ({ attempt, delay }) => {
      console.warn(`[rmq] reconnect attempt ${attempt} in ${delay}ms`);
    });
    connection.on("connect", () => {
      console.warn(
        `[rmq] reconnected — ${topology.length} topology entries and ${live.size} consumer(s) restored`,
      );
    });
    // Recovery has given up. Everything below this line is the old crash-fast
    // stance, unchanged: a process that cannot reach its broker is no use, and
    // the restart policy is what gets it looked at.
    connection.on("reconnect-failed", (error) => {
      (opts.onLost ?? connectionLost)(`recovery gave up: ${error.message}`);
    });

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
     */
    const publish = (pub: Publisher, content: Buffer, options: amqp.Options.Publish) =>
      publishChannel().then(
        (ch) =>
          new Promise<void>((resolve, reject) => {
            const fail = (error: Error) => {
              pending.delete(fail);
              reject(error);
            };
            pending.add(fail);
            ch.publish(pub.exchange, pub.routingKey, content, options, (error) => {
              if (!pending.delete(fail)) return; // already failed by a close
              if (error) reject(error instanceof Error ? error : new Error(String(error)));
              else resolve();
            });
          }),
      );

    /** Forget a consumer, so a recovery does not bring back one we retired. */
    const forget = (c: Consumer) => {
      for (const entry of live) {
        if (entry.handle === c) {
          live.delete(entry);
          return;
        }
      }
    };

    return {
      declareQueue: (name, options = {}) => {
        const durable = options.durable ?? false;
        const args = options.args ?? {};
        record(`q:${name}`, { kind: "queue", name, durable, args });
        return onFreshChannel("declareQueue", async (ch) => {
          await ch.assertQueue(name, { durable, exclusive: false, arguments: args });
          return name as RmqQueue;
        });
      },
      declareTopicExchange: (name, options = {}) => {
        const durable = options.durable ?? false;
        record(`x:${name}`, { kind: "exchange", name, durable });
        return onFreshChannel("declareExchange", async (ch) => {
          await ch.assertExchange(name, "topic", { durable });
          return name as RmqExchange;
        });
      },
      bind: (routingKey, source, destination) => {
        record(`b:${String(source)}:${routingKey}:${String(destination)}`, {
          kind: "bind",
          routingKey,
          source: source as string,
          destination: destination as string,
        });
        return onFreshChannel("bind", async (ch) => {
          await ch.bindQueue(destination as string, source as string, routingKey);
        }).pipe(Effect.asVoid);
      },
      consume: (queue, onMessage, options = {}) =>
        wrap("consume", async () => {
          const handle = { channel: undefined, consumerTag: "" } as unknown as Consumer;
          const entry: Live = {
            handle,
            queue,
            onMessage,
            prefetch: options.prefetch ?? DEFAULT_PREFETCH,
          };
          await attach(() => connection.createChannel(), entry);
          live.add(entry);
          return handle;
        }),
      publisherToExchange: (exchange, routingKey) => Effect.succeed({ exchange, routingKey }),
      publisherToQueue: (queue) =>
        // The default exchange routes by queue name, which is the same path
        // `deadLetterArgs` uses for dead-lettering.
        Effect.succeed({ exchange: "", routingKey: queue }),
      // The `traceparent` goes on as an ordinary header, from whatever span the
      // caller is inside. Outside a span it resolves to undefined and nothing
      // is added, so an untraced publish carries exactly the bytes it did
      // before — see Trace.ts.
      send: (pub, body, properties) =>
        Effect.flatMap(traceparent, (tp) => {
          const headers = O.match(tp, {
            onNone: () => properties,
            onSome: (value) => ({ ...(properties ?? {}), [TRACEPARENT]: value }),
          });
          return wrap("send", () =>
            publish(
              pub,
              Buffer.from(body, "utf8"),
              headers === undefined
                ? { persistent: true }
                : { persistent: true, headers },
            ),
          );
        }),
      cancelConsumer: (c) =>
        Effect.promise(() => {
          forget(c);
          return c.channel.cancel(c.consumerTag).then(() => {}, () => {});
        }),
      closeConsumer: (c) =>
        Effect.promise(() => {
          forget(c);
          return c.channel.close().then(() => {}, () => {});
        }),
    };
  });

/** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
export const RmqLive = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
