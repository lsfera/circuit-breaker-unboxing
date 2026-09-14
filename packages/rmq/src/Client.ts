import { Context, Data, Deferred, Effect, Layer, Option as O, Predicate, Scope, Tracer } from "effect";
import * as amqp from "amqplib";
import { parentFrom, TRACEPARENT, traceparent } from "./Trace.ts";
import { IDEMPOTENCY_KEY_HEADER } from "./ControlPlane.ts";
import type { ChannelModel, ConfirmChannel, ConsumeMessage, Channel } from "amqplib";

/**
 * Opaque handles. `declareQueue`/`declareTopicExchange` hand these straight
 * back to `bind`, and nothing else ever inspects them, so their shape is not
 * part of the contract.
 */
type RmqExchange = unknown;
type RmqQueue = unknown;

/**
 * A channel plus the tag the broker gave it. Mutable on purpose: recovery
 * rebuilds the consumer on a new channel and repoints this same object, so a
 * caller holding the handle — or comparing it by identity — keeps a valid one.
 */
export type Consumer = { channel: Channel; consumerTag: string };

/**
 * A publisher is an address, not a link. In AMQP 0-9-1 publishing takes the
 * exchange and routing key per call, so there is nothing to open, nothing to
 * fail halfway, and nothing to race.
 */
export type Publisher = { readonly exchange: string; readonly routingKey: string };

/**
 * Effect wrapper over amqplib (AMQP 0-9-1). Why this protocol and not 1.0:
 * docs/decisions/004-downgrade-to-amqp-0-9-1.md.
 *
 * Channels, not connections, are the unit of isolation. Every consumer gets its
 * own channel, so a channel that errors takes down nothing else and cancelling a
 * consumer leaves it able to settle the delivery it holds. Declares get a
 * throwaway channel each, so a `PRECONDITION_FAILED` from a mismatched redeclare
 * cannot take the publish path with it.
 */

export class RmqError extends Data.TaggedError("RmqError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {
  /**
   * `Data.TaggedError` prints its `message`, and without one every line reads
   * `RmqError:` and nothing else — which is what the fatal log looked like the
   * first time a connection was actually lost through this path.
   */
  override get message(): string {
    return `${this.operation}: ${String(this.cause)}`;
  }
}

type QueueArgs = Record<string, unknown>;

/**
 * What a handler asks the broker to do with its delivery.
 *
 * - `accept` — drop it from the queue; the default when a handler returns nothing.
 * - `requeue` — back on the queue, with no delay, counting toward a quorum
 *   queue's `x-delivery-limit` (see the comment on `settle` below), so an
 *   unbounded requeue on a failing dependency both hot-loops and eventually
 *   dead-letters. Bound it, or use `release` if the message did not fail.
 * - `discard` — rejected without requeue: dead-lettered where a target is
 *   declared, dropped where none is.
 * - `release` — back on the queue like `requeue`, but *not* counted: for a
 *   delivery held only for backpressure, not because the work itself failed —
 *   a local 503 from a concurrency limiter is the case this exists for. The
 *   broker hands it to the next available consumer, or back to this one,
 *   with no strike against it.
 */
export type Settlement = "accept" | "requeue" | "discard" | "release";

/** What the broker knows about this particular delivery. */
export type DeliveryInfo = {
  /**
   * The broker's own `x-delivery-count`, 0 on a first delivery. For looking at;
   * enforcement stays the queue's job via `x-delivery-limit`, because an
   * in-process counter dies when a message moves to another consumer.
   */
  readonly deliveryCount: number;
  /**
   * Where this message was dead-lettered from (`x-first-death-*`), `None` if it
   * arrived normally. Computed lazily; the high-rate handlers never read it. It is
   * what lets one dead-letter queue hold unrelated things and still be drainable.
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
   * The payments idempotency key, read directly off the one header rather than
   * through `properties`. The work and probe consumers read this on every
   * delivery — thousands a second — and materializing every header into
   * strings for one value would be waste.
   */
  readonly idempotencyKey: O.Option<string>;
  /**
   * The publishing span, when there was one. `None` for most messages, since
   * tracing is sampled at the root. Handed to the caller rather than applied here.
   */
  readonly parent: O.Option<Tracer.ExternalSpan>;
};

/**
 * How many deliveries a consumer may hold unsettled.
 *
 * A default, for consumers bounded by something other than how many messages
 * they hold — the control queue, the elections, a redrive pass. Any consumer
 * whose prefetch *is* its concurrency limit passes its own; see
 * docs/decisions/011-the-ceiling-belongs-to-the-broker.md.
 */
const DEFAULT_PREFETCH = 100;

export interface RmqService {
  /**
   * Durable unless told otherwise. RabbitMQ 4.3 refuses a transient queue that
   * is not exclusive by closing the connection, and this client only declares
   * non-exclusive queues, so `durable: false` is a broker error waiting to
   * happen rather than a choice.
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
   * Settled only once `onMessage` settles, which is the flow-control lever: a
   * handler that awaits its own work holds the delivery unacked, and `prefetch`
   * bounds how many it may hold. A synchronous handler gets no backpressure.
   *
   * The returned value chooses the outcome; returning nothing accepts. A handler
   * that throws also accepts — an unbounded redelivery loop driven by a bug is
   * worse than a lost message and much harder to see.
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
   * `properties` become message headers. Everything is published persistent, with
   * no flag: on a transient queue the broker ignores it, and on a durable one it is
   * the difference between keeping a message across a restart and appearing to.
   */
  readonly send: (
    pub: Publisher,
    body: string,
    properties?: Record<string, string>,
  ) => Effect.Effect<void, RmqError>;
  /**
   * Stop delivery, leaving the channel able to settle what it still holds — what
   * the probe needs, since closing instead would hand its message back to the queue.
   */
  readonly cancelConsumer: (c: Consumer) => Effect.Effect<void>;
  /**
   * Retire the consumer and its channel. Anything held unacked returns to the
   * queue, which is how a daemon abandons work wholesale when the circuit moves.
   */
  readonly closeConsumer: (c: Consumer) => Effect.Effect<void>;
  /**
   * Never completes while the connection is usable; fails once recovery has
   * given up on it.
   *
   * A lost connection is fatal, and that is a considered decision rather than
   * laziness: every consumer on it is gone and nothing in the process notices,
   * so it keeps reporting itself active while consuming nothing. What changed
   * is who acts on it. This module used to call `process.exit(1)` itself —
   * docs/decisions/008-configuration-is-a-boundary.md had already removed
   * exactly that from `@egress/config`, on the grounds that fail-fast is right
   * and owning the process's fate from inside a library is not. `launch` below
   * is the one place that turns this into an exit.
   */
  readonly lost: Effect.Effect<never, RmqError>;
}

export class Rmq extends Context.Service<Rmq, RmqService>()("@egress/rmq/Client/Rmq") {
  /** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
  static readonly layer = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

/**
 * Settling a delivery whose channel has since closed throws
 * `IllegalOperationError`, and with a deferred ack that is not an edge case —
 * it is what happens whenever a consumer is retired while calls are still in
 * flight, which is exactly what `OPEN` does to @egress/rmq-consumer's daemons.
 * The settlement is genuinely moot at that point, because the broker requeues
 * every unacked delivery when the channel goes.
 *
 * `reject`, not `nack`, for `requeue` — and the reverse for `release`, and
 * that split is deliberate rather than an inconsistency. From RabbitMQ 4.3 a
 * `nack` with requeue does not count toward a quorum queue's
 * `x-delivery-limit` — measured, 8,954 redeliveries in four seconds and never
 * dead-lettered, where 4.0 parked the message after four — while a requeuing
 * `reject` still counts on both. `requeue` needs the count to spend (a failed
 * call is an attempt); `release` needs it not to (a 429 never reached the
 * third party, so nothing about the message was tried). Same wire behaviour
 * on RabbitMQ 4.0 either way — the two only diverge on 4.3, in exactly the
 * direction each is meant to use.
 */
const settle = (channel: Channel, message: ConsumeMessage, outcome: Settlement) => {
  try {
    if (outcome === "discard") channel.reject(message, false);
    else if (outcome === "requeue") channel.reject(message, true);
    else if (outcome === "release") channel.nack(message, false, true);
    else channel.ack(message);
  } catch {
    // channel already gone; the broker has the delivery back
  }
};

type RmqConnectOptions = {
  readonly host: string;
  readonly port: number;
  readonly username?: string;
  readonly password?: string;
};

const describe = (delivery: ConsumeMessage): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  let deadLetter: DeliveryInfo["deadLetter"] | undefined;
  let properties: Readonly<Record<string, string>> | undefined;
  let idempotencyKey: DeliveryInfo["idempotencyKey"] | undefined;
  let parent: DeliveryInfo["parent"] | undefined;
  return {
    deliveryCount: Number(headers["x-delivery-count"] ?? 0),
    get deadLetter() {
      if (deadLetter === undefined) {
        const queue = headers["x-first-death-queue"];
        const reason = headers["x-first-death-reason"];
        deadLetter =
          Predicate.isString(queue)
            ? O.some({ queue, reason: Predicate.isString(reason) ? reason : "unknown" })
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
    get idempotencyKey() {
      if (idempotencyKey === undefined) {
        const header = headers[IDEMPOTENCY_KEY_HEADER];
        idempotencyKey = Predicate.isString(header) ? O.some(header) : O.none();
      }
      return idempotencyKey;
    },
    get parent() {
      if (parent === undefined) {
        const header = headers[TRACEPARENT];
        parent = parentFrom(Predicate.isString(header) ? header : undefined);
      }
      return parent;
    },
  };
};

/**
 * One AMQP connection that repairs itself, released with the surrounding scope.
 * Separate from `Rmq.layer` because a connection is not always process-lifetime
 * here — a redrive pass and a probe each open their own.
 *
 * amqplib's own `recovery` reopens the socket and nothing else: channels are not
 * recreated and consumers are not re-registered, so a "recovered" process would
 * be connected and consuming nothing. This records every queue, exchange,
 * binding and live consumer and rebuilds them from the `setup` hook. Order
 * matters — topology first, since a transient queue does not survive a broker
 * restart and its consumer would fail NOT_FOUND; publish channel; consumers.
 *
 * Bounded: past `maxRetries` recovery gives up and `lost` fails, which is what
 * `launchWithRmq` turns into a stopped process for the restart policy to pick
 * up. See docs/decisions/005-connection-recovery.md.
 */
export const makeRmq = Effect.fnUntraced(function* (
  opts: RmqConnectOptions,
): Effect.fn.Return<RmqService, RmqError, Scope.Scope> {
  type OnMessage = Parameters<RmqService["consume"]>[1];

  /**
   * Everything below runs in an amqplib event handler, off any fiber. Bare
   * `console.warn` was reaching stderr but not the logger the process
   * configured — no level, no fiber, and invisible to any sink or filter
   * that logger has. This is the same capture `@egress/rmq-consumer` makes
   * for its delivery callbacks, for the same reason.
   */
  const services = yield* Effect.context<never>();
  const forkInContext = Effect.runForkWith(services);

  /** Completed once, by the 'reconnect-failed' handler below. */
  const lost = Deferred.makeUnsafe<never, RmqError>();
  const warn = (message: string) => forkInContext(Effect.logWarning(`[rmq] ${message}`));

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

  /**
   * Whether the connection is usable, tracked from amqplib's own events.
   *
   * Only honest at the point a rebuild *fails*, which is the one place it is
   * read: a channel's 'close' arrives before the connection's 'disconnect',
   * so this still says `true` while a rebuild is being decided, and says
   * `false` by the time that rebuild's attach rejects. That is enough to tell
   * "this channel died and I could not put it back" from "the connection went
   * and `setup` is about to put everything back".
   */
  let connected = true;

  // `null` rather than `Option` on purpose: these are private mutable
  // interop state — "no channel open right now" and "no reopen in flight" —
  // not a value anyone outside this closure branches on. See
  // docs/decisions/006-representing-absence.md.
  let out: ConfirmChannel | null = null;
  let opening: Promise<ConfirmChannel> | null = null;

  /**
   * Publishes awaiting a confirm, so one 'close' listener per channel can fail
   * all of them. A listener per publish trips Node's leak warning at eleven
   * concurrent, which the producer's batching reaches immediately.
   */
  const pending = new Set<(error: Error) => void>();

  const watchPublishChannel = (ch: ConfirmChannel) => {
    ch.on("error", (error) => {
      warn(`publish channel error: ${error.message}`);
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
    (ch: Channel, entry: Live) =>
    (message: ConsumeMessage | null): void => {
      // The broker cancelled this consumer — its queue was deleted underneath
      // it. There is no delivery to settle and nothing to rebuild: the
      // consumer is simply not receiving any more, which is worth saying,
      // because the channel stays open and looks healthy.
      if (message === null) {
        warn(`broker cancelled the consumer on ${entry.queue} — it receives nothing now`);
        return;
      }
      const onMessage = entry.onMessage;
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
      if (Predicate.isString(done)) return settle(ch, message, done);
      void done.then(
        (outcome) => settle(ch, message, outcome ?? "accept"),
        () => settle(ch, message, "accept"),
      );
    };

  /**
   * Put a consumer back after its channel closed under it.
   *
   * amqplib recovers *connections*; a channel that dies on its own — a
   * protocol error, a queue deleted, a settle on a tag the broker has already
   * seen — takes its consumer with it and leaves the connection healthy, so
   * nothing else here would ever notice. The handle its caller holds still
   * looks live, which is how a process goes deaf while reporting itself well.
   *
   * Unbounded on purpose. Every failure seen here either stops itself — a
   * queue that is gone makes `attach` reject, and a rejection leaves no
   * channel to close again — or makes progress. A count of attempts was worse
   * than nothing: the election queues are idle by design, so a budget reset by
   * deliveries never reset on them, and the daemon left the election for good
   * over a condition the next rebuild would have fixed. If a genuine spin ever
   * turns up, the answer is a delay, not a limit on how often a consumer may
   * be repaired.
   */
  const rebuild = (entry: Live) => {
    // Retired deliberately. Both teardown paths forget their consumer, and
    // closing the connection forgets all of them, so this covers a deliberate
    // shutdown as well as a deliberate close.
    if (!live.has(entry)) return;
    attach(() => connection.createChannel(), entry).then(
      () => warn(`consumer channel on ${entry.queue} closed — rebuilt`),
      (error) => {
        // `connected` is false by now if the connection is what went, and
        // `setup` re-attaches everything when it returns. Saying so here would
        // report a failure that is already being handled.
        if (connected) {
          warn(`consumer on ${entry.queue} closed and could not be rebuilt: ${String(error)}`);
        }
      },
    );
  };

  /** Register one consumer on its own channel, and point its handle at it. */
  const attach = async (open: () => Promise<Channel>, entry: Live) => {
    const ch = await open();
    ch.on("error", (error) => {
      warn(`consumer channel error on ${entry.queue}: ${error.message}`);
    });
    ch.on("close", () => rebuild(entry));
    await ch.prefetch(entry.prefetch);
    const { consumerTag } = await ch.consume(entry.queue, deliver(ch, entry), {
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
   * Runs after every successful connect, before the connection is handed out.
   * Uses the model it is given rather than the recovering wrapper, which is not
   * serving connections yet and would deadlock waiting for this one.
   */
  const setup = async (model: ChannelModel) => {
    await applyTopology(() => model.createChannel());
    out = watchPublishChannel(await model.createConfirmChannel());
    for (const entry of live) await attach(() => model.createChannel(), entry);
    connected = true;
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
    // deliberate teardown should do. Forgetting every consumer first is what
    // stops the channel closes this causes from being read as consumers to
    // repair — the same bookkeeping `closeConsumer` uses, for all of them at
    // once.
    (conn) =>
      Effect.promise(() => {
        live.clear();
        return conn.close().then(() => {}, () => {});
      }),
  );

  connection.on("error", (error) => {
    warn(`connection error: ${error.message}`);
  });
  connection.on("disconnect", (error) => {
    connected = false;
    warn(`disconnected (${error?.message ?? "no reason given"}) — recovering`);
  });
  connection.on("reconnect-scheduled", ({ attempt, delay }) => {
    warn(`reconnect attempt ${attempt} in ${delay}ms`);
  });
  connection.on("connect", () => {
    warn(
      `reconnected — ${topology.length} topology entries and ${live.size} consumer(s) restored`,
    );
  });
  // Recovery has given up. The stance is unchanged — a process that cannot
  // reach its broker is no use, and the restart policy is what gets it looked
  // at — but it is now a failure that travels, not an exit taken here.
  connection.on("reconnect-failed", (error) => {
    Deferred.doneUnsafe(
      lost,
      Effect.fail(
        new RmqError({ operation: "connection", cause: `recovery gave up: ${error.message}` }),
      ),
    );
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
    Effect.acquireUseRelease(
      wrap(operation, () =>
        connection.createChannel().then((ch) => {
          // A declare that fails closes its channel, and an unhandled 'error'
          // on it would reach the process. The failure is the rejection below.
          ch.on("error", () => {});
          return ch;
        }),
      ),
      (ch) => wrap(operation, () => use(ch)),
      // Closing is best effort by definition: the channel this runs on may be
      // the one the broker just closed under us.
      (ch) => Effect.promise(() => ch.close().then(() => {}, () => {})),
    );

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

  return Rmq.of({
    declareQueue: (name, options = {}) => {
      const durable = options.durable ?? true;
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
    lost: Deferred.await(lost),
  });
});


/**
 * Build `layer` and run until its scope ends or the broker connection is lost,
 * whichever comes first — `Layer.launch` for a graph that contains an `Rmq`.
 *
 * This exists so the fatal-on-lost-connection decision has exactly one call
 * site. `Layer.launch` alone blocks forever, and a fiber forked into the
 * layer's scope cannot end it: measured, a defect in one leaves `Layer.launch`
 * running. So the guarantee has to be on the fiber that launches, and putting
 * it here rather than in each `main.ts` is the difference between a rule and
 * three places that have to remember it.
 */
export const launchWithRmq = <ROut, E, RIn>(
  layer: Layer.Layer<ROut | Rmq, E, RIn>,
): Effect.Effect<never, E | RmqError, RIn> =>
  Effect.scoped(
    Effect.flatMap(Layer.build(layer), (context) => Context.get(context, Rmq).lost),
  );
