import { randomUUID } from "node:crypto";
import { Array as Arr, Context, Data, Deferred, Effect, Layer, Match, Option as O, Predicate, Scope, Tracer } from "effect";
import * as amqp from "amqplib";
import { parentFrom, TRACEPARENT, traceparent } from "./Trace.ts";
import type { ChannelModel, ConfirmChannel, ConsumeMessage, Channel } from "amqplib";

/** Opaque handles: `declareQueue`/`declareTopicExchange` hand these back to `bind`; nothing else inspects them. */
type RmqExchange = unknown;
type RmqQueue = unknown;

/**
 * A channel plus the tag the broker gave it. Mutable on purpose: recovery repoints this same
 * object at a rebuilt channel, so a caller's handle stays valid.
 */
export type Consumer = { channel: Channel; consumerTag: string };

/** A publisher is an address, not a link: AMQP 0-9-1 takes exchange and routing key per publish, so nothing to open or race. */
export type Publisher = {
  readonly exchange: string;
  readonly routingKey: string;
  /**
   * Sent as the AMQP `content_type`/`content_encoding` on every message (`application/json`; `gzip`,
   * comma-separated). RabbitMQ ignores them; a reader uses them to decline what it cannot decode.
   */
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  /** The AMQP `type` property: what kind of message this is, dot-separated by convention (`egress.work`). */
  readonly type: O.Option<string>;
  /**
   * Whether the broker must return an unroutable message, which `send` then fails with `Unroutable`.
   * On for a queue, where the default exchange would drop it silently; off for an exchange, where no
   * bindings yet is ordinary.
   */
  readonly mandatory: boolean;
};

/** What a publisher may declare about its bodies; all optional, and a publisher that declares nothing says nothing. */
export type Format = {
  readonly contentType?: string;
  readonly contentEncoding?: string;
  readonly type?: string;
};

/** What a publish may carry beyond its body. `messageId` is the AMQP `message_id`; left out, `send` invents one. */
export type SendOptions = {
  readonly headers?: Record<string, string>;
  readonly messageId?: string;
};

/**
 * One message fetched by `get`, held unsettled until `ack`/`nack` runs — `ack` drops it, `nack` requeues it.
 * `Settlement`'s `requeue`/`release` split doesn't apply here: just "done with it" or "put it back".
 */
export type GotMessage = {
  readonly body: string;
  /** Headers as strings, same shape as `DeliveryInfo.properties` — Redrive.ts reads `REDRIVE_COUNT_HEADER` off a `get`, same as any `consume` handler would. */
  readonly properties: Readonly<Record<string, string>>;
  /** The AMQP `message_id`, so a caller that republishes this message (Redrive.ts) can carry the same idempotency key forward instead of `send` inventing a new one. */
  readonly messageId: O.Option<string>;
  readonly ack: Effect.Effect<void>;
  readonly nack: Effect.Effect<void>;
};

/** The broker returned a mandatory message it could not route to any queue. */
export class Unroutable extends Error {}

/** True when this failure is the broker handing a mandatory message back as unroutable. */
export const isUnroutable = (error: RmqError): boolean => error.cause instanceof Unroutable;

/**
 * Effect wrapper over amqplib (AMQP 0-9-1).
 *
 * Channels are the unit of isolation: one per consumer, and a throwaway per declare, so a
 * `PRECONDITION_FAILED` from a mismatched redeclare cannot take the publish path down.
 */

export class RmqError extends Data.TaggedError("RmqError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {
  /** `Data.TaggedError` prints `message`; without one every log line reads `RmqError:` alone. */
  override get message(): string {
    return `${this.operation}: ${String(this.cause)}`;
  }
}

type QueueArgs = Record<string, unknown>;

/**
 * What a handler asks the broker to do with its delivery.
 *
 * - `accept` — drop it; the default when a handler returns nothing.
 * - `requeue` — back on the queue with no delay, counted toward a quorum queue's `x-delivery-limit`
 *   (see `settle`), so an unbounded requeue on a failing dependency hot-loops, then dead-letters.
 *   Use `release` if the message did not fail.
 * - `discard` — reject without requeue: dead-lettered where a target is declared, dropped otherwise.
 * - `release` — back on the queue like `requeue` but not counted: for a delivery held only for
 *   backpressure (a `429`), not because the work failed.
 */
export type Settlement = "accept" | "requeue" | "discard" | "release";

/** What the broker knows about this particular delivery. */
export type DeliveryInfo = {
  /** The broker's `x-delivery-count`, 0 on a first delivery. For looking at: enforcement is the queue's `x-delivery-limit`. */
  readonly deliveryCount: number;
  /** Where this message was dead-lettered from (`x-first-death-*`), `None` if it arrived normally. Lazy. */
  readonly deadLetter: O.Option<{ readonly queue: string; readonly reason: string }>;
  /** Headers as strings. Broker death annotations are lost on republish, so a mover carries provenance here. */
  readonly properties: Readonly<Record<string, string>>;
  /** The `content_type`/`content_encoding` the publisher declared, `None` if it declared nothing. */
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  /** The AMQP `type`, `message_id` and `timestamp` the publisher stamped (`send` stamps the last two on every message). */
  readonly type: O.Option<string>;
  readonly messageId: O.Option<string>;
  /** When it was published, epoch milliseconds: AMQP carries seconds, so this is only as fine as a second. */
  readonly publishedAt: O.Option<number>;
  /** The publishing span, `None` for most messages (tracing is sampled at the root). Handed to the caller, not applied here. */
  readonly parent: O.Option<Tracer.ExternalSpan>;
};

/**
 * Default prefetch for a consumer bounded by something other than how many messages it holds. One whose
 * prefetch *is* its concurrency limit passes its own.
 */
const DEFAULT_PREFETCH = 100;

export interface RmqService {
  /** Durable unless told otherwise: RabbitMQ 4.3 closes the connection on a transient, non-exclusive queue. */
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
   * Settled only once `onMessage` settles — the flow-control lever: `prefetch` bounds how many awaited
   * deliveries a handler may hold unacked. A handler that throws or rejects is `discard`ed, not accepted.
   */
  readonly consume: (
    queue: string,
    onMessage: (
      body: string,
      delivery: DeliveryInfo,
    ) => void | Settlement | Promise<void | Settlement>,
    options?: { readonly prefetch?: number },
  ) => Effect.Effect<Consumer, RmqError>;
  /**
   * A single non-blocking fetch — `basic.get`, not a subscription. `None` when the queue was empty. Its own
   * throwaway channel, held open only until `GotMessage` is settled — not part of `topology` replay.
   */
  readonly get: (queue: string) => Effect.Effect<O.Option<GotMessage>, RmqError>;
  /** One publisher per fixed (exchange, routingKey) or (queue) target. */
  readonly publisherToExchange: (
    exchange: string,
    routingKey: string,
    format?: Format,
  ) => Effect.Effect<Publisher, RmqError>;
  readonly publisherToQueue: (queue: string, format?: Format) => Effect.Effect<Publisher, RmqError>;
  /** Always persistent: ignored on a transient queue, the difference between surviving a restart and appearing to. */
  readonly send: (pub: Publisher, body: string, options?: SendOptions) => Effect.Effect<void, RmqError>;
  /**
   * Stop delivery but leave the channel able to settle what it still holds; closing instead would hand
   * that back to the queue.
   */
  readonly cancelConsumer: (c: Consumer) => Effect.Effect<void>;
  /** Retire the consumer and its channel; anything held unacked returns to the queue. */
  readonly closeConsumer: (c: Consumer) => Effect.Effect<void>;
  /**
   * Never completes while the connection is usable; fails once recovery has given up. Fatal on purpose —
   * `launchWithRmq` is the one place that turns it into a process-ending failure.
   */
  readonly lost: Effect.Effect<never, RmqError>;
  /**
   * Tracked from amqplib's events; `false` lags a real break (a one-sided partition looks connected until
   * the heartbeat times out). A caller that needs to know publishing works must track outcomes itself.
   */
  readonly isConnected: Effect.Effect<boolean>;
  /**
   * Destroy the underlying sockets outright rather than a graceful close, which could wait on a peer that
   * never answers. Recovery is the ordinary reconnect path. For a caller fencing its own in-flight publishes; not routine use.
   */
  readonly resetConnection: Effect.Effect<void>;
}

export class Rmq extends Context.Service<Rmq, RmqService>()("@egress/rmq/Client/Rmq") {
  /** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
  static readonly layer = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

/**
 * A closed channel throws `IllegalOperationError` on settle — routine with a deferred ack, and moot: the
 * broker requeues every unacked delivery when the channel goes.
 *
 * `reject` for `requeue` but `nack` for `release`: RabbitMQ 4.3's requeuing `nack` doesn't count toward a
 * quorum queue's `x-delivery-limit` while a requeuing `reject` does. `requeue` needs the count (a failed
 * call is an attempt); `release` needs it not to (the message was never tried).
 */
const settle = (channel: Channel, message: ConsumeMessage, outcome: Settlement) => {
  try {
    Match.value(outcome).pipe(
      Match.when("discard", () => channel.reject(message, false)),
      Match.when("requeue", () => channel.reject(message, true)),
      Match.when("release", () => channel.nack(message, false, true)),
      Match.when("accept", () => channel.ack(message)),
      Match.exhaustive,
    );
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

/** Computed on first read and kept. The memo is an `Option` because a computed value is not the same as an absent one. */
const lazily = <A>(compute: () => A): (() => A) => {
  let memo = O.none<A>();
  return () =>
    O.getOrElse(memo, () => {
      const value = compute();
      memo = O.some(value);
      return value;
    });
};

/** When `condition` holds, run `effect`: a guard that is only a side effect, without a statement for it. */
const when = (condition: boolean, effect: () => void): void => {
  void (condition && effect());
};

/** Runs `run` for each item one after the other, stopping at the first rejection — the order matters and so does not overlap. */
const inSequence = <A>(items: Iterable<A>, run: (item: A) => Promise<unknown>): Promise<unknown> =>
  Array.from(items).reduce<Promise<unknown>>((done, item) => done.then(() => run(item)), Promise.resolve());

/** Headers as amqplib hands them back (values of unknown type) to the string-keyed, string-valued shape every caller here wants. */
const stringifyHeaders = (headers: Record<string, unknown>): Readonly<Record<string, string>> =>
  Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, String(v)]));

const describe = (delivery: ConsumeMessage): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  const header = (name: string) => O.liftPredicate(headers[name], Predicate.isString);
  const deadLetter = lazily(() =>
    O.map(header("x-first-death-queue"), (queue) => ({
      queue,
      reason: O.getOrElse(header("x-first-death-reason"), () => "unknown"),
    })),
  );
  const properties = lazily(() => stringifyHeaders(headers));
  const contentType = O.liftPredicate(delivery.properties.contentType, Predicate.isString);
  const contentEncoding = O.liftPredicate(delivery.properties.contentEncoding, Predicate.isString);
  const type = O.liftPredicate(delivery.properties.type, Predicate.isString);
  const messageId = O.liftPredicate(delivery.properties.messageId, Predicate.isString);
  const publishedAt = O.map(
    O.liftPredicate(delivery.properties.timestamp, Predicate.isNumber),
    (seconds) => seconds * 1000,
  );
  const parent = lazily(() => O.flatMap(header(TRACEPARENT), parentFrom));
  return {
    deliveryCount: Number(headers["x-delivery-count"] ?? 0),
    contentType,
    contentEncoding,
    type,
    messageId,
    publishedAt,
    get deadLetter() {
      return deadLetter();
    },
    get properties() {
      return properties();
    },
    get parent() {
      return parent();
    },
  };
};

/**
 * One AMQP connection that repairs itself, released with the surrounding scope. amqplib's own `recovery`
 * reopens the socket and nothing else — channels and consumers aren't recreated. This records every queue,
 * exchange, binding and live consumer and rebuilds them from `setup`, topology first (a transient queue
 * wouldn't survive a broker restart, so its consumer would fail NOT_FOUND), then consumers.
 *
 * Bounded: past `maxRetries` recovery gives up and `lost` fails, for `launchWithRmq`'s restart policy.
 */
export const makeRmq = Effect.fnUntraced(function* (
  opts: RmqConnectOptions,
): Effect.fn.Return<RmqService, RmqError, Scope.Scope> {
  type OnMessage = Parameters<RmqService["consume"]>[1];

  /** Handlers run in amqplib event callbacks, off any fiber: capture the context so `warn` reaches the configured logger. */
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
  // Keyed, first declaration wins, and a `Map` keeps insertion order, which is
  // the order the replay needs.
  const topology = new Map<string, Topology>();
  const record = (key: string, entry: Topology) => {
    topology.set(key, topology.get(key) ?? entry);
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
   * Whether the connection is usable. Only honest where it's read, at a rebuild failure: it says `true`
   * while the rebuild is being decided and `false` by the time `attach` rejects, telling "this channel
   * died and couldn't be put back" from "the connection went and `setup` will restore everything".
   */
  let connected = true;
  /** The same for the publishing connection, which fails and recovers on its own. */
  let publishing = true;

  /** `null`, not `Option`: private mutable interop state that nobody outside this closure branches on. */
  let out: ConfirmChannel | null = null;
  let opening: Promise<ConfirmChannel> | null = null;
  /** Publishes awaiting a confirm on `out`, so its 'close' listener can fail all of them at once. */
  const pending = new Set<(error: Error) => void>();
  /** `message_id`s the broker has handed back as unroutable and whose confirm has not yet arrived. */
  const unroutable = new Set<string>();

  /**
   * Captured from `setup`, not the `'connect'` event — that fires before `amqp.connect()` resolves and
   * would miss the first connection. `ChannelModel.connection`'s type omits `.stream`; see `resetConnection`.
   */
  let currentModel: ChannelModel | null = null;
  /** The publishing connection's model, kept for the same reason: `resetConnection` reaches both sockets. */
  let currentPublishModel: ChannelModel | null = null;

  const watchPublishChannel = (ch: ConfirmChannel) => {
    ch.on("error", (error) => {
      warn(`publish channel error: ${error.message}`);
    });
    ch.on("return", (message) => {
      unroutable.add(String(message.properties.messageId));
    });
    ch.on("close", () => {
      out = out === ch ? null : out;
      const closed = new Error("publish channel closed before the broker confirmed");
      [...pending].forEach((fail) => fail(closed));
    });
    return ch;
  };

  /** The delivery callback, shared by the first registration and every rebuild. */
  const deliver =
    (ch: Channel, entry: Live) =>
      (message: ConsumeMessage | null): void =>
        O.match(O.fromNullOr(message), {
          // The broker cancelled this consumer (its queue was deleted): nothing to settle or rebuild, and the
          // channel stays open and looks healthy, so say so.
          onNone: () => void warn(`broker cancelled the consumer on ${entry.queue} — it receives nothing now`),
          onSome: (delivery) => handle(ch, entry, delivery),
        });

  const isPending = (
    done: void | Settlement | PromiseLike<void | Settlement>,
  ): done is PromiseLike<void | Settlement> => Predicate.isPromiseLike(done);

  /** A handler that threw or rejected didn't finish, so acking would say it had — `discard`ed instead, and logged. */
  const failed = (ch: Channel, entry: Live, message: ConsumeMessage, error: unknown): void => {
    void warn(`handler for ${entry.queue} failed, dead-lettering the delivery: ${String(error)}`);
    settle(ch, message, "discard");
  };

  const handle = (ch: Channel, entry: Live, message: ConsumeMessage): void => {
    // A synchronous throw would escape into amqplib's delivery callback.
    let done: void | Settlement | PromiseLike<void | Settlement>;
    try {
      done = entry.onMessage(message.content.toString("utf8"), describe(message));
    } catch (error) {
      return failed(ch, entry, message, error);
    }
    Match.value(done).pipe(
      // A synchronous outcome is a string, not a thenable.
      Match.when(Predicate.isString, (outcome) => settle(ch, message, outcome)),
      Match.when(isPending, (later) =>
        void later.then(
          (outcome) => settle(ch, message, outcome ?? "accept"),
          (error) => failed(ch, entry, message, error),
        ),
      ),
      Match.orElse(() => settle(ch, message, "accept")),
    );
  };

  /**
   * Put a consumer back after its channel closed under it — amqplib recovers *connections*, not a channel
   * that dies alone, which would otherwise leave the caller's handle looking live but deaf.
   *
   * Unbounded on purpose: every failure here either stops itself or makes progress, and an idle queue would
   * never reset an attempt budget. If a genuine spin turns up, add a delay, not a limit.
   */
  const rebuild = (entry: Live) => {
    // Retired deliberately: both teardown paths forget their consumer, and closing the connection forgets all.
    O.liftPredicate(entry, (e: Live) => live.has(e)).pipe(
      O.map((current) =>
        attach(() => connection.createChannel(), current).then(
          () => warn(`consumer channel on ${current.queue} closed — rebuilt`),
          (error) =>
            // `connected` is false if the connection is what went, and `setup` re-attaches everything on return;
            // reporting here would flag a failure already being handled.
            when(connected, () =>
              warn(`consumer on ${current.queue} closed and could not be rebuilt: ${String(error)}`),
            ),
        ),
      ),
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
    const previous = entry.handle.channel;
    entry.handle.channel = ch;
    entry.handle.consumerTag = consumerTag;
    // Normally a no-op via `rebuild` — but a connection-level failure doesn't reliably close each channel
    // first, so `previous` can still be open here with nothing pointing at it. Close it explicitly.
    O.liftPredicate(previous, (stale: Channel) => stale !== ch).pipe(
      O.map((stale) => stale.close().catch(() => { })),
    );
  };

  /** Replay every declare and binding, in the order they were first made. */
  const replay = async (open: () => Promise<Channel>) => {
    const ch = await open();
    ch.on("error", () => { });
    try {
      await inSequence(topology.values(), (t) =>
        Match.value(t).pipe(
          Match.discriminatorsExhaustive("kind")({
            queue: (q) =>
              ch.assertQueue(q.name, { durable: q.durable, exclusive: false, arguments: q.args }),
            exchange: (x) => ch.assertExchange(x.name, "topic", { durable: x.durable }),
            bind: (b) => ch.bindQueue(b.destination, b.source, b.routingKey),
          }),
        ),
      );
    } finally {
      await ch.close().catch(() => { });
    }
  };
  const applyTopology = (open: () => Promise<Channel>) =>
    topology.size === 0 ? Promise.resolve() : replay(open);

  /**
   * Runs after every successful connect of the consuming connection, before it's handed out. Nothing here
   * publishes: a broker alarm blocks a connection by ceasing to read from it, so a consumer on a connection
   * that had published would hold its prefetch window unacked for the whole alarm (rabbitmq.com/docs/alarms).
   */
  const setup = async (model: ChannelModel) => {
    currentModel = model;
    await applyTopology(() => model.createChannel());
    await inSequence(live, (entry) => attach(() => model.createChannel(), entry));
    connected = true;
  };

  /** The same for the publishing connection, which owns the confirm channel and nothing else. A broker alarm blocks it, so it says so. */
  const publishingSetup = async (model: ChannelModel) => {
    currentPublishModel = model;
    model.on("blocked", (reason: string) => void warn(`publishing connection blocked by the broker: ${reason}`));
    model.on("unblocked", () => void warn("publishing connection unblocked"));
    // Opened eagerly, so the first publish does not pay for it.
    out = watchPublishChannel(await model.createConfirmChannel());
    publishing = true;
  };

  const open = (setup: (model: ChannelModel) => Promise<void>, onClose: () => void) =>
    Effect.acquireRelease(
      wrap("connect", () =>
        amqp.connect(
          {
            protocol: "amqp",
            hostname: opts.host,
            port: opts.port,
            username: opts.username ?? "guest",
            password: opts.password ?? "guest",
            // Unset negotiates RabbitMQ's 60s default, far too slow to notice a one-sided partition.
            heartbeat: 1,
          },
          {
            // Bounds every connect attempt to a fixed wall-clock time instead of the OS's ~135s SYN-retry timeout.
            timeout: 5000,
            recovery: {
              initialDelay: 200,
              maxDelay: 5000,
              // About five minutes of trying before the process gives up.
              maxRetries: 60,
              // amqplib listens for a new connection's `error` only once setup has finished, so an error during
              // the replay — a missed heartbeat, a fatal close — had no listener and crashed the process
              // (ADR 005). With one, the replay's pending calls reject and recovery schedules another attempt.
              setup: (model: ChannelModel) => {
                model.on("error", (error: Error) => void warn(`connection error during setup: ${error.message}`));
                return setup(model);
              },
            },
          },
        ),
      ),
      // Swallowed: `close` rejects if the broker has already gone. Consumers are forgotten first so the
      // resulting channel closes aren't read as ones to repair.
      (conn) =>
        Effect.promise(() => {
          onClose();
          return conn.close().then(() => { }, () => { });
        }),
    );

  const connection = yield* open(setup, () => live.clear());
  const publisher = yield* open(publishingSetup, () => {});

  /** A numeric `.code` is an AMQP reply code (e.g. 406 `PRECONDITION_FAILED`) — "the broker says no", not "isn't there". */
  const amqpReplyCode = (error: Error): O.Option<number> =>
    O.liftPredicate((error as { readonly code?: unknown }).code, Predicate.isNumber);

  /** Wires one connection's events to the shared bookkeeping; either connection failing for good fails `lost`. */
  const watch = (conn: typeof connection, role: string, up: (state: boolean) => void, restored: () => string) => {
    conn.on("error", (error) => {
      warn(`${role} connection error: ${error.message}`);
    });
    conn.on("disconnect", (error) => {
      up(false);
      warn(`${role} connection disconnected (${error?.message ?? "no reason given"}) — recovering`);
    });
    /**
     * Fires on every failed reconnect attempt. An AMQP reply code means the broker rejected a frame during
     * `setup` (e.g. a mismatched redeclare) — retrying can't fix that, so the budget is abandoned at once.
     */
    conn.on("connect-failed", (error) => {
      const code = amqpReplyCode(error);
      warn(
        `${role} connect attempt failed${O.match(code, { onNone: () => "", onSome: (c) => ` (AMQP ${c})` })}: ${error.message}`,
      );
      O.map(code, () =>
        Deferred.doneUnsafe(
          lost,
          Effect.fail(
            new RmqError({
              operation: "connection",
              cause: `${role} connection: broker rejected setup deterministically, abandoning retry budget: ${error.message}`,
            }),
          ),
        ),
      );
    });
    conn.on("reconnect-scheduled", ({ attempt, delay, error }) => {
      warn(`${role} reconnect attempt ${attempt} in ${delay}ms (${error.message})`);
    });
    conn.on("connect", () => {
      warn(`${role} connection reconnected — ${restored()}`);
    });
    // Recovery has given up: fail `lost` and let the restart policy take it from here.
    conn.on("reconnect-failed", (error) => {
      Deferred.doneUnsafe(
        lost,
        Effect.fail(
          new RmqError({ operation: "connection", cause: `${role} connection recovery gave up: ${error.message}` }),
        ),
      );
    });
  };
  watch(connection, "consuming", (state) => void (connected = state), () =>
    `${topology.size} topology entries and ${live.size} consumer(s) restored`,
  );
  watch(publisher, "publishing", (state) => void (publishing = state), () => "publish channel reopened");

  /** The live publish channel, opening one if the last was closed under us. */
  const publishChannel = (): Promise<ConfirmChannel> =>
    O.match(O.fromNullOr(out), {
      onSome: (open) => Promise.resolve(open),
      // One reopen at a time: racing sends must not each open a channel and orphan all but one.
      onNone: () =>
      (opening ??= publisher.createConfirmChannel().then(
        (ch) => {
          out = watchPublishChannel(ch);
          opening = null;
          return ch;
        },
        (error) => {
          opening = null;
          throw error;
        },
      )),
    });

  /** A throwaway channel each: a mismatched redeclare closes its channel (`PRECONDITION_FAILED`), not the publish path. */
  const onFreshChannel = <A>(operation: string, use: (ch: Channel) => Promise<A>) =>
    Effect.acquireUseRelease(
      wrap(operation, () =>
        connection.createChannel().then((ch) => {
          // A failed declare closes its channel; without a listener that 'error' would reach the process.
          ch.on("error", () => { });
          return ch;
        }),
      ),
      (ch) => wrap(operation, () => use(ch)),
      // Closing is best effort by definition: the channel this runs on may be
      // the one the broker just closed under us.
      (ch) => Effect.promise(() => ch.close().then(() => { }, () => { })),
    );

  /** Resolves only once the broker confirms — also the backpressure: an awaiting caller can't outrun it. */
  const publish = (pub: Publisher, content: Buffer, options: amqp.Options.Publish) =>
    publishChannel().then(
      (ch) =>
        new Promise<void>((resolve, reject) => {
          const fail = (error: Error) => {
            pending.delete(fail);
            reject(error);
          };
          pending.add(fail);
          ch.publish(pub.exchange, pub.routingKey, content, options, (error) =>
            // Skipped when a close already failed it.
            when(pending.delete(fail), () => {
              // A `basic.return` arrives before the confirm for the same message,
              // so by now the id is here if the broker could not route it.
              const returned = unroutable.delete(String(options.messageId));
              return returned
                ? reject(new Unroutable(`no queue for ${pub.exchange || "(default)"}/${pub.routingKey}`))
                : error
                  ? reject(error instanceof Error ? error : new Error(String(error)))
                  : resolve();
            }),
          );
        }),
    );

  /** Forget a consumer, so a recovery does not bring back one we retired. */
  const forget = (c: Consumer) => {
    O.map(Arr.findFirst(live, (entry) => entry.handle === c), (entry) => live.delete(entry));
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
        // The handle starts on the channel its first consumer will use, so `attach` finds nothing stale to close.
        const first = await connection.createChannel();
        const handle: Consumer = { channel: first, consumerTag: "" };
        const entry: Live = {
          handle,
          queue,
          onMessage,
          prefetch: options.prefetch ?? DEFAULT_PREFETCH,
        };
        // In `live` before `attach`: `attach` registers the channel's 'close' handler before it finishes, and a
        // channel that dies in that window would be dropped by `rebuild` (`!live.has(entry)`), leaving the consumer
        // deaf. Roll back on failure so a failed `consume` leaves no dead entry for `setup`.
        live.add(entry);
        try {
          await attach(() => Promise.resolve(first), entry);
        } catch (error) {
          live.delete(entry);
          throw error;
        }
        return handle;
      }),
    get: (queue) =>
      wrap("get", async () => {
        const ch = await connection.createChannel();
        ch.on("error", () => {});
        const msg = await ch.get(queue, { noAck: false });
        if (msg === false) {
          await ch.close().catch(() => {});
          return O.none();
        }
        // Guarded rather than trusted, the same defensiveness `settle` has for a channel that closed under a
        // caller sitting on an unsettled delivery — every caller here settles a `get` exactly once.
        let settled = false;
        const settleOnce = (act: () => void) =>
          Effect.sync(() => {
            if (settled) return;
            settled = true;
            try {
              act();
            } catch {
              // channel already gone; the broker has the delivery back
            }
            ch.close().catch(() => {});
          });
        return O.some({
          body: msg.content.toString("utf8"),
          properties: stringifyHeaders(msg.properties.headers ?? {}),
          messageId: O.liftPredicate(msg.properties.messageId, Predicate.isString),
          ack: settleOnce(() => ch.ack(msg)),
          nack: settleOnce(() => ch.nack(msg, false, true)),
        });
      }),
    publisherToExchange: (exchange, routingKey, format) =>
      Effect.succeed({
        exchange,
        routingKey,
        contentType: O.fromNullishOr(format?.contentType),
        contentEncoding: O.fromNullishOr(format?.contentEncoding),
        type: O.fromNullishOr(format?.type),
        mandatory: false,
      }),
    publisherToQueue: (queue, format) =>
      // The default exchange routes by queue name, which is the same path
      // `deadLetterArgs` uses for dead-lettering.
      Effect.succeed({
        exchange: "",
        routingKey: queue,
        contentType: O.fromNullishOr(format?.contentType),
        contentEncoding: O.fromNullishOr(format?.contentEncoding),
        type: O.fromNullishOr(format?.type),
        mandatory: true,
      }),
    // The `traceparent` goes on as an ordinary header when the caller is inside a span; outside one nothing is added (see Trace.ts).
    send: (pub, body, options) =>
      Effect.flatMap(traceparent, (tp) => {
        const properties = options?.headers;
        const headers = O.match(tp, {
          onNone: () => O.fromNullishOr(properties),
          onSome: (value) => O.some({ ...(properties ?? {}), [TRACEPARENT]: value }),
        });
        return wrap("send", () =>
          publish(
            pub,
            Buffer.from(body, "utf8"),
            {
              persistent: true,
              mandatory: pub.mandatory,
              // The id ties a broker's `basic.return` to this publish; both are stamped here so no caller has to remember.
              messageId: O.getOrElse(O.fromNullishOr(options?.messageId), () => randomUUID()),
              timestamp: Math.floor(Date.now() / 1000),
              ...O.match(headers, { onNone: () => ({}), onSome: (h) => ({ headers: h }) }),
              ...O.match(pub.contentType, { onNone: () => ({}), onSome: (c) => ({ contentType: c }) }),
              ...O.match(pub.contentEncoding, { onNone: () => ({}), onSome: (c) => ({ contentEncoding: c }) }),
              ...O.match(pub.type, { onNone: () => ({}), onSome: (t) => ({ type: t }) }),
            },
          ),
        );
      }),
    cancelConsumer: (c) =>
      Effect.promise(() => {
        forget(c);
        return c.channel.cancel(c.consumerTag).then(() => { }, () => { });
      }),
    closeConsumer: (c) =>
      Effect.promise(() => {
        forget(c);
        return c.channel.close().then(() => { }, () => { });
      }),
    lost: Deferred.await(lost),
    isConnected: Effect.sync(() => connected && publishing),
    // `destroy(error)`, not `destroy()`: amqplib only wires `onSocketError` to the stream's 'error' and 'end', and a
    // bare destroy emits neither, so the socket would die without amqplib noticing or reconnecting.
    resetConnection: Effect.sync(() =>
      // Both sockets: the publishes buffered on one are what this fences, and a reset process should come back whole.
      [currentModel, currentPublishModel].forEach((model) =>
        O.fromNullishOr(model).pipe(
          O.flatMap((m) =>
            O.fromNullishOr(
              (m.connection as { readonly stream?: { destroy: (err?: Error) => void } }).stream,
            ),
          ),
          O.map((stream) =>
            stream.destroy(new Error("connection reset: fencing a demoted leader's buffered publishes")),
          ),
        ),
      ),
    ),
  });
});


/**
 * `Layer.launch` for a graph that contains an `Rmq`: runs until the scope ends, the connection is lost, or
 * `alsoFatal` completes. One call site for the fatal-on-lost-connection decision, since `Layer.launch` alone
 * blocks forever and a defect in a forked fiber can't end it.
 */
export const launchWithRmq = <ROut, E, RIn, E2 = never>(
  layer: Layer.Layer<ROut | Rmq, E, RIn>,
  alsoFatal: Effect.Effect<never, E2, never> = Effect.never,
): Effect.Effect<never, E | RmqError | E2, RIn> =>
  Effect.scoped(
    Effect.flatMap(Layer.build(layer), (context) =>
      Effect.raceFirst(Context.get(context, Rmq).lost, alsoFatal),
    ),
  );
