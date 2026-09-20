import { randomUUID } from "node:crypto";
import { Array as Arr, Context, Data, Deferred, Effect, Layer, Match, Option as O, Predicate, Scope, Tracer } from "effect";
import * as amqp from "amqplib";
import { parentFrom, TRACEPARENT, traceparent } from "./Trace.ts";
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
export type Publisher = {
  readonly exchange: string;
  readonly routingKey: string;
  /**
   * What this publisher says its bodies are, sent as the AMQP `content_type`
   * and `content_encoding` properties on every message (`application/json`;
   * `gzip` if compressed, several separated by commas). RabbitMQ neither
   * validates nor uses them: they are for the applications on either side, so
   * a publisher declares its format once and a reader can decline what it does
   * not understand instead of guessing. See
   * https://www.rabbitmq.com/docs/consumers#content-type-and-encoding.
   */
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  /** The AMQP `type` property: what kind of message this is, dot-separated by convention (`egress.work`). */
  readonly type: O.Option<string>;
  /**
   * Whether the broker must hand back a message it cannot route, which `send`
   * then fails with `Unroutable`. On for a queue, where "no such queue" is a bug
   * and the default exchange would otherwise drop the message without a word;
   * off for an exchange, where a topic with no bindings yet is an ordinary state.
   */
  readonly mandatory: boolean;
};

/** What a publisher may declare about its bodies; all optional, and a publisher that declares nothing says nothing. */
export type Format = {
  readonly contentType?: string;
  readonly contentEncoding?: string;
  readonly type?: string;
};

/**
 * What a single publish may carry beyond its body. `headers` become message
 * headers. `messageId` is the message's identity, the AMQP `message_id`: the
 * publisher assigns it when the message has an identity that must outlive one
 * publish (a retry key), and a republish carries it forward explicitly. Left out,
 * `send` invents one, which is all the broker-side bookkeeping needs.
 */
export type SendOptions = {
  readonly headers?: Record<string, string>;
  readonly messageId?: string;
};

/**
 * The broker returned a message it could not route to any queue (a mandatory
 * publish to a queue that does not exist). Its own class, like `PublishNacked`
 * would be: the message was not taken, and that is knowable.
 */
export class Unroutable extends Error {}

/** True when this failure is the broker handing a mandatory message back as unroutable. */
export const isUnroutable = (error: RmqError): boolean => error.cause instanceof Unroutable;

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
   * The AMQP `content_type` and `content_encoding` the publisher declared,
   * `None` from a publisher that declared nothing. Read directly, like
   * `messageId`, since a daemon decides on them before it looks at the body.
   */
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  /** The AMQP `type`, `message_id` and `timestamp` the publisher stamped (`send` stamps the last two on every message). */
  readonly type: O.Option<string>;
  readonly messageId: O.Option<string>;
  /** When it was published, epoch milliseconds: AMQP carries seconds, so this is only as fine as a second. */
  readonly publishedAt: O.Option<number>;
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
  /** An exchange-to-exchange binding: what a delay chain is made of. */
  readonly bindExchange: (
    routingKey: string,
    source: RmqExchange,
    destination: RmqExchange,
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
    format?: Format,
  ) => Effect.Effect<Publisher, RmqError>;
  readonly publisherToQueue: (queue: string, format?: Format) => Effect.Effect<Publisher, RmqError>;
  /**
   * `properties` become message headers. Everything is published persistent, with
   * no flag: on a transient queue the broker ignores it, and on a durable one it is
   * the difference between keeping a message across a restart and appearing to.
   */
  readonly send: (pub: Publisher, body: string, options?: SendOptions) => Effect.Effect<void, RmqError>;
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
   * Stop delivery, let what the consumer holds settle, then retire it and its
   * channel. The graceful `closeConsumer`: nothing goes back to the queue, so a
   * consumer that is stopped because a circuit opened does not repeat the calls
   * it had already finished.
   */
  readonly drainConsumer: (c: Consumer) => Effect.Effect<void>;
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
  /**
   * Cheap read of the `connected` flag tracked from amqplib's own events —
   * see that variable's doc comment for exactly when it is honest. It is
   * `true` for most of a one-sided partition until the AMQP heartbeat times
   * out (see `amqp.connect`'s `heartbeat` option, below) — a channel's
   * 'close' arrives before the connection's 'disconnect' either way, so
   * `false` still lags the actual break by whatever that takes. A caller
   * that needs to know whether publishing actually works — not just whether
   * the socket looks open, or a heartbeat frame is still getting through —
   * has to track delivery outcomes itself, which is what
   * AmqpControlPlaneSink's consecutive-failure count is for.
   */
  readonly isConnected: Effect.Effect<boolean>;
  /**
   * Destroy the underlying socket outright — not a graceful close, which
   * would send a Close method and wait on a peer that may be a one-sided
   * partition away from ever answering. Anything already written but
   * unconfirmed (amqplib's own buffering, the kernel socket buffer) is
   * discarded with the socket rather than delivered late, once whatever hid
   * it heals, interleaved with a new leader's sequence.
   *
   * Every pending publish confirm fails as a side effect — the destroy
   * cascades to each channel's own 'close', which the publish channel's
   * listener already turns into a rejection — so a delivery stuck on this
   * connection counts as failed rather than hanging. Recovery is the same
   * path a real network partition already takes: `setup` reconnects and
   * replays topology and consumers (see docs/decisions/005-connection-recovery.md).
   *
   * For a caller demoted from leadership, not for routine use — see
   * AmqpControlPlaneSink's `resetConnection` and `Aggregator.ts`'s
   * `demoteAndFence`.
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

/**
 * Computed on the first read and kept. The high-rate handlers never read most
 * of what a delivery can describe, so nothing is worked out until asked. The
 * memo is an `Option` because a computed value is not the same thing as one
 * that is itself absent.
 */
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

const describe = (delivery: ConsumeMessage): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  const header = (name: string) => O.liftPredicate(headers[name], Predicate.isString);
  const deadLetter = lazily(() =>
    O.map(header("x-first-death-queue"), (queue) => ({
      queue,
      reason: O.getOrElse(header("x-first-death-reason"), () => "unknown"),
    })),
  );
  const properties = lazily(() =>
    Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, String(v)])),
  );
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
    }
    | {
      readonly kind: "exchangeBind";
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
    /** Deliveries handed to `onMessage` and not yet settled on the broker. */
    outstanding: number;
    /** Resolved when `outstanding` next reaches zero. */
    readonly idle: Array<() => void>;
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
  /** The same for the publishing connection, which fails and recovers on its own. */
  let publishing = true;

  /**
   * `out`/`opening` are `null` rather than `Option`: private mutable interop
   * state, not a value anyone outside this closure branches on. See
   * docs/decisions/006-representing-absence.md.
   */
  let out: ConfirmChannel | null = null;
  let opening: Promise<ConfirmChannel> | null = null;
  /** Publishes awaiting a confirm on `out`, so its 'close' listener can fail all of them at once. */
  const pending = new Set<(error: Error) => void>();
  /** `message_id`s the broker has handed back as unroutable and whose confirm has not yet arrived. */
  const unroutable = new Set<string>();

  /**
   * The current live model, captured from `setup` (which amqplib runs on
   * every successful connect, initial and reconnect alike, before handing
   * the connection to anyone) rather than from the `'connect'` event: that
   * event fires synchronously inside amqplib's own connect chain, before the
   * `amqp.connect()` promise this closure awaits below has even resolved, so
   * subscribing to it afterwards would miss exactly the first connection.
   *
   * `ChannelModel.connection` is typed as `{ serverProperties }` only, but
   * the runtime object is connection.js's full `Connection`, which owns the
   * wrapped socket as `.stream` — see `resetConnection`.
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
          // The broker cancelled this consumer — its queue was deleted underneath
          // it. There is no delivery to settle and nothing to rebuild: the
          // consumer is simply not receiving any more, which is worth saying,
          // because the channel stays open and looks healthy.
          onNone: () => void warn(`broker cancelled the consumer on ${entry.queue} — it receives nothing now`),
          onSome: (delivery) => handle(ch, entry, delivery),
        });

  const isPending = (
    done: void | Settlement | PromiseLike<void | Settlement>,
  ): done is PromiseLike<void | Settlement> => Predicate.isPromiseLike(done);

  /**
   * A handler that threw, or whose promise rejected, did not finish its work, so
   * acknowledging the delivery would say it had. It is dead-lettered instead
   * (`discard` is `reject` without requeue): kept where the queue has somewhere
   * to put it, and never a tight requeue loop on a queue with no delivery limit.
   * Logged, because a handler that fails without a trace is how a queue quietly
   * empties into a dead-letter queue nobody reads.
   */
  const failed = (ch: Channel, entry: Live, message: ConsumeMessage, error: unknown): void => {
    void warn(`handler for ${entry.queue} failed, dead-lettering the delivery: ${String(error)}`);
    settle(ch, message, "discard");
  };

  /** One delivery settled: the last one out wakes whoever is draining this consumer. */
  const released = (entry: Live): void => {
    entry.outstanding -= 1;
    O.map(
      O.liftPredicate(entry, (e: Live) => e.outstanding === 0),
      (e) => e.idle.splice(0).forEach((resolve) => resolve()),
    );
  };

  const handle = (ch: Channel, entry: Live, message: ConsumeMessage): void => {
    entry.outstanding += 1;
    // A handler that throws synchronously would escape into amqplib's
    // delivery callback. Every handler in this repo is careful, which is
    // exactly the kind of thing that stops being true later.
    let done: void | Settlement | PromiseLike<void | Settlement>;
    try {
      done = entry.onMessage(message.content.toString("utf8"), describe(message));
    } catch (error) {
      failed(ch, entry, message, error);
      return released(entry);
    }
    Match.value(done).pipe(
      // A synchronous outcome is a string, not a thenable.
      Match.when(Predicate.isString, (outcome) => {
        settle(ch, message, outcome);
        released(entry);
      }),
      Match.when(isPending, (later) =>
        void later.then(
          (outcome) => settle(ch, message, outcome ?? "accept"),
          (error) => failed(ch, entry, message, error),
        ).then(() => released(entry)),
      ),
      Match.orElse(() => {
        settle(ch, message, "accept");
        released(entry);
      }),
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
    O.liftPredicate(entry, (e: Live) => live.has(e)).pipe(
      O.map((current) =>
        attach(() => connection.createChannel(), current).then(
          () => warn(`consumer channel on ${current.queue} closed — rebuilt`),
          (error) =>
            // `connected` is false by now if the connection is what went, and
            // `setup` re-attaches everything when it returns. Saying so here would
            // report a failure that is already being handled.
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
    // The usual path here is `rebuild`, where `previous` already fired its
    // own 'close' and this is a no-op. But `setup` re-attaches every live
    // entry after a reconnect, and a connection-level failure does not
    // reliably fire 'close' on each of its channels first — so `previous`
    // can still be open, still registered as a consumer on the broker, with
    // nothing left in this process pointing at it. Close it explicitly
    // rather than trust the broker to notice on its own: an implicit,
    // un-acked close is exactly how a stale consumer outlives the channel
    // that should have taken it down, and why the broker's consumer count
    // climbs while this process only ever sees five.
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
            exchangeBind: (b) => ch.bindExchange(b.destination, b.source, b.routingKey),
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
   * Runs after every successful connect of the consuming connection, before it
   * is handed out. Uses the model it is given rather than the recovering
   * wrapper, which is not serving connections yet and would deadlock waiting
   * for this one.
   *
   * Nothing here publishes, on purpose. A broker alarm blocks a connection by
   * ceasing to read from it, and RabbitMQ's own advice is "to only use
   * individual connections for either producing or consuming"
   * (https://www.rabbitmq.com/docs/alarms): measured on this client, a consumer
   * on a connection that had published held its prefetch window unacknowledged
   * for as long as the alarm lasted, because its acks were never read.
   */
  const setup = async (model: ChannelModel) => {
    currentModel = model;
    await applyTopology(() => model.createChannel());
    await inSequence(live, (entry) => attach(() => model.createChannel(), entry));
    connected = true;
  };

  /**
   * The same, for the publishing connection: it owns the confirm channel and
   * nothing else. It is the one a broker alarm blocks, so it is also the one that
   * says so; a blocked publisher otherwise just waits on its confirm with
   * nothing to say why.
   */
  const publishingSetup = async (model: ChannelModel) => {
    currentPublishModel = model;
    model.on("blocked", (reason: string) => void warn(`publishing connection blocked by the broker: ${reason}`));
    model.on("unblocked", () => void warn("publishing connection unblocked"));
    // Always reopened, same as it always has been: most processes never publish
    // before their first real event, but the ones that do should not pay for
    // opening it there.
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
            // Unset, this negotiates RabbitMQ's own 60s default — far too slow
            // to matter for Coordination.ts's 5000ms lease TTL. amqplib already
            // independently tracks broker activity on both sides of the
            // connection and closes it (emitting the same 'disconnect' any
            // other failure does, which `connected` below already turns into
            // `isConnected`) after ~2-3 missed intervals, with zero application
            // code — so a short interval here does for a silent one-sided
            // partition what an application-level heartbeat would otherwise
            // have to be built to do. Measured live against
            // `net-control-partition+outage`, a one-directional packet drop:
            // the broker's own side of the heartbeat timed out first, closed
            // the connection, and that reached this side as `ECONNRESET` at
            // 2.76s — `AmqpControlPlaneSink.ts`'s per-attempt `isConnected`
            // check (see its own comment) turned that into a step-down at
            // 3.46s, matching what a bespoke message-level heartbeat had taken
            // a whole ADR's worth of machinery to achieve no faster. See
            // docs/decisions/017's second amendment.
            heartbeat: 1,
          },
          {
            // Bounds every socket connect — initial and every reconnect alike,
            // since amqplib reuses these options on each attempt — to a fixed
            // wall-clock time rather than the OS's own SYN-retry timeout.
            // Without this, a one-sided network partition (outbound packets to
            // the broker silently dropped rather than refused or reset) leaves
            // `net.connect` retrying at the kernel level: measured against a
            // real one, three consecutive reconnect attempts each took ~135s to
            // fail — Linux's default `tcp_syn_retries` — before amqplib's own
            // `setTimeout` backoff even got a turn. A `maxRetries: 60` budget
            // meant to take "about five minutes" (see `recovery` below and
            // docs/decisions/005-connection-recovery.md) would have taken over
            // two hours instead, indistinguishable from hung to anything
            // watching less than that. 5s comfortably covers a real connect on
            // this network and is the same order of magnitude as `maxDelay`
            // below, so a partition now fails each attempt fast enough that the
            // documented five-minute budget is the actual bound again.
            timeout: 5000,
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
          onClose();
          return conn.close().then(() => { }, () => { });
        }),
    );

  const connection = yield* open(setup, () => live.clear());
  const publisher = yield* open(publishingSetup, () => {});

  /**
   * A numeric `.code` on an amqplib error is an AMQP reply code — the broker
   * accepted the connection, parsed a frame, and rejected it for a protocol
   * reason (channel.js's `ChannelClose` handler stamps `error.code` from the
   * close frame's `replyCode`, e.g. 406 for `PRECONDITION_FAILED`). A
   * network-level failure — broker unreachable, connection refused or reset —
   * never has one: those surface as Node's own string error codes
   * (`ECONNREFUSED` and the like) or no `.code` at all, because the broker
   * never got far enough to reject anything. That split is what tells "the
   * broker is actively saying no" from "the broker is not there right now".
   */
  const amqpReplyCode = (error: Error): O.Option<number> =>
    O.liftPredicate((error as { readonly code?: unknown }).code, Predicate.isNumber);

  /**
   * Wires one connection's events to the shared bookkeeping. Both connections
   * report through the same handlers: either one failing for good fails `lost`,
   * since a process with half its broker access is no use either.
   */
  const watch = (conn: typeof connection, role: string, up: (state: boolean) => void, restored: () => string) => {
    conn.on("error", (error) => {
      warn(`${role} connection error: ${error.message}`);
    });
    conn.on("disconnect", (error) => {
      up(false);
      warn(`${role} connection disconnected (${error?.message ?? "no reason given"}) — recovering`);
    });
    /**
     * Fires on every failed reconnect attempt, not just the last —
     * recovery.js's `_connect()` emits it from its `catch` before scheduling
     * the next try. Nothing listened here before: a deterministic failure (the
     * broker closing `setup`'s topology replay with the same
     * `PRECONDITION_FAILED` on every attempt, e.g. a queue redeclared with
     * different arguments — see docs/decisions/016's churn on `x-delivery-limit`)
     * retried silently for up to five minutes before `reconnect-failed` said
     * anything at all, which is indistinguishable from hung to anyone watching
     * for less than five minutes. Measured against a real broker: 21 attempts
     * and 84 seconds of exactly this silence before this handler existed.
     *
     * An AMQP reply code here (see `amqpReplyCode`) can only mean the broker
     * itself rejected a frame during `setup` — retrying the identical topology
     * against it cannot succeed without a human fixing the mismatch, so this
     * abandons the retry budget immediately (failing `lost`, same as
     * `reconnect-failed` below) rather than burning the full five minutes on a
     * setup that is doomed on attempt 1 as surely as on attempt 60. A
     * transient failure — broker unreachable, connection refused — has no such
     * code and keeps its full budget; `RecoveringCore` schedules its next
     * attempt right after this handler returns, unless `lost` failing has
     * already torn the connection down.
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
    // Recovery has given up. The stance is unchanged — a process that cannot
    // reach its broker is no use, and the restart policy is what gets it looked
    // at — but it is now a failure that travels, not an exit taken here.
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
      // One reopen at a time. Several sends racing here must not each open a
      // channel and leave all but one orphaned on the broker.
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
          ch.on("error", () => { });
          return ch;
        }),
      ),
      (ch) => wrap(operation, () => use(ch)),
      // Closing is best effort by definition: the channel this runs on may be
      // the one the broker just closed under us.
      (ch) => Effect.promise(() => ch.close().then(() => { }, () => { })),
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
        // The handle exists before its first consumer does, so it starts on the
        // channel that consumer will use: `attach` then finds nothing stale to
        // close, and there is no half-built handle to point at nothing.
        const first = await connection.createChannel();
        const handle: Consumer = { channel: first, consumerTag: "" };
        const entry: Live = {
          handle,
          queue,
          onMessage,
          prefetch: options.prefetch ?? DEFAULT_PREFETCH,
          outstanding: 0,
          idle: [],
        };
        // In `live` before `attach`, not after: `attach` registers the
        // channel's 'close' handler before it finishes, and a channel that
        // dies in that window would find `rebuild` seeing `!live.has(entry)`
        // and silently give up on it — the "goes deaf" failure `rebuild`'s
        // own comment warns about, for a queue that never got the chance to
        // be rebuilt once. Roll back on failure so a `consume` that never
        // succeeded doesn't leave a dead entry for `setup` to trip over.
        live.add(entry);
        try {
          await attach(() => Promise.resolve(first), entry);
        } catch (error) {
          live.delete(entry);
          throw error;
        }
        return handle;
      }),
    bindExchange: (routingKey, source, destination) => {
      record(`e:${String(source)}:${routingKey}:${String(destination)}`, {
        kind: "exchangeBind",
        routingKey,
        source: source as string,
        destination: destination as string,
      });
      return onFreshChannel("bindExchange", async (ch) => {
        await ch.bindExchange(destination as string, source as string, routingKey);
      }).pipe(Effect.asVoid);
    },
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
    // The `traceparent` goes on as an ordinary header, from whatever span the
    // caller is inside. Outside a span there is none and nothing is added, so
    // an untraced publish carries exactly the bytes it did before — see Trace.ts.
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
              // The id is what ties a broker's `basic.return` to this publish, and
              // the timestamp is the AMQP-standard "when", both stamped here so no
              // caller has to remember.
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
    drainConsumer: (c) =>
      Effect.promise(async () => {
        const entry = Arr.findFirst(live, (e) => e.handle === c);
        forget(c);
        await c.channel.cancel(c.consumerTag).then(() => { }, () => { });
        // What it still holds is settled on this channel, so the channel stays
        // open until the last of it is — closing first would hand each back
        // unacked, and a call that already succeeded would run again.
        await O.match(entry, {
          onNone: () => Promise.resolve(),
          onSome: (e) =>
            e.outstanding === 0 ? Promise.resolve() : new Promise<void>((resolve) => e.idle.push(resolve)),
        });
        await c.channel.close().then(() => { }, () => { });
      }),
    closeConsumer: (c) =>
      Effect.promise(() => {
        forget(c);
        return c.channel.close().then(() => { }, () => { });
      }),
    lost: Deferred.await(lost),
    isConnected: Effect.sync(() => connected && publishing),
    // `destroy(error)`, not `destroy()`: amqplib only wires `onSocketError`
    // to the stream's 'error' and 'end' events, and a plain destroy with no
    // argument emits neither — Node's Duplex emits bare 'close' for that, which
    // nothing here listens for. Without the error this would be a silent
    // no-op: the socket dies, but amqplib's Connection never notices, never
    // closes its channels, and never tells RecoveringCore to reconnect.
    resetConnection: Effect.sync(() =>
      // Both sockets: what this fences is the publishes buffered on one, and a
      // process that resets should come back whole rather than half connected.
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
 * Build `layer` and run until its scope ends, the broker connection is lost,
 * or `alsoFatal` completes — whichever comes first. `Layer.launch` for a
 * graph that contains an `Rmq`.
 *
 * This exists so the fatal-on-lost-connection decision has exactly one call
 * site. `Layer.launch` alone blocks forever, and a fiber forked into the
 * layer's scope cannot end it: measured, a defect in one leaves `Layer.launch`
 * running. So the guarantee has to be on the fiber that launches, and putting
 * it here rather than in each `main.ts` is the difference between a rule and
 * three places that have to remember it.
 *
 * `alsoFatal` is the same gap for the caller's own long-running fiber (the
 * daemon loop, the producer loop): forked into the layer with `forkScoped`,
 * its defects are just as invisible to `Layer.launch` as a lost connection
 * would be without this function. A caller with such a fiber catches its
 * defect, fails a `Deferred` from that handler, and passes `Deferred.await`
 * of it here — the same `Fatal`-deferred pattern every entry point in this
 * repo uses for its own long-running fiber.
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
