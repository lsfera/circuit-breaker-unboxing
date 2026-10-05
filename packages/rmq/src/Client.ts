import { AMQPClient } from "@cloudamqp/amqp-client";
import type { AMQPChannel, AMQPConsumer, AMQPMessage, AMQPProperties, Field } from "@cloudamqp/amqp-client";
import type { AMQPView } from "@cloudamqp/amqp-client/amqp-view";
import { Context, Data, Deferred, Effect, Layer, Match, Option as O, Predicate, Record as Rec } from "effect";
import type { Scope, Tracer } from "effect";
import { assertSupportedRabbitMqVersion, UnsupportedRabbitMqVersionError } from "./RabbitMqVersion.ts";
import { parentFrom, TRACEPARENT, traceparent } from "./Trace.ts";

/**
 * A channel plus the tag the broker gave it. Mutable on purpose: recovery repoints this same
 * object at a rebuilt channel, so a caller's handle stays valid.
 */
export type Consumer = { channel: AMQPChannel; consumerTag: string; };

/**
 * What a publisher declares about its bodies, sent on every message; one that declares nothing says nothing.
 * `contentType`/`contentEncoding` (`application/json`; `gzip`, comma-separated): RabbitMQ ignores them, a reader
 * uses them to decline what it cannot decode. `type`: what kind of message this is (`egress.work`).
 */
export type Format = {
  readonly contentType?: string;
  readonly contentEncoding?: string;
  readonly type?: string;
};

/** A publisher is an address, not a link: AMQP 0-9-1 takes exchange and routing key per publish, so nothing to open or race. */
export type Publisher = {
  readonly exchange: string;
  readonly routingKey: string;
  readonly format: Format;
  /**
   * Whether the broker must return an unroutable message, which `send` then fails with `Unroutable`.
   * On for a queue, where the default exchange would drop it silently; off for an exchange, where no
   * bindings yet is ordinary.
   */
  readonly mandatory: boolean;
};

/**
 * What a publish may carry beyond its body. `headers` become message headers. `messageId` is the AMQP
 * `message_id`: set it when the message's identity must outlive one publish (a retry key); a republish
 * carries it forward. Left out, `send` invents one. `format`, when given, replaces the publisher's.
 */
export type SendOptions = {
  readonly headers?: Record<string, string>;
  readonly messageId?: string;
  readonly format?: Format;
};

/** Text is sent as UTF-8; bytes are sent as they are. */
export type Body = string | Uint8Array;

/** One message of a `sendBatch`. */
type BatchMessage = SendOptions & { readonly body: Body; };

/**
 * One message fetched by `get`, held unsettled until `ack`/`nack` runs — `ack` drops it, `nack` requeues it.
 * `Settlement`'s `requeue`/`release` split doesn't apply here: just "done with it" or "put it back".
 */
export type GotMessage = DeliveryInfo & {
  readonly body: Uint8Array;
  readonly ack: Effect.Effect<void>;
  readonly nack: Effect.Effect<void>;
};

/**
 * What a republish carries of the message it moves, so it is still the same message: its `message_id` (the
 * idempotency key), its declared format, and its trace. Broker annotations are not carried; `headers` are added.
 */
export const carry = (from: DeliveryInfo, headers: Record<string, string>): SendOptions => ({
  messageId: O.getOrUndefined(from.messageId),
  format: {
    contentType: O.getOrUndefined(from.contentType),
    contentEncoding: O.getOrUndefined(from.contentEncoding),
    type: O.getOrUndefined(from.type)
  },
  headers: { ...Rec.filter(from.properties, (_, key) => key === TRACEPARENT), ...headers }
});

/** The broker returned a mandatory message it could not route to any queue. */
class Unroutable extends Error {}

/** True when this failure is the broker handing a mandatory message back as unroutable. */
export const isUnroutable = (error: RmqError): boolean => error.cause instanceof Unroutable;

/**
 * Effect wrapper over `@cloudamqp/amqp-client` (AMQP 0-9-1).
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
 * What a handler asks the broker to do with its delivery: `accept` (the default) drops it; `requeue` puts it back,
 * counted toward `x-delivery-limit`; `release` puts it back uncounted, for a message that did not fail; `discard`
 * dead-letters it where a target is declared and drops it otherwise.
 */
export type Settlement = "accept" | "requeue" | "discard" | "release";

/** What the broker knows about this particular delivery. */
export type DeliveryInfo = {
  /**
   * The broker's `x-delivery-count`, 0 on a first delivery. For looking at: enforcement is the queue's
   * `x-delivery-limit`, because an in-process counter dies when a message changes consumer.
   */
  readonly deliveryCount: number;
  /**
   * Where this message was dead-lettered from (`x-first-death-*`), `None` if it arrived normally.
   * Lazy: the high-rate handlers never read it.
   */
  readonly deadLetter: O.Option<{ readonly queue: string; readonly reason: string; }>;
  /**
   * Headers carried on the message, as strings. Broker death annotations are lost when anything
   * republishes, so a consumer that moves messages inside the dead-letter queue carries provenance here.
   */
  readonly properties: Readonly<Record<string, string>>;
  /**
   * The `content_type`/`content_encoding` the publisher declared, `None` if it declared nothing.
   * Read directly: a daemon decides on them before it looks at the body.
   */
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
  /**
   * Durable unless told otherwise: RabbitMQ 4.3 closes the connection on a transient queue that is not
   * exclusive, and this client only declares non-exclusive ones.
   */
  readonly declareQueue: (
    name: string,
    options?: { readonly args?: QueueArgs; readonly durable?: boolean; }
  ) => Effect.Effect<string, RmqError>;
  readonly declareTopicExchange: (
    name: string,
    options?: { readonly durable?: boolean; }
  ) => Effect.Effect<string, RmqError>;
  readonly bind: (
    routingKey: string,
    source: string,
    destination: string
  ) => Effect.Effect<void, RmqError>;
  /** An exchange-to-exchange binding: what a delay chain is made of. */
  readonly bindExchange: (
    routingKey: string,
    source: string,
    destination: string
  ) => Effect.Effect<void, RmqError>;
  /**
   * Settled only once `onMessage` settles, which is the flow-control lever: a handler that awaits its
   * work holds the delivery unacked, and `prefetch` bounds how many it may hold. A synchronous handler
   * gets no backpressure. The return value chooses the outcome; nothing accepts. A handler that throws or
   * rejects is `discard`ed, not acknowledged as if it had finished.
   */
  readonly consume: (
    queue: string,
    onMessage: (
      body: Uint8Array,
      delivery: DeliveryInfo
    ) => void | Settlement | Promise<void | Settlement>,
    options?: { readonly prefetch?: number; }
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
    format?: Format
  ) => Effect.Effect<Publisher, RmqError>;
  readonly publisherToQueue: (queue: string, format?: Format) => Effect.Effect<Publisher, RmqError>;
  /**
   * `options.headers` become message headers. Always persistent: ignored on a transient queue, the
   * difference between keeping a message across a restart and appearing to on a durable one.
   */
  readonly send: (pub: Publisher, body: Body, options?: SendOptions) => Effect.Effect<void, RmqError>;
  /**
   * Publishes every message back to back on the confirm channel, in order, then waits for all their confirms:
   * one round trip for the batch rather than one per message. Fails if any message was nacked or unroutable;
   * the others may still have been taken, so a caller retrying the batch must tolerate duplicates.
   */
  readonly sendBatch: (pub: Publisher, messages: ReadonlyArray<BatchMessage>) => Effect.Effect<void, RmqError>;
  /**
   * Stop delivery but leave the channel able to settle what it still holds; closing instead would hand
   * that back to the queue.
   */
  readonly cancelConsumer: (c: Consumer) => Effect.Effect<void>;
  /**
   * Retire the consumer and its channel; anything held unacked returns to the queue, and on a quorum queue each of
   * those counts as a failed delivery toward `x-delivery-limit`, as a lost connection does.
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
   * Never completes while the connection is usable; fails once recovery has given up. A lost connection
   * is fatal on purpose (every consumer on it is gone while the process keeps reporting itself active),
   * but the library does not exit: `launchWithRmq` is the one place that turns it into a failure that ends the process.
   */
  readonly lost: Effect.Effect<never, RmqError>;
  /**
   * The `connected` flag tracked from the client's disconnects and reconnects. A one-sided partition looks
   * connected until the heartbeat times out, so `false` lags a real break. A caller that needs to know publishing works must track delivery
   * outcomes itself.
   */
  readonly isConnected: Effect.Effect<boolean>;
  /**
   * Destroy the underlying sockets outright, not a graceful close, which would wait on a peer that may
   * never answer. Buffered, unconfirmed writes go with them instead of arriving late, and every pending
   * publish confirm fails. Recovery is then the ordinary reconnect path (`setup` replays topology and
   * consumers). For a caller that must fence its own in-flight publishes; not routine use.
   */
  readonly resetConnection: Effect.Effect<void>;
}

export class Rmq extends Context.Service<Rmq, RmqService>()("@egress/rmq/Client/Rmq") {
  /** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
  static readonly layer = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

/** Settles `promise` either way: for closes, cancels and settlements on a channel or connection that may already be gone. */
const quietly = (promise: Promise<unknown>): Promise<void> => promise.then(() => {}, () => {});

/**
 * `reject` for `requeue` but `nack` for `release`: from RabbitMQ 4.3 a requeuing `reject` counts toward
 * `x-delivery-limit` and a requeuing `nack` does not. A rejection means the channel is gone, and the broker
 * already has the delivery back.
 */
const settle = (channel: AMQPChannel, message: AMQPMessage, outcome: Settlement): Promise<void> =>
  quietly(
    Match.value(outcome).pipe(
      Match.when("discard", () => channel.basicReject(message.deliveryTag, false)),
      Match.when("requeue", () => channel.basicReject(message.deliveryTag, true)),
      Match.when("release", () => channel.basicNack(message.deliveryTag, true)),
      Match.when("accept", () => channel.basicAck(message.deliveryTag)),
      Match.exhaustive
    )
  );

type RmqConnectOptions = {
  readonly host: string;
  readonly port: number;
  readonly username?: string;
  readonly password?: string;
  /** Seconds, 5 unless given. A test that needs a missed heartbeat inside a few seconds passes 1. */
  readonly heartbeat?: number;
};

/**
 * RabbitMQ defaults to a 60s heartbeat. Five seconds is the shortest RabbitMQ recommends and notices partitions
 * in 10–15s without treating a brief GC pause as a dead connection. The client also uses it as the socket's idle
 * timeout while connecting, so it bounds every connect attempt too, instead of the OS's SYN-retry timeout (~135s on
 * Linux): without that a one-sided partition stalls each attempt so long that the retry budget takes hours.
 */
const amqpUrl = (opts: RmqConnectOptions): string => {
  const url = new URL(`amqp://${opts.host}:${opts.port}/`);
  url.username = encodeURIComponent(opts.username ?? "guest");
  url.password = encodeURIComponent(opts.password ?? "guest");
  url.searchParams.set("heartbeat", String(opts.heartbeat ?? 5));
  return url.href;
};

/** A method frame, and `connection.start` (class 10, method 10). */
const METHOD_FRAME = 1;
const CONNECTION_START = 10;

/**
 * `AMQPClient` reads past the broker's `connection.start` without keeping it. This keeps its server properties,
 * which carry the broker's version: after the 7-byte frame header come class and method (bytes 7–10), the protocol
 * version (11–12), then the table, at 13. The socket client hands `parseFrames` one frame at a time, and
 * `connection.start` is the first the broker sends, so only that one is looked at: anything else, a heartbeat
 * among them, is 8 bytes and too short to read this far into.
 */
class Client extends AMQPClient {
  serverProperties: Readonly<Record<string, Field>> = {};
  private started = false;

  protected override parseFrames(view: AMQPView): void {
    if (!this.started) {
      this.started = true;
      if (
        view.byteLength > 13 && view.getUint8(0) === METHOD_FRAME && view.getUint16(7) === CONNECTION_START &&
        view.getUint16(9) === CONNECTION_START
      ) {
        this.serverProperties = view.getTable(13)[0];
      }
    }
    super.parseFrames(view);
  }
}

const assertClientVersion = (client: Client): void =>
  assertSupportedRabbitMqVersion(String(client.serverProperties["version"] ?? ""));

/**
 * Destroy the socket outright. A failed connect leaves it open (the client rejects on a connect timeout without
 * closing anything), and a graceful close would wait on a peer that may never answer.
 */
const destroy = (client: Client, reason?: Error): void => void client.socket?.destroy(reason);

/** A graceful close, bounded: past `ms` the socket is destroyed instead. */
const closeWithin = (client: Client, ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(() => (destroy(client), resolve()), ms);
    void quietly(client.close()).then(() => (clearTimeout(timer), resolve()));
  });

const checkRabbitMqVersion = async (opts: RmqConnectOptions): Promise<void> => {
  const client = new Client(amqpUrl(opts));
  try {
    await client.connect();
    assertClientVersion(client);
  } finally {
    await closeWithin(client, 1000);
  }
};

/** Runs `run` for each item one after the other, stopping at the first rejection — the order matters and so does not overlap. */
const inSequence = <A>(items: Iterable<A>, run: (item: A) => Promise<unknown>): Promise<unknown> =>
  Array.from(items).reduce<Promise<unknown>>((done, item) => done.then(() => run(item)), Promise.resolve());

/** Computed on first read and kept. The memo is an `Option` because a computed value is not the same as an absent one. */
const lazily = <A>(compute: () => A): () => A => {
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

/** Headers as the client hands them back (values of any field type) to the string-valued shape every caller here wants. */
const stringifyHeaders = (headers: Record<string, unknown>): Readonly<Record<string, string>> =>
  Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, String(v)]));

const describe = (delivery: Pick<AMQPMessage, "properties">): DeliveryInfo => {
  const headers = delivery.properties.headers ?? {};
  const header = (name: string) => O.liftPredicate(headers[name], Predicate.isString);
  const deadLetter = lazily(() =>
    O.map(header("x-first-death-queue"), (queue) => ({
      queue,
      reason: O.getOrElse(header("x-first-death-reason"), () => "unknown")
    }))
  );
  const properties = lazily(() => stringifyHeaders(headers));
  const contentType = O.liftPredicate(delivery.properties.contentType, Predicate.isString);
  const contentEncoding = O.liftPredicate(delivery.properties.contentEncoding, Predicate.isString);
  const type = O.liftPredicate(delivery.properties.type, Predicate.isString);
  const messageId = O.liftPredicate(delivery.properties.messageId, Predicate.isString);
  const publishedAt = O.map(O.liftPredicate(delivery.properties.timestamp, Predicate.isDate), (at) => at.getTime());
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
    }
  };
};

/**
 * The AMQP reply code at the end of a client error (`channel 3 closed: PRECONDITION_FAILED - … (406)`): the broker
 * parsed a frame and rejected it. A network failure never has one (`connect ECONNREFUSED …`, `timeout`).
 */
const amqpReplyCode = (error: Error): O.Option<number> =>
  O.map(O.fromNullOr(/\((\d{3})\)$/.exec(error.message)), ([, code]) => Number(code));

/**
 * A 4xx reply is the broker refusing what it was asked (403 access refused, 404 not found, 406 precondition failed):
 * asked again, it says the same. A 3xx or 5xx is about the connection (320 forced closed, 5xx frame errors), and the
 * next connection may well be fine. That split is "the broker says no" versus "the broker is not there".
 */
const refused = (code: number): boolean => code >= 400 && code < 500;

/** The client's refusal to publish while the broker has the connection blocked. */
const isBlockedRejection = (error: unknown): boolean =>
  error instanceof Error && error.message.startsWith("Connection blocked by server");

/** Reconnect delays: from 200ms, doubling, capped at 5s, each ±20% so a fleet does not reconnect in step. */
const RECONNECT_INITIAL_MS = 200;
const RECONNECT_MAX_MS = 5000;
/** About five minutes of trying before the process gives up. */
const RECONNECT_MAX_ATTEMPTS = 60;

const reconnectDelay = (attempt: number): number => {
  const base = Math.min(RECONNECT_MAX_MS / 1.2, RECONNECT_INITIAL_MS * 2 ** (attempt - 1));
  return Math.round(base * (0.8 + Math.random() * 0.4));
};

/** One connection that reconnects itself, and the hooks a caller watches it through. */
type Link = {
  /** A channel on the live connection; while it is reconnecting, waits for it, and fails once recovery gives up. */
  readonly channel: () => Promise<AMQPChannel>;
  /** The live connection, `null` while reconnecting. */
  readonly current: () => Client | null;
  /** Stops recovery and closes the connection. */
  readonly close: () => Promise<void>;
};

type LinkHooks = {
  readonly role: string;
  /** Runs on every connect, before the connection is handed out. A rejection fails that attempt. */
  readonly setup: (client: Client) => Promise<void>;
  readonly onDown: () => void;
  /** What a reconnect put back, for its log line. */
  readonly restored: () => string;
  readonly warn: (message: string) => void;
  /** Recovery is over, for good. */
  readonly onLost: (error: RmqError) => void;
};

/**
 * Connect, then keep reconnecting with backoff when the connection drops, running `setup` on every connection
 * before anyone else gets it. Resolves with the first connection, or rejects once the attempts run out.
 *
 * A failure retrying cannot fix ends recovery at once: a broker below the supported version, or an AMQP reply code
 * during `setup` (a queue redeclared with different arguments). A transient one (unreachable, refused, timed
 * out) keeps the full budget.
 */
const connectLink = (url: string, hooks: LinkHooks): Promise<Link> => {
  let current: Client | null = null;
  let stopped = false;
  let failure: Error | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let wake: (() => void) | undefined;
  const waiters: Array<{ readonly resolve: (client: Client) => void; readonly reject: (error: Error) => void; }> = [];

  const attempt = async (): Promise<Client> => {
    const client = new Client(url);
    try {
      await client.connect();
      assertClientVersion(client);
      await hooks.setup(client);
      // A connection that dropped while `setup` ran has fired its disconnect with nobody listening yet.
      if (client.closed) throw new Error("connection closed during setup");
      return client;
    } catch (error) {
      destroy(client);
      throw error;
    }
  };

  const bind = (client: Client): void => {
    client.onerror = (error) => void hooks.warn(`${hooks.role} connection error: ${error.message}`);
    client.ondisconnect = (error) => {
      if (current !== client) return;
      current = null;
      if (stopped) return;
      hooks.onDown();
      hooks.warn(`${hooks.role} connection disconnected (${error?.message ?? "no reason given"}) — recovering`);
      void recover(error ?? new Error("connection closed"));
    };
    current = client;
    waiters.splice(0).forEach((waiter) => waiter.resolve(client));
  };

  const abandon = (error: Error): void => {
    stopped = true;
    failure = error;
    waiters.splice(0).forEach((waiter) => waiter.reject(error));
    hooks.onLost(
      new RmqError({ operation: "connection", cause: `${hooks.role} connection recovery gave up: ${error.message}` })
    );
  };

  /** Whether this failure is one retrying cannot fix: reported, and recovery ends with it. */
  const hopeless = (error: Error): boolean => {
    if (error instanceof UnsupportedRabbitMqVersionError) {
      hooks.warn(error.message);
      return true;
    }
    const code = amqpReplyCode(error);
    hooks.warn(
      `${hooks.role} connect attempt failed${
        O.match(code, { onNone: () => "", onSome: (c) => ` (AMQP ${c})` })
      }: ${error.message}`
    );
    return O.exists(code, refused);
  };

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      wake = resolve;
      timer = setTimeout(resolve, ms);
    });

  /** Retries with backoff until connected, out of attempts, or stopped; `connected` hears of the connection it made. */
  const recover = async (cause: Error, connected?: (client: Client) => void): Promise<void> => {
    let last = cause;
    for (let n = 1; n <= RECONNECT_MAX_ATTEMPTS; n++) {
      const delay = reconnectDelay(n);
      hooks.warn(`${hooks.role} reconnect attempt ${n} in ${delay}ms (${last.message})`);
      await sleep(delay);
      if (stopped) return;
      try {
        const client = await attempt();
        if (stopped) return destroy(client);
        bind(client);
        connected?.(client);
        hooks.warn(`${hooks.role} connection reconnected — ${hooks.restored()}`);
        return;
      } catch (error) {
        last = error instanceof Error ? error : new Error(String(error));
        if (hopeless(last)) return abandon(last);
      }
    }
    abandon(last);
  };

  const link: Link = {
    channel: () =>
      current !== null
        ? current.channel()
        : stopped
        ? Promise.reject(failure ?? new Error("connection closed"))
        : new Promise<Client>((resolve, reject) => waiters.push({ resolve, reject })).then((client) =>
          client.channel()
        ),
    current: () => current,
    close: async () => {
      stopped = true;
      clearTimeout(timer);
      wake?.();
      waiters.splice(0).forEach((waiter) => waiter.reject(new Error("connection closed")));
      const client = current;
      current = null;
      if (client !== null) await closeWithin(client, 2000);
    }
  };

  // The first attempt is made at once; only after it fails does the backoff start.
  return attempt().then(
    (client) => (bind(client), link),
    (error: unknown) => {
      const first = error instanceof Error ? error : new Error(String(error));
      if (hopeless(first)) return Promise.reject(first);
      return new Promise<Link>((resolve, reject) => {
        void recover(first, () => resolve(link)).then(() => when(current === null, () => reject(failure ?? first)));
      });
    }
  );
};

/**
 * A connection that repairs itself, released with the scope. The client reconnects nothing on its own, so this
 * reconnects with backoff, records every queue, exchange, binding and consumer, and rebuilds them in `setup`:
 * topology first, then consumers. Past the retry budget, `lost` fails.
 */
export const makeRmq = Effect.fnUntraced(function*(
  opts: RmqConnectOptions
): Effect.fn.Return<RmqService, RmqError, Scope.Scope> {
  yield* Effect.catch(
    wrap("RabbitMQ version preflight", () => checkRabbitMqVersion(opts)),
    (error) =>
      error.cause instanceof UnsupportedRabbitMqVersionError
        ? Effect.fail(error)
        : Effect.logWarning(
          `[rmq] AMQP version preflight failed: ${String(error.cause)}; startup will use the connection retry policy`
        )
  );

  type OnMessage = Parameters<RmqService["consume"]>[1];

  /** Handlers run in client callbacks, off any fiber: capture the context so `warn` reaches the configured logger. */
  const services = yield* Effect.context<never>();
  const forkInContext = Effect.runForkWith(services);

  /** Completed once, when either connection's recovery gives up. */
  const lost = yield* Deferred.make<never, RmqError>();
  const warn = (message: string) => void forkInContext(Effect.logWarning(`[rmq] ${message}`));
  const failLost = (error: RmqError) => void forkInContext(Deferred.fail(lost, error));

  /** Everything this connection was told to create, so it can be created again. */
  type Topology =
    | { readonly kind: "queue"; readonly name: string; readonly durable: boolean; readonly args: QueueArgs; }
    | { readonly kind: "exchange"; readonly name: string; readonly durable: boolean; }
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
  const live = new Map<Consumer, Live>();

  /** Whether each connection is usable: false from a disconnect until `setup` has put everything back. */
  let connected = true;
  /** The same for the publishing connection, which fails and recovers on its own. */
  let publishing = true;

  /** `null`, not `Option`: private mutable interop state that nobody outside this closure branches on. */
  let out: AMQPChannel | null = null;
  let opening: Promise<AMQPChannel> | null = null;
  /** `message_id`s the broker has handed back as unroutable and whose confirm has not yet arrived. */
  const unroutable = new Set<string>();

  /**
   * Settles when the broker next lifts its block on the publishing connection; `null` while there is none. A broker
   * alarm (memory, disk) blocks the connections that publish, and the client fails a publish made meanwhile instead of
   * holding it, so `publish` holds it here: an alarm is backpressure, not a failed send.
   */
  let blocked: Promise<void> | null = null;
  let unblock = (): void => {};
  /** Settles once the publishing connection is not blocked; a block may come and go more than once meanwhile. */
  const unblocked = (): Promise<void> => blocked === null ? Promise.resolve() : blocked.then(unblocked);
  const block = (): void => {
    blocked ??= new Promise<void>((resolve) => {
      unblock = () => {
        blocked = null;
        unblock = () => {};
        resolve();
      };
    });
  };

  /** A confirm channel for publishing; a close fails every publish awaiting a confirm on it, which the client does itself. */
  const openPublishChannel = async (ch: AMQPChannel): Promise<AMQPChannel> => {
    ch.onerror = (reason) => warn(`publish channel error: ${reason}`);
    ch.onReturn = (message) => void unroutable.add(String(message.properties.messageId));
    await ch.confirmSelect();
    return ch;
  };

  /** The delivery callback, shared by the first registration and every rebuild. */
  const deliver = (ch: AMQPChannel, entry: Live) => (message: AMQPMessage): void => handle(ch, entry, message);

  /**
   * A handler that threw or rejected did not finish, so acking would say it had. It is `discard`ed:
   * dead-lettered where the queue has somewhere to put it, never a tight requeue loop. Logged, because a
   * handler that fails without a trace is how a queue quietly empties into a dead-letter queue nobody reads.
   */
  const failed = (ch: AMQPChannel, entry: Live, message: AMQPMessage, error: unknown): Promise<void> => {
    warn(`handler for ${entry.queue} failed, dead-lettering the delivery: ${String(error)}`);
    return settle(ch, message, "discard");
  };

  /** One delivery settled: the last one out wakes whoever is draining this consumer. */
  const released = (entry: Live): void => {
    entry.outstanding -= 1;
    when(entry.outstanding === 0, () => entry.idle.splice(0).forEach((resolve) => resolve()));
  };

  const handle = (ch: AMQPChannel, entry: Live, message: AMQPMessage): void => {
    entry.outstanding += 1;
    const body = message.body ?? new Uint8Array(0);
    // The executor turns a synchronous throw into a rejection, so it never escapes into the client's callback.
    void new Promise<void | Settlement>((resolve) => resolve(entry.onMessage(body, describe(message))))
      .then(
        (outcome) => settle(ch, message, outcome ?? "accept"),
        (error) => failed(ch, entry, message, error)
      )
      .finally(() => released(entry));
  };

  /**
   * Put a consumer back after its channel closed while the connection stayed up: a protocol error, a deleted
   * queue, a settle on an unknown tag. The client does not notice. Unbounded on purpose: a deleted queue makes
   * `attach` reject and stops it, and a budget would never reset on an idle queue. A consumer retired on purpose
   * is no longer live, and one whose connection went is put back by `setup` with everything else.
   */
  const rebuild = (entry: Live, ch: AMQPChannel) =>
    when(
      live.has(entry.handle) && entry.handle.channel === ch && !ch.connection.closed,
      () =>
        void attach(() => consuming.channel(), entry).then(
          () => warn(`consumer channel on ${entry.queue} closed — rebuilt`),
          (error) =>
            // `connected` is false if the connection went meanwhile, and `setup` re-attaches everything on return;
            // reporting here would flag a failure already being handled.
            when(connected, () => warn(`consumer on ${entry.queue} closed and could not be rebuilt: ${String(error)}`))
        )
    );

  /**
   * How a consumer ends. Its channel closed, by us or by the broker: rebuild. The broker cancelled it (its queue was
   * deleted) and the channel stays open and looks healthy: say so, since it receives nothing now. We cancelled it:
   * nothing to do.
   */
  const watchConsumer = (consumer: AMQPConsumer, ch: AMQPChannel, entry: Live): void => {
    const ended = (error?: unknown) =>
      ch.closed
        ? rebuild(entry, ch)
        : when(error !== undefined, () =>
          warn(`broker cancelled the consumer on ${entry.queue} — it receives nothing now`));
    void consumer.wait().then(() => ended(), ended);
  };

  /** Register one consumer on its own channel, and point its handle at it. */
  const attach = async (open: () => Promise<AMQPChannel>, entry: Live) => {
    const ch = await open();
    ch.onerror = (reason) => warn(`consumer channel error on ${entry.queue}: ${reason}`);
    await ch.basicQos(entry.prefetch);
    const consumer = await ch.basicConsume(entry.queue, { noAck: false }, deliver(ch, entry));
    const previous = entry.handle.channel;
    entry.handle.channel = ch;
    entry.handle.consumerTag = consumer.tag;
    watchConsumer(consumer, ch, entry);
    // Normally `rebuild`, where `previous` is already closed and this is a no-op. But `setup` re-attaches every live
    // entry after a reconnect, so close `previous` explicitly in case it is somehow still open: a stale consumer
    // would otherwise outlive its channel and inflate the broker's consumer count.
    when(previous !== ch, () => void quietly(previous.close()));
  };

  /** A channel whose server-side close is expected and not worth reporting: a declare that failed closes the one it ran on. */
  const quiet = (ch: AMQPChannel): AMQPChannel => {
    ch.onerror = () => {};
    return ch;
  };

  /** Replay every declare and binding, in the order they were first made. */
  const replay = async (client: Client) => {
    const ch = quiet(await client.channel());
    try {
      await inSequence(topology.values(), (t) =>
        Match.value(t).pipe(
          Match.discriminatorsExhaustive("kind")({
            queue: (q) => ch.queueDeclare(q.name, { durable: q.durable, exclusive: false, autoDelete: false }, q.args),
            exchange: (x) => ch.exchangeDeclare(x.name, "topic", { durable: x.durable, autoDelete: false }),
            bind: (b) => ch.queueBind(b.destination, b.source, b.routingKey),
            exchangeBind: (b) => ch.exchangeBind(b.destination, b.source, b.routingKey)
          })
        ));
    } finally {
      await quietly(ch.close());
    }
  };

  /**
   * Runs on every connect, before the connection is handed out; uses `client`, since the link would wait for
   * itself. Never publishes: an alarm blocks a publishing connection, and its consumers with it.
   */
  const setup = async (client: Client) => {
    if (topology.size > 0) await replay(client);
    await inSequence(live.values(), (entry) => attach(() => client.channel(), entry));
    connected = true;
  };

  /** The same for the publishing connection, which owns the confirm channel and nothing else. A broker alarm blocks it, so it says so. */
  const publishingSetup = async (client: Client) => {
    client.onblocked = (reason) => {
      warn(`publishing connection blocked by the broker: ${reason}`);
      block();
    };
    client.onunblocked = () => {
      warn("publishing connection unblocked");
      unblock();
    };
    // Opened eagerly, so the first publish does not pay for it.
    out = await openPublishChannel(await client.channel());
    publishing = true;
  };

  const open = (role: string, setup: (client: Client) => Promise<void>, onDown: () => void, restored: () => string) =>
    Effect.acquireRelease(
      wrap("connect", () => connectLink(amqpUrl(opts), { role, setup, onDown, restored, warn, onLost: failLost })),
      // Closing also stops recovery. Forgetting every consumer first stops the resulting channel closes from being
      // read as consumers to repair.
      (link) =>
        Effect.promise(() => {
          live.clear();
          // A publish held for an alarm goes on to fail on the closed connection rather than wait forever.
          unblock();
          return link.close();
        })
    );

  const consuming = yield* open(
    "consuming",
    setup,
    () => void (connected = false),
    () => `${topology.size} topology entries and ${live.size} consumer(s) restored`
  );
  const publisher = yield* open(
    "publishing",
    publishingSetup,
    () => {
      publishing = false;
      // The next connection starts unblocked; the broker blocks it again if the alarm still holds.
      unblock();
    },
    () => "publish channel reopened"
  );

  /** The live publish channel, opening one if the last was closed under us. */
  const publishChannel = (): Promise<AMQPChannel> =>
    out !== null && !out.closed
      ? Promise.resolve(out)
      // One reopen at a time: racing sends must not each open a channel and orphan all but one.
      : (opening ??= publisher.channel().then(openPublishChannel).then(
        (ch) => {
          out = ch;
          opening = null;
          return ch;
        },
        (error) => {
          opening = null;
          throw error;
        }
      ));

  /**
   * Declares run on a throwaway channel each: a redeclare whose arguments disagree with the existing queue
   * closes the channel it arrives on (`PRECONDITION_FAILED`) and must not take the publish path with it.
   */
  const onFreshChannel = <A>(operation: string, use: (ch: AMQPChannel) => Promise<A>) =>
    Effect.acquireUseRelease(
      wrap(operation, () => consuming.channel().then(quiet)),
      (ch) => wrap(operation, () => use(ch)),
      // Closing is best effort by definition: the channel this runs on may be
      // the one the broker just closed under us.
      (ch) => Effect.promise(() => quietly(ch.close()))
    );

  /**
   * Resolves when the broker has confirmed the message, and not before. This is also the backpressure: a
   * caller that awaits its own confirm cannot outrun the broker.
   */
  const publish = async (pub: Publisher, body: Body, properties: AMQPProperties): Promise<void> => {
    await unblocked();
    const ch = await publishChannel();
    const rejected = await ch.basicPublish(pub.exchange, pub.routingKey, body, properties, pub.mandatory).then(
      () => O.none<unknown>(),
      (error: unknown) => O.some(error)
    );
    // A `basic.return` arrives before the confirm for the same message, so by now the id is here if the broker
    // could not route it. Taken out either way, so a nacked or failed publish leaves nothing behind.
    if (unroutable.delete(String(properties.messageId))) {
      throw new Unroutable(`no queue for ${pub.exchange || "(default)"}/${pub.routingKey}`);
    }
    // Blocked between the wait above and the publish: wait again, then send it.
    if (O.exists(rejected, isBlockedRejection)) return publish(pub, body, properties);
    if (O.isSome(rejected)) throw rejected.value;
  };

  /**
   * The properties every publish carries. The `traceparent` goes on as an ordinary header when the caller is inside
   * a span; outside one nothing is added (see Trace.ts). The id ties a broker's `basic.return` to its publish.
   */
  const propertiesFor = (pub: Publisher, tp: O.Option<string>, options: SendOptions): AMQPProperties => ({
    ...(options.format ?? pub.format),
    deliveryMode: 2,
    messageId: options.messageId ?? crypto.randomUUID(),
    // AMQP carries seconds.
    timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
    headers: O.match(tp, {
      onNone: () => options.headers,
      onSome: (value) => ({ ...options.headers, [TRACEPARENT]: value })
    })
  });

  return Rmq.of({
    declareQueue: (name, options = {}) => {
      const durable = options.durable ?? true;
      const args = options.args ?? {};
      record(`q:${name}`, { kind: "queue", name, durable, args });
      return onFreshChannel(
        "declareQueue",
        (ch) => ch.queueDeclare(name, { durable, exclusive: false, autoDelete: false }, args).then(() => name)
      );
    },
    declareTopicExchange: (name, options = {}) => {
      const durable = options.durable ?? false;
      record(`x:${name}`, { kind: "exchange", name, durable });
      return onFreshChannel(
        "declareExchange",
        (ch) => ch.exchangeDeclare(name, "topic", { durable, autoDelete: false }).then(() => name)
      );
    },
    bind: (routingKey, source, destination) => {
      record(`b:${source}:${routingKey}:${destination}`, { kind: "bind", routingKey, source, destination });
      return onFreshChannel("bind", (ch) => ch.queueBind(destination, source, routingKey));
    },
    consume: (queue, onMessage, options = {}) =>
      wrap("consume", async () => {
        // The handle starts on the channel its first consumer will use, so `attach` finds nothing stale to close.
        const first = await consuming.channel();
        const handle: Consumer = { channel: first, consumerTag: "" };
        const entry: Live = {
          handle,
          queue,
          onMessage,
          prefetch: options.prefetch ?? DEFAULT_PREFETCH,
          outstanding: 0,
          idle: []
        };
        // In `live` before `attach`: a channel that dies once `attach` has registered on it is rebuilt only if
        // its entry is live. Roll back on failure so a failed `consume` leaves no dead entry for `setup`.
        live.set(handle, entry);
        try {
          await attach(() => Promise.resolve(first), entry);
        } catch (error) {
          live.delete(handle);
          void quietly(first.close());
          throw error;
        }
        return handle;
      }),
    bindExchange: (routingKey, source, destination) => {
      record(`e:${source}:${routingKey}:${destination}`, { kind: "exchangeBind", routingKey, source, destination });
      return onFreshChannel("bindExchange", (ch) => ch.exchangeBind(destination, source, routingKey));
    },
    get: (queue) =>
      wrap("get", async () => {
        const ch = quiet(await consuming.channel());
        const msg = await ch.basicGet(queue, { noAck: false }).catch(async (error: unknown) => {
          await quietly(ch.close());
          throw error;
        });
        if (msg === null) {
          await quietly(ch.close());
          return O.none();
        }
        // Settled at most once, and tolerant of a channel that closed under the caller: the broker has the
        // delivery back by then.
        let settled = false;
        const settleOnce = (act: () => Promise<void>) =>
          Effect.promise(async () => {
            if (settled) return;
            settled = true;
            await quietly(act());
            await quietly(ch.close());
          });
        return O.some({
          ...describe(msg),
          body: msg.body ?? new Uint8Array(0),
          ack: settleOnce(() => ch.basicAck(msg.deliveryTag)),
          nack: settleOnce(() => ch.basicNack(msg.deliveryTag, true))
        });
      }),
    publisherToExchange: (exchange, routingKey, format = {}) =>
      Effect.succeed({ exchange, routingKey, format, mandatory: false }),
    // The default exchange routes by queue name, which is the same path `deadLetterArgs` uses for dead-lettering.
    publisherToQueue: (queue, format = {}) =>
      Effect.succeed({ exchange: "", routingKey: queue, format, mandatory: true }),
    send: (pub, body, options) =>
      Effect.flatMap(
        traceparent,
        (tp) => wrap("send", () => publish(pub, body, propertiesFor(pub, tp, options ?? {})))
      ),
    // Every `publish` is issued before any is awaited: they reach the channel in order and their confirms pipeline.
    sendBatch: (pub, messages) =>
      Effect.flatMap(
        traceparent,
        (tp) =>
          wrap("sendBatch", () => Promise.all(messages.map((m) => publish(pub, m.body, propertiesFor(pub, tp, m)))))
      ).pipe(Effect.asVoid),
    // Each teardown forgets its consumer first, so a recovery does not bring back one we retired.
    cancelConsumer: (c) =>
      Effect.promise(() => {
        live.delete(c);
        return quietly(c.channel.basicCancel(c.consumerTag));
      }),
    drainConsumer: (c) =>
      Effect.promise(async () => {
        const entry = live.get(c);
        live.delete(c);
        await quietly(c.channel.basicCancel(c.consumerTag));
        // What it still holds is settled on this channel, so the channel stays open until the last of it is —
        // closing first would hand each back unacked, and a call that already succeeded would run again.
        await new Promise<void>((resolve) => (entry && entry.outstanding > 0 ? entry.idle.push(resolve) : resolve()));
        await quietly(c.channel.close());
      }),
    closeConsumer: (c) =>
      Effect.promise(() => {
        live.delete(c);
        return quietly(c.channel.close());
      }),
    lost: Deferred.await(lost),
    isConnected: Effect.sync(() => connected && publishing),
    // `destroy(error)`: the client takes a socket error as a lost connection, rejects every pending confirm on it,
    // and the link reconnects.
    resetConnection: Effect.sync(() =>
      // Both sockets: the publishes buffered on one are what this fences, and a reset process should come back whole.
      [consuming.current(), publisher.current()].forEach((client) =>
        client?.socket?.destroy(new Error("connection reset: fencing a demoted leader's buffered publishes"))
      )
    )
  });
});

/**
 * Build `layer`, then run `program` with its services until `program` ends or fails, or the broker connection is
 * lost: the one place a lost connection becomes a failure that ends the process. `program` runs in the race, not
 * forked into the layer's scope, where a defect could not end it.
 */
export const launchWithRmq = <ROut, E, RIn, A, E2>(
  layer: Layer.Layer<ROut | Rmq, E, RIn>,
  program: Effect.Effect<A, E2, ROut | Rmq>
): Effect.Effect<A, E | E2 | RmqError, RIn> =>
  Effect.scoped(
    Effect.flatMap(Layer.build(layer), (context) =>
      Effect.raceFirst(Context.get(context, Rmq).lost, Effect.provideContext(program, context)))
  );
