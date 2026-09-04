import { Context, Data, Effect, Layer, Scope, Semaphore } from "effect";
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
 * operation that touches it. Note this is not about sharing one connection
 * between separate daemons — each daemon is its own process with its own
 * connection, as it would be in production. It is about a *single* process
 * opening several links on its own connection concurrently, which is
 * ordinary: the aggregator creates a publisher per API on the same tick,
 * and one daemon opens a work-queue consumer, a control-plane consumer, a
 * SAC probe-trigger consumer and a trigger publisher. The cost is nil at
 * this repo's volumes, and correctness here is not the place to trade for
 * throughput.
 *
 * ## Why a connection is not always process-lifetime
 *
 * The second thing this client does silently: closing a consumer while the
 * broker still has deliveries in flight for it strands those deliveries, and
 * enough of them stall the whole *connection* — every link on it, not just
 * the one that was closed. Measured against a real broker: opening a
 * consumer on a 4000-message queue, taking one message and closing (the
 * HALF_OPEN probe, exactly) kills an unrelated long-lived consumer on the
 * same connection after seven cycles, with no error anywhere. The same loop
 * with each probe on its own throwaway connection ran clean.
 *
 * So `makeRmq` is exported alongside `RmqLive`: anything that closes
 * consumers with a backlog behind them gets a connection it can afford to
 * destroy, and the connection carrying the control plane only ever opens
 * links. See docs/rmq-control-plane.md.
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
 * - `requeue` — released, back onto the queue for another consumer or
 *   another attempt. Nothing about it is delayed, so an unbounded requeue on
 *   a failing dependency is a hot loop; bound it.
 * - `discard` — rejected. On a queue declared with `x-dead-letter-exchange`
 *   that routes the message to the dead-letter queue; on one without, it is
 *   simply dropped. This is how a failure becomes visible and drainable
 *   instead of silent.
 */
export type Settlement = "accept" | "requeue" | "discard";

/**
 * What the broker knows about this particular delivery.
 *
 * `deliveryCount` is AMQP 1.0's header field of the same name, as RabbitMQ
 * reports it — 0 on a first delivery. It is the only thing that makes a
 * redelivery budget possible across *different* consumers: an in-process
 * attempt counter is lost the moment the message goes back to the queue and
 * is picked up by another daemon.
 */
export type DeliveryInfo = {
  readonly deliveryCount: number;
  /**
   * Where this message was dead-lettered from, when it was — RabbitMQ 4
   * reports it as AMQP 1.0 message annotations (`x-first-death-queue`,
   * `x-first-death-reason`, and the fuller `x-opt-deaths` array). `null` for
   * a message that arrived normally.
   *
   * This is what makes one canonical dead-letter queue workable rather than
   * a bin of unrelated things: anything consuming it can tell a work message
   * that failed its third-party call from a control message that failed to
   * decode, and treat them differently. Verified against a live broker
   * rather than read off the spec.
   */
  readonly deadLetter: { readonly queue: string; readonly reason: string } | null;
  /**
   * Application properties carried on the message, as strings.
   *
   * The broker's death annotations are lost the moment anything republishes a
   * message, so a consumer that moves messages around inside the dead-letter
   * queue has to carry the provenance itself. This is where it puts it.
   */
  readonly properties: Readonly<Record<string, string>>;
};

export interface RmqService {
  /** durable: false, exclusive: false — every queue this repo declares is a demo fixture, not durable state. */
  readonly declareQueue: (name: string, args?: QueueArgs) => Effect.Effect<RmqQueue, RmqError>;
  readonly declareTopicExchange: (name: string) => Effect.Effect<RmqExchange, RmqError>;
  readonly bind: (
    routingKey: string,
    source: RmqExchange,
    destination: RmqQueue,
  ) => Effect.Effect<void, RmqError>;
  /**
   * The message is settled only once `onMessage` settles. Returning a
   * promise is therefore the flow-control lever this client otherwise
   * doesn't give you: AMQP 1.0 credit is replenished on settlement, so a
   * handler that waits for its own work keeps the broker from pushing more
   * than the consumer can absorb. A synchronous handler settles immediately
   * and gets no backpressure at all.
   *
   * The returned value chooses the outcome, synchronously or from a promise;
   * returning nothing accepts, which is what every handler that cannot fail
   * wants. A synchronous outcome matters for the handlers that decide
   * immediately — a message that fails to decode is rejected on the spot,
   * with no work to await. A handler that *rejects* accepts anyway: the
   * alternative is an unbounded redelivery loop driven by a bug, which is
   * worse than a lost message and much harder to see.
   */
  readonly consume: (
    queue: string,
    onMessage: (
      body: string,
      delivery: DeliveryInfo,
    ) => void | Settlement | Promise<void | Settlement>,
  ) => Effect.Effect<Consumer, RmqError>;
  /** One publisher per fixed (exchange, routingKey) or (queue) target — see Client.ts's module doc for why this is a publisher-per-target library, not per-message addressing. */
  readonly publisherToExchange: (
    exchange: string,
    routingKey: string,
  ) => Effect.Effect<Publisher, RmqError>;
  readonly publisherToQueue: (queue: string) => Effect.Effect<Publisher, RmqError>;
  /** `properties` become AMQP application properties on the message — see `DeliveryInfo.properties` for why anything republishing needs them. */
  readonly send: (
    pub: Publisher,
    body: string,
    properties?: Record<string, string>,
  ) => Effect.Effect<void, RmqError>;
  readonly closeConsumer: (c: Consumer) => Effect.Effect<void>;
}

export class Rmq extends Context.Service<Rmq, RmqService>()("Rmq") {}

const wrap = <A>(operation: string, promise: () => Promise<A>) =>
  Effect.tryPromise({ try: promise, catch: (cause) => new RmqError({ operation, cause }) });

/**
 * Accepting a delivery whose link has since closed throws `Receiver link is
 * closed`, and with a deferred accept that is not an edge case — it is what
 * happens every time a consumer is retired while calls are still in flight,
 * which is exactly what `OPEN` does to @egress/rmq-consumer's daemons. The
 * settlement is genuinely moot at that point (the broker requeues an
 * unsettled delivery when the link goes), so swallowing it is correct rather
 * than merely convenient. Thrown from inside a socket callback, it would
 * otherwise take the process down.
 */
type DeliveryContext = {
  accept: () => void;
  discard: (annotations?: unknown) => void;
  requeue: (annotations?: unknown) => void;
};

const settle = (ctx: DeliveryContext, outcome: Settlement) => {
  try {
    if (outcome === "discard") ctx.discard();
    else if (outcome === "requeue") ctx.requeue();
    else ctx.accept();
  } catch {
    // link already gone; the delivery goes back to the queue
  }
};

export type RmqConnectOptions = {
  readonly host: string;
  readonly port: number;
  readonly username?: string;
  readonly password?: string;
};

/**
 * One real AMQP 1.0 connection, released when the surrounding scope closes.
 *
 * Exposed separately from `RmqLive` because a connection is not always a
 * process-lifetime thing here: `@egress/rmq-consumer`'s daemon deliberately
 * runs its *work* consumers on a second, disposable connection it opens and
 * closes as the circuit moves, precisely so that churn can never damage the
 * control-plane connection. See `closing a consumer with deliveries in
 * flight` in docs/rmq-control-plane.md for why that separation is load
 * bearing rather than tidiness.
 */
export const makeRmq = (
  opts: RmqConnectOptions,
): Effect.Effect<RmqService, RmqError, Scope.Scope> =>
  Effect.gen(function* () {
      const env = createEnvironment({
        host: opts.host,
        port: opts.port,
        username: opts.username ?? "guest",
        password: opts.password ?? "guest",
      });
      const connection: Connection = yield* Effect.acquireRelease(
        wrap("connect", () => env.createConnection()),
        // Swallowed deliberately: this runs on every scope close, and the
        // daemon closes connections constantly (a probe, a redrive pass, the
        // work connection on every transition). A broker that has already
        // gone makes `close` reject, and `Effect.promise` turns a rejection
        // into a defect — so without this, tearing down a connection to a
        // broker that died first would fail the teardown rather than
        // complete it.
        () => Effect.promise(() => env.close().then(() => {}, () => {})),
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
                const annotations =
                  (message as { message_annotations?: Record<string, unknown> })
                    .message_annotations ?? {};
                const deathQueue = annotations["x-first-death-queue"];
                const deathReason = annotations["x-first-death-reason"];
                const delivery: DeliveryInfo = {
                  deliveryCount: Number(
                    (message as { delivery_count?: number }).delivery_count ?? 0,
                  ),
                  deadLetter:
                    typeof deathQueue === "string"
                      ? {
                          queue: deathQueue,
                          reason: typeof deathReason === "string" ? deathReason : "unknown",
                        }
                      : null,
                  properties: Object.fromEntries(
                    Object.entries(
                      (message as { application_properties?: Record<string, unknown> })
                        .application_properties ?? {},
                    ).map(([k, v]) => [k, String(v)]),
                  ),
                };
                // A handler that throws *synchronously* would escape into
                // rhea's socket callback, where nothing can catch it — the
                // client keeps its container private, so there is no error
                // listener to attach and the process dies. Every handler in
                // this repo is careful, which is exactly the kind of thing
                // that stops being true later.
                let done: void | Settlement | Promise<void | Settlement>;
                try {
                  done = onMessage(String(message.body), delivery);
                } catch {
                  return settle(ctx as DeliveryContext, "accept");
                }
                if (done === undefined) return settle(ctx as DeliveryContext, "accept");
                // A synchronous outcome is a string, not a thenable — calling
                // .then() on it would throw from the same unreachable place.
                if (typeof done === "string") return settle(ctx as DeliveryContext, done);
                void done.then(
                  (outcome) => settle(ctx as DeliveryContext, outcome ?? "accept"),
                  () => settle(ctx as DeliveryContext, "accept"),
                );
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
        send: (pub, body, properties) =>
          guarded("send", () =>
            pub.publish(
              (properties === undefined
                ? { body }
                : { body, application_properties: properties }) as never,
            ),
          ).pipe(Effect.asVoid),
        closeConsumer: (c) => Effect.sync(() => c.close()),
      };
    });

/** The process-lifetime connection: one per layer instance, closed with the layer's scope. */
export const RmqLive = (opts: RmqConnectOptions) => Layer.effect(Rmq, makeRmq(opts));
