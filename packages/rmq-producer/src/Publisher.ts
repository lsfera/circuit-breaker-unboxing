import { isUnroutable, Rmq } from "@egress/rmq/Client.ts";
import type { RmqError } from "@egress/rmq/Client.ts";
import * as Contract from "@egress/rmq/Contract.ts";
import { Data, Effect, Metric, Option as O } from "effect";
import type { Schema } from "effect";
import * as Telemetry from "./Telemetry.ts";

/**
 * A contract's messages onto the contract's exchange. A publisher knows the exchange and nothing behind it: which
 * queues receive its messages is each consumer's to decide, by binding its own queue. Nor does it read breaker state:
 * arrivals do not stop because a consumer's dependency is down, and nothing here could tell it was.
 */
export interface Publisher<A> {
  /** The exchange it publishes to, as the contract declares it. */
  readonly exchange: Contract.Exchange;
  /**
   * Publishes a `Publication`, `one` message or a `batch`, and succeeds once the broker has routed every message to a
   * queue and confirmed it. Each is written in the publisher's format and stamped with a `message_id` it keeps for
   * life, the idempotency key a consumer acts on: `one` returns its id, a `batch` theirs in order. A batch goes back to
   * back on the client's confirm channel, one round trip for all of it rather than one per message.
   *
   * A message the contract refuses fails the publication before anything is sent (`ContractRefused`). Otherwise the
   * failure carries the ids the messages were sent with (`Unroutable`, `BrokerFailed`): some may be held by the
   * broker, so publish them again with those ids (`one(m, { id })`, `batch(ms, { ids })`), which a consumer's
   * idempotency on the key makes harmless.
   *
   * A broker alarm (memory, disk) blocks the publishing connection, and a publication made meanwhile waits until the
   * broker unblocks it, however long that takes: backpressure, not a failure. A caller with a deadline bounds it
   * itself (`Effect.timeout`), and on timing out publishes again with the same ids.
   */
  readonly publish: {
    (publication: One<A>): Effect.Effect<string, PublishError>;
    (publication: Batch<A>): Effect.Effect<ReadonlyArray<string>, PublishError>;
  };
}

/**
 * Where on the exchange a message goes: its `routingKey`, which a direct or topic exchange routes on, and its
 * `headers`, which a headers exchange matches the binding's arguments against (and which every message carries).
 * A contract with a `route` owns where its messages go: a `routingKey` beside it is a defect, and its route's
 * headers win over any of the same name.
 */
export type Routing = {
  readonly routingKey?: string;
  readonly headers?: Record<string, string>;
};

/**
 * What `publish` takes: tagged rather than told apart by shape, so a contract whose message is itself an array is
 * still one message or a batch of them, never a guess. `id`/`ids`, when given, are the `message_id`s to publish with,
 * to repeat a publication that failed; otherwise the publisher stamps new ones.
 */
export type Publication<A> = Data.TaggedEnum<{
  One: { readonly message: A; readonly routing: Routing; readonly id: O.Option<string>; };
  Batch: {
    readonly messages: ReadonlyArray<A>;
    readonly routing: Routing;
    readonly ids: O.Option<ReadonlyArray<string>>;
  };
}>;

interface PublicationDefinition extends Data.TaggedEnum.WithGenerics<1> {
  readonly taggedEnum: Publication<this["A"]>;
}

const Publication = Data.taggedEnum<PublicationDefinition>();

/** One message, and where on the exchange it goes. */
export type One<A> = Extract<Publication<A>, { readonly _tag: "One"; }>;

/** Messages published together, in order, all to the same place on the exchange. */
export type Batch<A> = Extract<Publication<A>, { readonly _tag: "Batch"; }>;

/** A single message, routed by `routing` beside the publisher's own; `id` repeats one already sent. */
export const one = <A>(message: A, options: Routing & { readonly id?: string; } = {}): One<A> => {
  const { id, ...routing } = options;
  return Publication.One({ message, routing, id: O.fromUndefinedOr(id) }) as One<A>;
};

/**
 * Messages published together, in order, routed by `routing` beside the publisher's own; `ids` repeat a batch
 * already sent, one per message, in the same order.
 */
export const batch = <A>(
  messages: ReadonlyArray<A>,
  options: Routing & { readonly ids?: ReadonlyArray<string>; } = {}
): Batch<A> => {
  const { ids, ...routing } = options;
  if (ids !== undefined && ids.length !== messages.length) {
    throw new Error(`a batch of ${messages.length} messages given ${ids.length} ids`);
  }
  return Publication.Batch({ messages, routing, ids: O.fromUndefinedOr(ids) }) as Batch<A>;
};

/** The contract refused a message: nothing in the publication was sent. */
export class ContractRefused extends Data.TaggedError("ContractRefused")<{ readonly cause: Schema.SchemaError; }> {}

/**
 * A message no queue is bound to receive. The rest may have been routed and held, so `ids` are what to publish the
 * publication again with.
 */
export class Unroutable extends Data.TaggedError("Unroutable")<{ readonly ids: ReadonlyArray<string>; }> {}

/**
 * The broker nacked, or the channel was lost with the publication unconfirmed: what it holds is unknown, so `ids` are
 * what to publish the publication again with.
 */
export class BrokerFailed extends Data.TaggedError("BrokerFailed")<{
  readonly ids: ReadonlyArray<string>;
  readonly cause: RmqError;
}> {}

/**
 * Why a publication failed, by `reason`: `ContractRefused` before anything was sent, `Unroutable` or `BrokerFailed`
 * after, carrying the ids to repeat it with. `Effect.catchReason("PublishError", "Unroutable", …)` handles one.
 */
export class PublishError extends Data.TaggedError("PublishError")<{
  readonly exchange: string;
  readonly reason: ContractRefused | Unroutable | BrokerFailed;
}> {
  override get message(): string {
    const { reason } = this;
    const why = reason._tag === "ContractRefused"
      ? `the contract refused a message — ${reason.cause.message}`
      : reason._tag === "Unroutable"
      ? "no queue is bound to receive a message"
      : `the broker — ${reason.cause.message}`;
    return `publishing to ${this.exchange}: ${why}`;
  }
}

export type PublisherOptions = Routing & {
  /**
   * The media type bodies are written as: one of the contract's formats that can write. Default: the first that can.
   * One the contract lacks, or only reads, is a defect when the publisher is made.
   */
  readonly format?: string;
  /**
   * Whether a message no queue is bound to receive fails the publish (on unless given) or is dropped by the broker,
   * which suits an exchange whose consumers may legitimately all be gone, such as a notification fan-out.
   */
  readonly mandatory?: boolean;
};

/**
 * A publisher of `contract` onto its exchange. Declares the exchange as every consumer of the contract declares it,
 * so whichever starts first creates it; messages are published `mandatory` unless asked otherwise, so one sent
 * before any consumer has bound a queue fails instead of vanishing.
 *
 * `message_id`s are `<run>:<n>`: a run id unique to this publisher, then its sequence. `n` alone restarts at zero
 * with the process, so a restarted producer would reuse the key of different work and a third party would drop it
 * as a duplicate. The last `:` splits run from sequence.
 */
export const make = Effect.fnUntraced(function*<A>(contract: Contract.Contract<A>, options: PublisherOptions = {}) {
  const rmq = yield* Rmq;
  const { exchange } = contract;
  const format = options.format ?? Contract.writable(contract)[0];
  if (format === undefined) {
    return yield* Effect.die(new Error(`${exchange.name}: the contract has no format a publisher can write`));
  }
  const encode = Contract.encoder(contract, format);
  // Another key would send the contract's messages to whichever contract's consumers bound that one.
  const routed = (where: string) =>
    Effect.die(new Error(`${exchange.name}: the contract routes its messages, so a ${where} gives no routing key`));
  if (O.isSome(contract.route) && options.routingKey !== undefined) return yield* routed("publisher");
  const route = O.match(contract.route, {
    onNone: () => ({ routingKey: "", headers: {} }),
    onSome: Contract.Route.$match({
      RoutingKey: ({ routingKey }) => ({ routingKey, headers: {} }),
      Headers: ({ headers }) => ({ routingKey: "", headers })
    })
  });
  yield* rmq.declareExchange(exchange.name, exchange);
  const destination = yield* rmq.publisherToExchange(exchange.name, options.routingKey ?? route.routingKey, {
    contentType: format,
    ...O.match(contract.type, { onNone: () => ({}), onSome: (type) => ({ type }) })
  }, { mandatory: options.mandatory ?? true });

  const run = crypto.randomUUID().slice(0, 8);
  let sent = 0;
  const failed = (reason: "contract_refused" | "broker_failed", n: number) =>
    Metric.update(Metric.withAttributes(Telemetry.failed, { exchange: exchange.name, reason }), n);
  const routingKeyOf = (routing: Routing) => routing.routingKey ?? destination.routingKey;
  const fail = (reason: PublishError["reason"]) => new PublishError({ exchange: exchange.name, reason });

  /** Every publication, one message or many: encoded first, so a refusal sends nothing; then sent as one batch. */
  const send = Effect.fnUntraced(
    function*(messages: ReadonlyArray<A>, routing: Routing, given: O.Option<ReadonlyArray<string>>) {
      if (O.isSome(contract.route) && routing.routingKey !== undefined) return yield* routed("publication");
      const bodies = yield* Effect.forEach(messages, encode).pipe(
        Effect.mapError((cause) => fail(new ContractRefused({ cause })))
      );
      const ids = O.getOrElse(given, () => bodies.map(() => `${run}:${sent++}`));
      const headers = { ...options.headers, ...routing.headers, ...route.headers };
      yield* rmq.sendBatch(
        { ...destination, routingKey: routingKeyOf(routing) },
        bodies.map((body, i) => ({ body, messageId: ids[i]!, headers }))
      ).pipe(
        Effect.mapError((cause) =>
          fail(isUnroutable(cause) ? new Unroutable({ ids }) : new BrokerFailed({ ids, cause }))
        )
      );
      return ids;
    },
    // Counted only where the broker cannot: what it confirmed or returned unroutable, it reports itself.
    (effect, messages) =>
      Effect.tapError(effect, ({ reason }) =>
        reason._tag === "ContractRefused"
          ? failed("contract_refused", messages.length)
          : reason._tag === "BrokerFailed"
          ? failed("broker_failed", messages.length)
          : Effect.void),
    // One publication is one trace root, a batch or a single message: `sendBatch` stamps the same traceparent on each
    // message, and each consumer's processing span inherits it from its delivery.
    (effect, messages, routing) =>
      Effect.withSpan(effect, "work.publish", {
        attributes: {
          "messaging.system": "rabbitmq",
          "messaging.operation.name": "publish",
          "messaging.destination.name": exchange.name,
          "messaging.rabbitmq.destination.routing_key": routingKeyOf(routing),
          "messaging.batch.message_count": messages.length
        }
      })
  );

  // A single message is a batch of one on the wire: the same encoding, ids, metric and span.
  const publish = ((publication: Publication<A>) =>
    Publication.$match(publication, {
      One: ({ message, routing, id }) => Effect.map(send([message], routing, O.map(id, (i) => [i])), (ids) => ids[0]!),
      Batch: ({ messages, routing, ids }) => send(messages, routing, ids)
    })) as Publisher<A>["publish"];

  return { exchange, publish } satisfies Publisher<A>;
});
