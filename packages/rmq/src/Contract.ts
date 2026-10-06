import { Data, Effect, Option as O, Predicate, Record as Rec, Schema } from "effect";
import type { BindingArgs, ExchangeOptions } from "./Client.ts";
import { accept, canWrite } from "./Negotiation.ts";
import type { Negotiate, Parser } from "./Negotiation.ts";

/**
 * A message contract, declared once and shared by the side that publishes it (`@egress/rmq-producer`) and the side
 * that consumes it (`@egress/rmq-consumer`): what a message is, the exchange it is published to, the AMQP `type` it
 * carries, and the formats its body is written in, by media type. The exchange is the meeting point: a publisher sends
 * to it and knows no queue; each consumer binds its own queue to it. Declaring it twice, once per side and per format, let the two drift apart
 * with nothing to notice but a consumer parking what a producer sent.
 *
 *   const Payment = Contract.make(Schema.Struct({ … }), {
 *     exchange: "egress.payments",                     // or { name, type: "headers", args: { … }, … }
 *     type: "egress.work",
 *     formats: { "application/json": text(Schema.fromJsonString(Schema.Unknown)), … },
 *     undeclared: "application/json"
 *   });
 *
 * A contract has an exchange to itself unless it opts in to sharing one, by a `route`: then several contracts can
 * name the same exchange, and each consumer's queue receives only its own contract's messages.
 */
export const TypeId = "~@egress/rmq/Contract" as const;

export interface Contract<A> {
  readonly [TypeId]: typeof TypeId;
  /** What a message is. A consumer decodes with it; a publisher encodes with it, so a value it refuses is never sent. */
  readonly schema: Schema.Codec<A, any>;
  /** The exchange its messages are published to, declared identically by both sides. */
  readonly exchange: Exchange;
  /** The AMQP `type` a publisher stamps and a consumer, when a message declares one, requires. */
  readonly type: O.Option<string>;
  /** Each format the contract is written in, by media type. A publisher can write only those whose parser encodes. */
  readonly formats: Readonly<Record<string, Parser>>;
  /** The format a consumer reads a message as when its publisher declared none; `None` declines such a message. */
  readonly undeclared: O.Option<string>;
  /**
   * Where on the exchange its messages go, when it shares the exchange with other contracts: a publisher publishes
   * on it and a consumer binds to it. `None`, the default, has the exchange to itself: a consumer binds to all of it.
   */
  readonly route: O.Option<Route>;
}

/**
 * A contract's place on a shared exchange: a `RoutingKey` on a direct or topic exchange, published with and bound to
 * as it is (no wildcards: it is both); or `Headers` on a headers exchange, which every message carries and a
 * consumer's binding requires all of.
 */
export type Route = Data.TaggedEnum<{
  RoutingKey: { readonly routingKey: string; };
  Headers: { readonly headers: Readonly<Record<string, string>>; };
}>;

export const Route = Data.taggedEnum<Route>();

/** How a contract gives its route: a routing key, or the headers to route on. */
export type RouteInput = string | { readonly headers: Record<string, string>; };

/** A route that cannot work on `exchange` is refused when the contract is made, not when nothing arrives. */
const routeOf = (input: RouteInput, exchange: Exchange): Route => {
  const route = Predicate.isString(input) ? Route.RoutingKey({ routingKey: input }) : Route.Headers(input);
  const refuse = (why: string) => {
    throw new Error(`${exchange.name}: ${why}`);
  };
  if (exchange.type === "fanout") refuse("a fanout exchange delivers everything to every queue: it cannot route");
  return Route.$match(route, {
    RoutingKey: ({ routingKey }) => {
      if (exchange.type === "headers") refuse("a headers exchange routes on headers, not a routing key");
      if (routingKey === "") refuse("an empty routing key is no route");
      if (exchange.type === "topic" && routingKey.split(".").some((word) => word === "*" || word === "#")) {
        refuse(`route ${routingKey} has a wildcard: it is published with, as well as bound to`);
      }
      return route;
    },
    Headers: ({ headers }) => {
      if (exchange.type === "direct" || exchange.type === "topic") {
        refuse(`a ${exchange.type} exchange routes on the routing key, not headers`);
      }
      if (Rec.isEmptyRecord(headers)) refuse("a route needs at least one header");
      const reserved = Object.keys(headers).find((name) => name.startsWith("x-"));
      if (reserved !== undefined) refuse(`${reserved}: a headers exchange does not match on x- headers`);
      return route;
    }
  });
};

/**
 * An exchange as both sides declare it: its name and every setting, defaults filled in, so a publisher and a
 * consumer of one contract cannot declare it two ways (RabbitMQ refuses the second).
 */
export type Exchange = { readonly name: string; } & Required<Omit<ExchangeOptions, "type">> & {
  readonly type: NonNullable<ExchangeOptions["type"]>;
};

/** How a contract names its exchange: a name alone, a durable `topic` exchange, or a name and its settings. */
export type ExchangeInput = string | ({ readonly name: string; } & ExchangeOptions);

/** Each setting not given takes the client's default. */
const exchangeOf = (exchange: ExchangeInput): Exchange => {
  const { name, ...options } = Predicate.isString(exchange) ? { name: exchange } : exchange;
  return {
    name,
    type: options.type ?? "topic",
    durable: options.durable ?? true,
    autoDelete: options.autoDelete ?? false,
    internal: options.internal ?? false,
    args: options.args ?? {}
  };
};

/** Whether `u` is a `Contract` made by `make`, for an API that takes either one or a schema. */
export const isContract = (u: unknown): u is Contract<unknown> => Predicate.hasProperty(u, TypeId);

/** A contract. `undeclared`, when given, must name one of `formats`: a message nobody can read is a mistake here, not at runtime. */
export const make = <A>(
  schema: Schema.Codec<A, any>,
  options: {
    readonly exchange: ExchangeInput;
    readonly formats: Record<string, Parser>;
    readonly type?: string;
    readonly undeclared?: string;
    /** Opts in to sharing the exchange: see `Route`. */
    readonly route?: RouteInput;
  }
): Contract<A> => {
  const exchange = exchangeOf(options.exchange);
  if (exchange.internal) throw new Error(`${exchange.name} is internal: a publisher could never send to it`);
  if (Rec.isEmptyRecord(options.formats)) throw new Error("a contract needs at least one format");
  if (options.undeclared !== undefined && !(options.undeclared in options.formats)) {
    throw new Error(`undeclared names ${options.undeclared}, which is not one of the contract's formats`);
  }
  return {
    [TypeId]: TypeId,
    schema,
    exchange,
    type: O.fromUndefinedOr(options.type),
    formats: options.formats,
    undeclared: O.fromUndefinedOr(options.undeclared),
    route: O.map(O.fromUndefinedOr(options.route), (route) => routeOf(route, exchange))
  };
};

/**
 * How a consumer of the contract binds its queue unless told otherwise: to its route when it has one, so it receives
 * only its own contract's messages; to the whole exchange when it has none (`#` on a topic exchange, `""` on others).
 */
export const binding = (contract: Contract<any>): { readonly routingKey: string; readonly args: BindingArgs; } =>
  O.match(contract.route, {
    onNone: () => ({ routingKey: contract.exchange.type === "topic" ? "#" : "", args: {} }),
    onSome: Route.$match({
      RoutingKey: ({ routingKey }) => ({ routingKey, args: {} }),
      Headers: ({ headers }) => ({ routingKey: "", args: { "x-match": "all", ...headers } })
    })
  });

/** How a consumer of the contract negotiates: its formats, unencoded, and its `type` when a message declares one. */
export const negotiate = (contract: Contract<any>): Negotiate =>
  accept(contract.formats, {
    ...O.match(contract.undeclared, { onNone: () => ({}), onSome: (undeclared) => ({ undeclared }) }),
    ...O.match(contract.type, { onNone: () => ({}), onSome: (type) => ({ type }) })
  });

/** The media types a publisher can write the contract in, in the order the contract lists them. */
export const writable = (contract: Contract<any>): ReadonlyArray<string> =>
  Object.keys(contract.formats).filter((mediaType) => canWrite(contract.formats[mediaType]!));

/**
 * The contract's messages written in the format named by `mediaType`: the schema encodes the message, then the
 * format writes it as bytes. Fails with a `SchemaError` for a message either refuses. A media type the contract does
 * not list, or lists only to read, throws here, when the encoder is made, rather than on every message.
 */
export const encoder = <A>(
  contract: Contract<A>,
  mediaType: string
): (message: A) => Effect.Effect<Uint8Array, Schema.SchemaError> => {
  const parser = contract.formats[mediaType];
  if (parser === undefined) throw new Error(`${mediaType} is not one of the contract's formats`);
  if (!canWrite(parser)) throw new Error(`${mediaType} is a format the contract only reads`);
  const toEncoded = Schema.encodeEffect(contract.schema);
  const toBytes = Schema.encodeEffect(parser);
  return (message) => Effect.flatMap(toEncoded(message), toBytes);
};
