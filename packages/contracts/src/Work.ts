import * as Contract from "@egress/rmq/Contract.ts";
import { bytes, text } from "@egress/rmq/Negotiation.ts";
import { Schema } from "effect";
import protobuf from "protobufjs";

/**
 * The work this scenario's producers publish and its consumer reads, declared once for both. The producer writes in
 * one format per process (`--format`); the consumer reads any of them, and the contract decides whether what it read
 * is a message.
 */

/**
 * `message Work { string api_id = 1; int64 n = 2; }`, defined at runtime. Read with `defaults`: proto3 leaves a zero
 * off the wire, and `n` starts at 0. `longs: Number`: the contract's `n` is a number, not a `Long`.
 */
const WorkProto = protobuf.Type.fromJSON("Work", {
  fields: { apiId: { type: "string", id: 1 }, n: { type: "int64", id: 2 } }
});

/** The media types work is written in, and how each reads and writes a body. */
export const formats = {
  "application/json": text(Schema.fromJsonString(Schema.Unknown)),
  "application/x-protobuf": bytes({
    decode: (body) => WorkProto.toObject(WorkProto.decode(body), { longs: Number, defaults: true }),
    encode: (value) => WorkProto.encode(value as { apiId: string; n: number; }).finish()
  })
};

/** `json` and `protobuf`, as a flag names them, to the media type each writes. */
export const mediaTypes = { json: "application/json", protobuf: "application/x-protobuf" } as const;

/**
 * `n` is an integer: it keeps the idempotency key stable across a redelivery. A message that declares no format is
 * read as JSON, the format this scenario's first producers wrote without saying so.
 *
 * `exchange` is the whole exchange the publisher publishes to, not just its name: its type (`direct`, `fanout`,
 * `topic`, `headers`), durability and arguments. A name alone is a durable topic exchange. Being in the contract,
 * the consumer declares it identically, so either may start first; a declare that disagrees is refused (406).
 */
const work = <A>(exchange: Contract.ExchangeInput, schema: Schema.Codec<A, any>) =>
  Contract.make(schema, { exchange, type: "egress.work", formats, undeclared: "application/json" });

/** A payment to charge at the third party and record in the ledger. */
export const Payment = work(
  { name: "egress.payments", type: "topic" },
  Schema.Struct({ apiId: Schema.String, n: Schema.Int })
);

/** A refund to record in the ledger. The same shape as a payment today; each is its own contract. */
export const Refund = work(
  { name: "egress.refunds", type: "topic" },
  Schema.Struct({ apiId: Schema.String, n: Schema.Int })
);
