import * as Contract from "@egress/rmq/Contract.ts";
import { read } from "@egress/rmq/Negotiation.ts";
import { Effect, Option as O, Result, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "vitest";
import { mediaTypes, Payment, Refund } from "../src/Work.ts";

const declared = (contentType: string) => ({
  contentType: O.some(contentType),
  contentEncoding: O.none(),
  type: O.some("egress.work")
});

test("what the producer writes, in either format, the consumer reads as the same payment or refund", async () => {
  for (const contract of [Payment, Refund]) {
    const readBack = read(Contract.negotiate(contract), Schema.decodeUnknownOption(contract.schema));
    for (const mediaType of Object.values(mediaTypes)) {
      // n = 0 too: proto3 leaves a zero off the wire, and the reader must put it back.
      for (const message of [{ apiId: "payments-provider", n: 0 }, { apiId: "payments-provider", n: 41_000 }]) {
        const body = await Effect.runPromise(Contract.encoder(contract, mediaType)(message));
        assert.deepEqual(readBack(body, declared(mediaType)), Result.succeed(message), `${mediaType} ${message.n}`);
      }
    }
  }
});

test("a body written as protobuf is the wire format of `message Work { string api_id = 1; int64 n = 2; }`", async () => {
  const body = await Effect.runPromise(Contract.encoder(Payment, mediaTypes.protobuf)({ apiId: "a", n: 1 }));
  // field 1 (string "a"), field 2 (varint 1)
  assert.deepEqual([...body], [0x0a, 0x01, 0x61, 0x10, 0x01]);
});

test("each contract names the exchange it is published to, type and all, for both sides to declare alike", () => {
  for (const [contract, name] of [[Payment, "egress.payments"], [Refund, "egress.refunds"]] as const) {
    assert.deepEqual(contract.exchange, {
      name,
      type: "topic",
      durable: true,
      autoDelete: false,
      internal: false,
      args: {}
    });
  }
});
