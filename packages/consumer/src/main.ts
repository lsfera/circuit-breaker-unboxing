import { PgClient } from "@effect/sql-pg";
import * as Consumer from "@egress/rmq-consumer";
import { traceparent } from "@egress/rmq/Trace.ts";
import { Config, Context, Effect, Layer, Match, Option as O, Redacted, Result, Schema } from "effect";
import { Flag } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";
import { SqlClient, SqlError } from "effect/sql";
import protobuf from "protobufjs";

/**
 * The consumer application (`node src/main.ts`). Payments are charged at the third party, then recorded in the
 * ledger; refunds are only recorded. A third-party outage stops payments only; a ledger outage stops both.
 */

/** The two contracts this application agrees with its producers. The same shape today; each is its own contract. */
const Payment = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
const Refund = Schema.Struct({ apiId: Schema.String, n: Schema.Int });

/** The third party, reached at one address. The idempotency key rides as its own header, so a redelivery repeats the same request. */
const withTraceParent = (request: HttpClientRequest.HttpClientRequest): Effect.Effect<HttpClientRequest.HttpClientRequest> =>
  Effect.flatMap(traceparent, (header) =>
    O.match(header, {
      onNone: () => Effect.succeed(request),
      onSome: (value) => Effect.succeed(request.pipe(HttpClientRequest.setHeader("traceparent", value)))
    })
  );

class PaymentsApi extends Context.Service<
  PaymentsApi,
  { readonly charge: (key: string) => Effect.Effect<number, HttpClientError.HttpClientError>; }
>()("@egress/consumer/main/PaymentsApi") {
  static readonly layer = (url: string) =>
    Layer.effect(
      PaymentsApi,
      Effect.map(HttpClient.HttpClient, (client) =>
        PaymentsApi.of({
          charge: (key) =>
            Effect.flatMap(
              withTraceParent(HttpClientRequest.get(url).pipe(HttpClientRequest.setHeader("x-idempotency-key", key))),
              (request) =>
                Effect.withSpan(
                  client.execute(request),
                  "payments-api.charge",
                  {
                    attributes: {
                      "http.request.method": "GET",
                      "server.address": url,
                      "url.full": url
                    }
                  }
                )
            ).pipe(
              // Drain the body even though nothing wants it: an unconsumed response holds its connection out of the pool.
              Effect.tap((response) => Effect.ignore(response.text)),
              Effect.map((response) => response.status)
            ) as Effect.Effect<number, HttpClientError.HttpClientError>
        }))
    ).pipe(Layer.provide(FetchHttpClient.layer));
}

/** `payments` and `refunds` are assumed to exist. Idempotent on `message_id`: a redelivery writes nothing new. */
class Ledger extends Context.Service<
  Ledger,
  {
    readonly record: (key: string, payment: typeof Payment.Type) => Effect.Effect<void, SqlError.SqlError>;
    readonly refund: (key: string, refund: typeof Refund.Type) => Effect.Effect<void, SqlError.SqlError>;
  }
>()("@egress/consumer/main/Ledger") {
  static readonly layer = (url: Redacted.Redacted) =>
    Layer.effect(
      Ledger,
      Effect.map(SqlClient.SqlClient, (sql) =>
        Ledger.of({
          record: (key, p) =>
            Effect.asVoid(
              sql`INSERT INTO payments ${
                sql.insert({ message_id: key, api_id: p.apiId, n: p.n })
              } ON CONFLICT (message_id) DO NOTHING`
            ),
          refund: (key, r) =>
            Effect.asVoid(
              sql`INSERT INTO refunds ${
                sql.insert({ message_id: key, api_id: r.apiId, n: r.n })
              } ON CONFLICT (message_id) DO NOTHING`
            )
        }))
    ).pipe(Layer.provide(PgClient.layer({ url })));
}

/** 2xx ok; 429 full, not broken; any other 4xx but 408 refused this request; the rest, and a request that got no response, failing. */
const byHttpStatus = (result: Result.Result<number, HttpClientError.HttpClientError>): Consumer.Verdict =>
  Result.match(result, {
    onSuccess: (status) => ({
      reason: String(status),
      outcome: Match.value(status).pipe(
        Match.when((s) => s >= 200 && s < 300, () => "ok" as const),
        Match.when(429, () => "throttled" as const),
        Match.when((s) => s >= 400 && s < 500 && s !== 408, () => "client_error" as const),
        Match.orElse(() => "failed" as const)
      )
    }),
    // A transport failure, or one of ours such as an invalid URL: the tag says which.
    onFailure: ({ reason }) => ({ outcome: "failed", reason: reason._tag })
  });

/** Contention means "fewer at once"; a row the schema refuses is this message's fault; anything else is the database failing. */
const bySqlError = (result: Result.Result<void, SqlError.SqlError>): Consumer.Verdict =>
  Result.match(result, {
    onSuccess: () => ({ outcome: "ok", reason: "ok" }),
    onFailure: ({ reason }) => ({
      reason: reason._tag,
      outcome: Match.value(reason._tag).pipe(
        Match.when(
          Match.is("DeadlockError", "SerializationError", "LockTimeoutError", "StatementTimeoutError"),
          () => "throttled" as const
        ),
        Match.when("ConstraintError", () => "client_error" as const),
        // A connection or authentication failure, a missing table (SqlSyntaxError), anything unknown.
        Match.orElse(() => "failed" as const)
      )
    })
  });

const ThirdParty = Consumer.Dependency("payments-api", { classify: byHttpStatus });
// Our own database comes back in seconds to minutes (a restart, a failover), and while it is down every consumer
// waits, so a day-long hold would leave the fleet dark long after it is back: five minutes at most.
const Database = Consumer.Dependency("ledger", { classify: bySqlError, breaker: { maxDelaySeconds: 300 } });

/**
 * `message Work { string api_id = 1; int64 n = 2; }`, defined at runtime. `defaults`: proto3 leaves a zero off the
 * wire, and `n` starts at 0. `longs: Number`: the contract's `n` is a number, not a `Long`.
 */
const WorkProto = protobuf.Type.fromJSON("Work", {
  fields: { apiId: { type: "string", id: 1 }, n: { type: "int64", id: 2 } }
});

/** Each message is read by the format it declares; either way, the contract decides whether it is a payment. */
const formats = Consumer.accept(
  {
    "application/json": Consumer.text(Schema.fromJsonString(Schema.Unknown)),
    "application/x-protobuf": Consumer.bytes((body) =>
      WorkProto.toObject(WorkProto.decode(body), { longs: Number, defaults: true })
    )
  },
  { undeclared: "application/json", type: "egress.work" }
);

/** The third party dedupes on it: no `message_id`, no safe retry. */
const keyOf = (metadata: Consumer.Metadata) =>
  O.match(metadata.messageId, {
    onNone: () => Effect.fail(new Consumer.Rejected({ reason: "keyless" })),
    onSome: Effect.succeed
  });

const payments = Consumer.For(Payment, formats).bind(
  Effect.fnUntraced(function*(payment, metadata) {
    const key = yield* keyOf(metadata);
    // Halts here unless the charge was ok, so only an accepted charge is recorded.
    yield* ThirdParty((yield* PaymentsApi).charge(key));
    yield* Database((yield* Ledger).record(key, payment));
  }),
  [ThirdParty, Database]
);

const refunds = Consumer.For(Refund, formats).bind(
  Effect.fnUntraced(function*(refund, metadata) {
    const key = yield* keyOf(metadata);
    yield* Database((yield* Ledger).refund(key, refund));
  }),
  [Database]
);

Consumer.run({
  consumers: { "payments-provider": payments, "refunds-provider": refunds },
  flags: {
    egressAddr: Flag.String("egress-addr").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("EGRESS_ADDR")),
      Flag.withDescription("The one address the third party is reached at")
    ),
    apiPath: Flag.String("api-path").pipe(
      Flag.withFallbackConfig(Config.NonEmptyString("API_PATH")),
      Flag.withDefault("/payments"),
      Flag.withDescription("The route on egress-addr that charges a payment")
    ),
    databaseUrl: Flag.Redacted("database-url").pipe(
      Flag.withFallbackConfig(Config.Redacted("DATABASE_URL")),
      Flag.withDescription("The ledger: postgres://user:password@host:port/database")
    )
  },
  layer: ({ egressAddr, apiPath, databaseUrl }) =>
    Layer.mergeAll(PaymentsApi.layer(`${egressAddr}${apiPath}`), Ledger.layer(databaseUrl))
});
