import { Duration, Effect } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

/**
 * The one third-party call this daemon makes, reduced to whether it worked: a timeout, a refused connection
 * and a 5xx are not told apart. The idempotency key rides along as the third party's own HTTP header, so a
 * redelivered message repeats the same request rather than a new one.
 */

/**
 * The payments idempotency key as the third party receives it: an HTTP header. A retry that reuses the key
 * keeps the third party from charging twice. On the broker it is the AMQP `message_id`: assigned once
 * (the producer's `workMessageId`), and a republish must carry it explicitly or the replay is a new message with a new key.
 */
export const IDEMPOTENCY_KEY_HTTP_HEADER = "x-idempotency-key";

export type CallOutcome = "ok" | "failed";

const TIMEOUT = Duration.seconds(2);

export const call = (url: string, idempotencyKey: string): Effect.Effect<CallOutcome, never, HttpClient.HttpClient> =>
  HttpClient.HttpClient.pipe(
    Effect.flatMap((client) =>
      client.execute(HttpClientRequest.get(url).pipe(HttpClientRequest.setHeader(IDEMPOTENCY_KEY_HTTP_HEADER, idempotencyKey))),
    ),
    // Drain the body even though nothing wants it: an unconsumed response
    // holds its connection out of the pool.
    Effect.tap((response) => Effect.ignore(response.text)),
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.timeout(TIMEOUT),
    Effect.match({ onFailure: (): CallOutcome => "failed", onSuccess: (): CallOutcome => "ok" }),
  );
