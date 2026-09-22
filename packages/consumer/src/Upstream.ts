import { Cause, Duration, Effect, Predicate } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { IDEMPOTENCY_KEY_HTTP_HEADER } from "@egress/rmq/WorkQueue.ts";

/**
 * The one third-party call this daemon makes, reduced to what came back: the HTTP status, or `timeout` /
 * `network` when there was none. It judges nothing; what a status means is the breaker policy's to say
 * (Breaker.ts). The idempotency key rides along as the third party's own HTTP header, so a redelivered message
 * repeats the same request rather than a new one.
 */

export type CallStatus = number | "timeout" | "network";

export const isCallStatus = (u: unknown): u is CallStatus => Predicate.isNumber(u) || u === "timeout" || u === "network";

const TIMEOUT = Duration.seconds(2);

export const call = (url: string, idempotencyKey: string): Effect.Effect<CallStatus, never, HttpClient.HttpClient> =>
  HttpClient.HttpClient.pipe(
    Effect.flatMap((client) =>
      client.execute(HttpClientRequest.get(url).pipe(HttpClientRequest.setHeader(IDEMPOTENCY_KEY_HTTP_HEADER, idempotencyKey))),
    ),
    // Drain the body even though nothing wants it: an unconsumed response
    // holds its connection out of the pool.
    Effect.tap((response) => Effect.ignore(response.text)),
    Effect.map((response): CallStatus => response.status),
    Effect.timeout(TIMEOUT),
    Effect.catch((error) => Effect.succeed<CallStatus>(Cause.isTimeoutError(error) ? "timeout" : "network")),
  );
