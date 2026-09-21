import { Duration, Effect } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import { IDEMPOTENCY_KEY_HTTP_HEADER } from "@egress/rmq/WorkQueue.ts";

/**
 * The one third-party call this daemon makes, reduced to whether it worked.
 *
 * Nothing here distinguishes a timeout, a refused connection or a 5xx from
 * one another — that distinction is exactly what a breaker exists to make.
 * The idempotency key rides along as the third party's own HTTP header, so a
 * redelivered message repeats the same request rather than a new one.
 */

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
