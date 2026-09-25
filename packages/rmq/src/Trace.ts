import { Effect, Option as O, Tracer } from "effect";

/**
 * A trace across the broker, as a W3C `traceparent` header. Here, not in
 * `@egress/tracing`, so publishing needs no OpenTelemetry stack.
 */

export const TRACEPARENT = "traceparent";

/** `00-<32 hex trace id>-<16 hex span id>-<2 hex flags>`. */
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

/** `None` when nothing is traced, the common case. */
export const traceparent: Effect.Effect<O.Option<string>> = Effect.map(
  Effect.option(Effect.currentSpan),
  O.map((span) => `00-${span.traceId}-${span.spanId}-01`),
);

/** `None` for anything unparseable: a bad header loses a trace, never a message. */
export const parentFrom = (header: string): O.Option<Tracer.ExternalSpan> =>
  O.map(O.fromNullOr(TRACEPARENT_RE.exec(header)), ([, traceId = "", spanId = ""]) =>
    Tracer.externalSpan({ traceId, spanId }),
  );
