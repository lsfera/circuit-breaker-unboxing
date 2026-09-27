import { Effect, Option as O, Tracer } from "effect";

/**
 * Carrying a trace across the broker. It lives in `@egress/rmq`, not `@egress/tracing`, because the boundary is
 * what it is about: AMQP has no notion of a trace, so the context travels as an ordinary application header.
 * It needs `effect` alone: publishing a message does not require a tracing stack.
 */

/** The W3C `traceparent` header, in W3C's format so that something else (a collector, the next service) can read the trace. */
export const TRACEPARENT = "traceparent";

/** `00-<32 hex trace id>-<16 hex span id>-<2 hex flags>`. */
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

/** The current span as a `traceparent`, or `None` when nothing is traced (the common case): absence is a value, not a failure. */
export const traceparent: Effect.Effect<O.Option<string>> = Effect.map(
  Effect.option(Effect.currentSpan),
  O.map((span) => `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`),
);

/**
 * Turn a `traceparent` back into a parent span, so work past the broker joins the trace that caused it. `None` for
 * anything unparseable, quietly and on purpose: a malformed header is a reason to lose a trace, never a message.
 */
export const parentFrom = (header: string): O.Option<Tracer.ExternalSpan> =>
  O.fromNullOr(TRACEPARENT_RE.exec(header)).pipe(
    // Bit 0 of the flags is `sampled`: an unsampled root must stay unsampled past the broker.
    O.map(([, traceId, spanId, flags]) =>
      Tracer.externalSpan({ traceId: traceId!, spanId: spanId!, sampled: (parseInt(flags!, 16) & 1) === 1 }),
    ),
  );

