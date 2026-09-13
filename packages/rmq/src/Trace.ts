import { Effect, Option as O, Tracer } from "effect";

/**
 * Carrying a trace across the broker.
 *
 * This lives in `@egress/rmq` rather than in `@egress/tracing` because the
 * boundary is what it is about: AMQP has no notion of a trace, so the context
 * travels as an ordinary application header, and the component that owns the
 * headers should own that. It needs `effect` and nothing else — the
 * OpenTelemetry SDK stays behind `@egress/tracing`, so the dependency edge
 * says what is true: publishing a message does not require a tracing stack.
 */

/**
 * The W3C `traceparent` header, which is how a span crosses a broker.
 *
 * AMQP has no notion of a trace, so the context travels as an ordinary
 * application header next to the ones the redrive already stamps. W3C's format
 * rather than anything of our own because the point of a trace is that
 * something else can read it — a collector, a sidecar, or the next service,
 * none of which know about this repo.
 */
export const TRACEPARENT = "traceparent";

/** `00-<32 hex trace id>-<16 hex span id>-<2 hex flags>`. */
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

/**
 * The current span as a `traceparent`, or `None` when nothing is being traced —
 * which is the common case, and why absence is a value here rather than a
 * failure or an invented id.
 */
export const traceparent: Effect.Effect<O.Option<string>> = Effect.map(
  Effect.option(Effect.currentSpan),
  O.map((span) => `00-${span.traceId}-${span.spanId}-01`),
);

/**
 * Turn a `traceparent` back into a parent span, so work on the far side of the
 * broker joins the trace that caused it rather than starting its own.
 *
 * `None` for anything unparseable, deliberately and quietly: a malformed
 * header is a reason to lose a trace, never a reason to lose a message.
 */
export const parentFrom = (header: string | undefined): O.Option<Tracer.ExternalSpan> => {
  if (header === undefined) return O.none();
  const match = TRACEPARENT_RE.exec(header);
  if (match === null) return O.none();
  return O.some(Tracer.externalSpan({ traceId: match[1]!, spanId: match[2]! }));
};

