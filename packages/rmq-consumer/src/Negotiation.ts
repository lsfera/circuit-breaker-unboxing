import { Option as O, Record as Rec, Result, Schema } from "effect";

/**
 * Content negotiation, the reader's side. RabbitMQ neither validates nor uses `content_type`, and AMQP has no
 * `Accept`, so what a body is and whether this application reads it is decided here, before the body is touched.
 */

/** What the publisher declared about the body. */
export type Declared = {
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  readonly type: O.Option<string>;
};

/**
 * How a body of one media type is read: text in, a value out for the contract to decode. A schema rather than a
 * function, so a body that does not parse is an answer, not an exception. `Schema.fromJsonString(Schema.Unknown)`
 * reads JSON.
 */
export type Parser = Schema.Codec<unknown, string, never, unknown>;

/**
 * The parser for what the publisher declared, or `None` if this application cannot read it (the delivery is parked
 * unread). The message schema then decides whether what it parsed is a message.
 */
export type Negotiate = (declared: Declared) => O.Option<Parser>;

const mediaType = (contentType: string): string => contentType.split(";")[0]!.trim().toLowerCase();

/** `content_encoding` may list several, comma-separated; only "nothing applied" is readable. */
const unencoded = (contentEncoding: string): boolean =>
  contentEncoding.split(",").every((encoding) => ["", "identity"].includes(encoding.trim().toLowerCase()));

/**
 * Negotiation by media type, every choice explicit. `parsers` is keyed by the media types read (compared without
 * parameters, case-insensitively). Only unencoded bodies. `undeclared` names the parser for a message whose
 * publisher set no `content_type` at all; omitted, such a message is declined. `type`, when given, must equal the
 * AMQP `type` of a message that declares one.
 */
export const accept = (
  parsers: Record<string, Parser>,
  options: { readonly undeclared?: string; readonly type?: string } = {},
): Negotiate => {
  const byMediaType = Rec.mapKeys(parsers, mediaType);
  const parserFor = (contentType: string) => Rec.get(byMediaType, mediaType(contentType));
  return (declared) =>
    O.match(declared.contentEncoding, { onNone: () => true, onSome: unencoded }) &&
    O.match(declared.type, { onNone: () => true, onSome: (t) => options.type === undefined || t === options.type })
      ? O.match(declared.contentType, {
          onNone: () => O.flatMap(O.fromUndefinedOr(options.undeclared), parserFor),
          onSome: parserFor,
        })
      : O.none();
};

/** Why a delivery was not read: its declared format is declined, or its body is not a message of the contract. */
export type Unreadable = "format" | "malformed";

/** Negotiate, parse, then decode with the contract: two steps, so a body that does not parse and one that is not a message are both `malformed`, and neither throws. */
export const read =
  (negotiate: Negotiate, decode: (input: unknown) => O.Option<unknown>) =>
  (body: string, declared: Declared): Result.Result<unknown, Unreadable> =>
    O.match(negotiate(declared), {
      onNone: () => Result.fail("format" as const),
      onSome: (parse) =>
        Result.fromOption(O.flatMap(Schema.decodeUnknownOption(parse)(body), decode), () => "malformed" as const),
    });
