import { Effect, Option as O, Predicate, Record as Rec, Result, Schema, SchemaGetter, SchemaIssue } from "effect";

/**
 * Content negotiation. RabbitMQ neither validates nor uses `content_type`, and AMQP has no `Accept`, so what a body
 * is and whether an application reads it is decided here, before the body is touched. A `Parser` also writes the
 * format it reads where it can, which is how a publisher and a consumer share one `Contract` (`Contract.ts`).
 */

/** What the publisher declared about the body. */
export type Declared = {
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  readonly type: O.Option<string>;
};

/**
 * How a body of one media type is read: its bytes in, a value out for the contract to decode; and, encoding, the
 * same value back to bytes. A schema rather than a function, so a body that does not parse is an answer, not an
 * exception. `text(Schema.fromJsonString(Schema.Unknown))` reads and writes JSON; `bytes({ decode, encode })` wraps
 * a binary codec, and `bytes(decode)` a decoder that only reads.
 */
export type Parser = Schema.Codec<unknown, Uint8Array>;

/**
 * Bytes through `decode` into a `to`, and back through `encode` when there is one. Either throwing makes the value
 * an issue, an answer rather than an exception; with no `encode`, encoding is refused.
 */
const fromBytes = <A>(
  to: Schema.Codec<A>,
  expected: string,
  decode: (body: Uint8Array) => A,
  encode?: (value: A) => Uint8Array
) =>
  Schema.Uint8Array.pipe(
    Schema.decodeTo(to, {
      decode: SchemaGetter.transformEffect((body, options) =>
        Effect.try({ try: () => decode(body), catch: () => new SchemaIssue.InvalidValue({ expected }, body, options) })
      ),
      encode: encode === undefined
        ? SchemaGetter.forbidden(() => "this parser only reads")
        : SchemaGetter.transformEffect((value, options) =>
          Effect.try({
            try: () => encode(value),
            catch: () => new SchemaIssue.InvalidValue({ expected: `a value ${expected} can hold` }, value, options)
          })
        )
    })
  );

const utf8 = new TextDecoder("utf-8", { fatal: true });
const toUtf8 = new TextEncoder();

/** Fatal on bytes that are not UTF-8: replacement characters would make it a different message. */
const Utf8 = fromBytes(Schema.String, "UTF-8 text", (body) => utf8.decode(body), (text) => toUtf8.encode(text));

/** A parser for a text format, such as `Schema.fromJsonString(…)`, reading the body as UTF-8 and writing it so. */
export const text = (parser: Schema.Codec<unknown, string>): Parser => Utf8.pipe(Schema.decodeTo(parser));

/** A binary format's two directions, such as protobuf's `decode` and `encode`. */
export type BinaryCodec = {
  readonly decode: (body: Uint8Array) => unknown;
  readonly encode: (value: unknown) => Uint8Array;
};

/** The parsers made to read only, so a publisher can refuse one when it is chosen rather than on every message. */
const readOnly = new WeakSet<Parser>();

/**
 * Whether `parser` can write a body: false for one made from a decoder alone. A text parser writes when the
 * schema it wraps encodes, which `Schema.fromJsonString` does.
 */
export const canWrite = (parser: Parser): boolean => !readOnly.has(parser);

/**
 * A parser for a binary format: from a decoder alone, which only reads, or from a `BinaryCodec`, which also writes.
 * The decoder gets a plain `Uint8Array` view, never a `Buffer`: protobufjs 7's `Buffer` path read a truncated
 * string as a shorter one.
 */
export const bytes = (codec: BinaryCodec | ((body: Uint8Array) => unknown)): Parser => {
  const { decode, encode } = Predicate.isFunction(codec) ? { decode: codec, encode: undefined } : codec;
  const parser = fromBytes(
    Schema.Unknown,
    "a body its decoder reads",
    (body) => decode(new Uint8Array(body.buffer, body.byteOffset, body.byteLength)),
    encode
  );
  if (encode === undefined) readOnly.add(parser);
  return parser;
};

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
  options: { readonly undeclared?: string; readonly type?: string; } = {}
): Negotiate => {
  const byMediaType = Rec.mapKeys(parsers, mediaType);
  const parserFor = (contentType: string) => Rec.get(byMediaType, mediaType(contentType));
  return (declared) =>
    O.match(declared.contentEncoding, { onNone: () => true, onSome: unencoded }) &&
      O.match(declared.type, { onNone: () => true, onSome: (t) => options.type === undefined || t === options.type })
      ? O.match(declared.contentType, {
        onNone: () => O.flatMap(O.fromUndefinedOr(options.undeclared), parserFor),
        onSome: parserFor
      })
      : O.none();
};

/** Why a delivery was not read: its declared format is declined, or its body is not a message of the contract. */
export type Unreadable = "format" | "malformed";

/** Negotiate, parse, then decode with the contract: two steps, so a body that does not parse and one that is not a message are both `malformed`, and neither throws. */
export const read =
  (negotiate: Negotiate, decode: (input: unknown) => O.Option<unknown>) =>
  (body: Uint8Array, declared: Declared): Result.Result<unknown, Unreadable> =>
    O.match(negotiate(declared), {
      onNone: () => Result.fail("format" as const),
      onSome: (parse) =>
        Result.fromOption(O.flatMap(Schema.decodeUnknownOption(parse)(body), decode), () => "malformed" as const)
    });
