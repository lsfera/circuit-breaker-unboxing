import { Option as O, Result, Schema } from "effect";
import { readerFor } from "@egress/domain/Model.ts";
import type { CircuitEvent, DecodeFailure } from "@egress/domain/Model.ts";

/** Topology and message shapes shared by the aggregator (publisher) and the daemons (consumers). */

export const CONTROL_EXCHANGE = "circuit.control";

/**
 * The idempotency key as the third party receives it. On the broker it is the
 * `message_id`, assigned once by the producer; every republish must pass
 * `messageId` forward or `send` stamps a new one.
 */
export const IDEMPOTENCY_KEY_HTTP_HEADER = "x-idempotency-key";

/** A work message's identity, stable for its life. `n` alone would collide across producer restarts. */
export const workMessageId = (run: string, n: number): string => `${run}:${n}`;

/** What a work message says, declared once so encoder and decoder can't drift. Unknown fields are ignored. */
export const WorkMessage = Schema.Struct({ apiId: Schema.String, n: Schema.Int });
export type WorkMessage = typeof WorkMessage.Type;

/** The AMQP `content_type` a work publisher declares. AMQP has no `Accept`, so negotiation is the reader's. */
export const WORK_CONTENT_TYPE = "application/json";

/** The AMQP `type` of a work message, dot-separated by RabbitMQ's convention. A daemon declines any other type. */
export const WORK_MESSAGE_TYPE = "egress.work";

const mediaType = (contentType: string): string => contentType.split(";")[0]!.trim().toLowerCase();

/** `content_encoding` may list several, comma-separated; only "nothing applied" is readable. */
const unencoded = (contentEncoding: string): boolean =>
  contentEncoding.split(",").every((encoding) => ["", "identity"].includes(encoding.trim().toLowerCase()));

/** What a delivery declared about itself; `None` wherever the publisher said nothing, which is read as a match. */
type Declared = {
  readonly contentType: O.Option<string>;
  readonly contentEncoding: O.Option<string>;
  readonly type: O.Option<string>;
};

const readsFormat =
  (contentType: string, messageType: string) =>
  (declared: Declared): boolean =>
    O.match(declared.contentType, { onNone: () => true, onSome: (t) => mediaType(t) === contentType }) &&
    O.match(declared.contentEncoding, { onNone: () => true, onSome: unencoded }) &&
    O.match(declared.type, { onNone: () => true, onSome: (t) => t === messageType });

export const readsWorkFormat = readsFormat(WORK_CONTENT_TYPE, WORK_MESSAGE_TYPE);

const WorkMessageJson = Schema.fromJsonString(WorkMessage);
export const encodeWorkMessage = Schema.encodeSync(WorkMessageJson);
export const decodeWorkMessage = Schema.decodeUnknownOption(WorkMessageJson);

/**
 * Calls made since the message was last fresh — not `x-delivery-count`, since a
 * retry is a republished copy. Absent means zero; reset by a redrive.
 */
export const ATTEMPTS_HEADER = "x-egress-attempts";

/**
 * Stamped on anything moved between queues by a publish rather than by the
 * broker's dead-lettering, which is the only thing that sets `x-first-death-*`.
 * Without it the redrive cannot attribute the message.
 */
export const ORIGIN_QUEUE_HEADER = "x-egress-origin-queue";
export const ORIGIN_REASON_HEADER = "x-egress-origin-reason";

/** One routing key per API: a fleet binds only its own and never sees other APIs' events. */
export const routingKeyFor = (apiId: string): string => `circuit.${apiId}`;

/** Every daemon process gets its own queue on the control exchange, never shared. */
export const controlQueueFor = (apiId: string, instanceId: string): string =>
  `${apiId}.control.${instanceId}`;

/** The always-idle SAC queue that elects the HALF_OPEN prober: one per API, contended by that API's whole fleet. */
export const probeTriggerQueueFor = (apiId: string): string => `${apiId}.probe-trigger`;

/**
 * The redrive election, deliberately a separate queue from `probe-trigger`: the
 * two elections are independent, and sharing one would let a single daemon's
 * failure take out both.
 */
export const redriveTriggerQueueFor = (apiId: string): string => `${apiId}.redrive-trigger`;

/** The primary competing-consumer work queue daemons drain. */
export const workQueueFor = (apiId: string): string => `${apiId}.work`;

/** Where work that could not be completed ends up. */
export const deadLetterQueueFor = (apiId: string): string => `${apiId}.work.dead`;

/** Poison: redriven `MAX_REDRIVES` times, refused with a 4xx, or never work at all. Nothing leaves it on its own. */
export const parkedQueueFor = (apiId: string): string => `${apiId}.work.parked`;

/**
 * One dead-letter destination for every queue. Every process must declare a
 * queue identically: RabbitMQ rejects a redeclare whose arguments differ.
 */
const deadLetterArgs = (apiId: string): Record<string, unknown> => ({
  "x-dead-letter-exchange": "",
  "x-dead-letter-routing-key": deadLetterQueueFor(apiId),
});

/** Identical to `deadLetterArgs` today; named separately because here it is designed behaviour, not a backstop. */
const workQueueArgs = deadLetterArgs;

/** Dead-letters, so a malformed trigger is kept. */
const sacQueueArgs = (apiId: string): Record<string, unknown> => ({
  ...deadLetterArgs(apiId),
  "x-single-active-consumer": true,
});

/**
 * Calls per outage, counted in `ATTEMPTS_HEADER` (ADR 016). The same number is the
 * work queue's `x-delivery-limit`, the backstop for a delivery that never reaches
 * a republish. Low because every attempt is load on a failing upstream.
 */
export const WORK_DELIVERY_LIMIT = 3;

/**
 * Every queue is durable: RabbitMQ 4.3 answers a non-exclusive transient declare by
 * closing the connection (541). Control and floor queues are classic with
 * `x-expires`; everything else is quorum.
 */
export const workQueueOptions = (apiId: string) => ({
  args: {
    ...workQueueArgs(apiId),
    "x-queue-type": "quorum",
    "x-delivery-limit": WORK_DELIVERY_LIMIT,
    // At-least-once dead-lettering requires reject-publish on a quorum queue.
    "x-dead-letter-strategy": "at-least-once",
    "x-overflow": "reject-publish",
  },
  durable: true,
});

/**
 * `x-delivery-limit: -1`: a quorum queue defaults to 20, and one with no dead-letter
 * target drops a message at its limit. Every redrive pass returns what it did not
 * move, so the default lost 1,570 dead letters in one chaos run.
 */
export const deadLetterQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/** Terminal like the dead-letter queue, and for the same reason never allowed to drop at a delivery limit. */
export const parkedQueueOptions = () => ({
  args: { "x-queue-type": "quorum", "x-delivery-limit": -1 },
  durable: true,
});

/** Redrives so far; absent means zero. */
export const REDRIVE_COUNT_HEADER = "x-egress-redrive-count";

/** Outages survived, not attempts: each redrive grants a fresh budget. */
export const MAX_REDRIVES = 5;

/**
 * A departed daemon's control queue would otherwise stay bound and fill for ever.
 * Ten minutes outlasts the client's ~5 minutes of reconnecting.
 */
export const CONTROL_QUEUE_EXPIRES_MS = 600_000;

/**
 * Single-active-consumer and bound to `circuit.control`, so every published event
 * re-elects one daemon to run when the fraction alone selects nobody (ADR 013).
 */
export const floorQueueFor = (apiId: string) => `${apiId}.floor`;

export const floorQueueOptions = () => ({
  args: {
    "x-single-active-consumer": true,
    "x-expires": CONTROL_QUEUE_EXPIRES_MS,
    // The lease is short: an event nobody took is worthless within seconds.
    "x-message-ttl": 30_000,
    "x-max-length": 16,
  },
  durable: true,
});

export const controlQueueOptions = (apiId: string) => ({
  args: { ...deadLetterArgs(apiId), "x-expires": CONTROL_QUEUE_EXPIRES_MS },
  durable: true,
});

export const sacQueueOptions = (apiId: string) => ({
  args: { ...sacQueueArgs(apiId), "x-queue-type": "quorum" },
  durable: true,
});

export const CONTROL_EXCHANGE_OPTIONS = { durable: true };

export const encodeCircuitEvent = (event: CircuitEvent): string => JSON.stringify(event);

/**
 * The body on both election queues. `Natural` is load-bearing: the dedupe is
 * `sequence <= seen`, and every comparison against `NaN` is false (ADR 007).
 */
const ElectionTrigger = Schema.Struct({ sequence: Schema.Natural });
type ElectionTrigger = typeof ElectionTrigger.Type;

export const encodeElectionTrigger = (trigger: ElectionTrigger): string =>
  JSON.stringify(trigger);

export const decodeElectionTrigger: (
  body: string,
) => Result.Result<ElectionTrigger, DecodeFailure> = readerFor(ElectionTrigger);
