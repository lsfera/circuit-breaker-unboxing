import { Array as Arr, Effect } from "effect";
import { Rmq } from "./Client.ts";
import type { Publisher, RmqError, SendOptions } from "./Client.ts";

/**
 * Delivery held in the broker for up to about 36 hours, with nothing but RabbitMQ doing the holding: the
 * binary-counter chain of NServiceBus's RabbitMQ transport
 * (https://docs.particular.net/transports/rabbitmq/delayed-delivery), at one-second resolution and `LEVELS`
 * levels instead of 28.
 *
 * Level `n` is a queue whose messages all expire after exactly 2^n seconds and dead-letter into level `n-1`'s
 * exchange; level 0 dead-letters into the delivery exchange. A delay of `d` seconds is `d` written in binary,
 * one word per level, and every level's exchange asks whether its bit is set: set, the message waits in this
 * level's queue; clear, it is passed straight down. Every message in a queue shares one TTL, so the next to
 * expire is always the head, which per-queue TTL handles exactly and a queue of mixed TTLs would not.
 *
 * The routing key is `<LEVELS bits, high first>.<destination>`, and the destination is whatever `#.<destination>`
 * a queue bound on the delivery exchange. Nothing here knows what a breaker is.
 */

/** 2^17 - 1 seconds is 36.4 hours, the first power of two past a day. */
export const LEVELS = 17;
export const MAX_DELAY_SECONDS = 2 ** LEVELS - 1;

export const DELIVERY_EXCHANGE = "rmq.delay.delivery";
export const levelName = (level: number): string => `rmq.delay.level.${String(level).padStart(2, "0")}`;

/** Level `n` reads word `LEVELS - 1 - n` of the routing key, so the key reads high bit first. */
const wordOf = (level: number): number => LEVELS - 1 - level;
const wildcards = (words: number): string => "*.".repeat(words);

/** Whole seconds, at least 1, at most `MAX_DELAY_SECONDS`. */
export const clampSeconds = (seconds: number): number => Math.min(MAX_DELAY_SECONDS, Math.max(1, Math.ceil(seconds)));

/** One `0` or `1` per level, high bit first. */
export const bits = (seconds: number): ReadonlyArray<string> =>
  Arr.makeBy(LEVELS, (word) => String((clampSeconds(seconds) >> (LEVELS - 1 - word)) & 1));

export const routingKey = (seconds: number, destination: string): string => [...bits(seconds), destination].join(".");

/** The first level whose bit is set: the top of the chain a message has to enter. */
export const entryLevel = (seconds: number): number => LEVELS - 1 - bits(seconds).indexOf("1");

/** What a destination queue binds to the delivery exchange to receive its own delayed messages. */
export const bindingKey = (destination: string): string => `#.${destination}`;

/**
 * The chain, idempotent and safe to declare from every process. Durable and quorum, since a message in it is a
 * promise that outlives a broker restart, with `at-least-once` dead-lettering so a hop between levels is never
 * where one is lost.
 */
export const declare = Effect.fnUntraced(function* () {
  const rmq = yield* Rmq;
  const levels = Arr.makeBy(LEVELS, (n) => n);
  const nextOf = (level: number): string => (level === 0 ? DELIVERY_EXCHANGE : levelName(level - 1));

  yield* Effect.forEach([DELIVERY_EXCHANGE, ...Arr.map(levels, levelName)], (name) =>
    rmq.declareTopicExchange(name, { durable: true }),
  );
  yield* Effect.forEach(levels, (n) =>
    Effect.gen(function* () {
      const queue = yield* rmq.declareQueue(levelName(n), {
        args: {
          "x-queue-type": "quorum",
          "x-message-ttl": 1000 * 2 ** n,
          "x-dead-letter-exchange": nextOf(n),
          "x-dead-letter-strategy": "at-least-once",
          "x-overflow": "reject-publish",
        },
      });
      yield* rmq.bind(`${wildcards(wordOf(n))}1.#`, levelName(n), queue);
      yield* rmq.bindExchange(`${wildcards(wordOf(n))}0.#`, levelName(n), nextOf(n));
    }),
  );
});

/** Give an already-declared `queue` its own delayed messages. Run after `declare`. */
export const receive = Effect.fnUntraced(function* (queue: string) {
  const rmq = yield* Rmq;
  yield* rmq.bind(bindingKey(queue), DELIVERY_EXCHANGE, queue);
});

/**
 * Deliver `body` to `destination` in `seconds`, give or take the chain's own latency (one TTL check per level
 * the message waits in). Confirmed once it is in the first queue, which is when the delay is a promise.
 */
export const sendDelayed = Effect.fnUntraced(function* (
  destination: string,
  seconds: number,
  body: string,
  options?: SendOptions,
): Effect.fn.Return<void, RmqError, Rmq> {
  const rmq = yield* Rmq;
  const publisher: Publisher = yield* rmq.publisherToExchange(
    levelName(entryLevel(seconds)),
    routingKey(seconds, destination),
  );
  yield* rmq.send(publisher, body, options);
});
