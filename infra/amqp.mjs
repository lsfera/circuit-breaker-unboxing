/**
 * The AMQP client the scripts share: the one `@egress/rmq` runs on, loaded from that package's dependencies because
 * `infra/` is not a workspace package of its own.
 */
import { createRequire } from "node:module";

const { AMQPClient } = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("@cloudamqp/amqp-client");

/** A connected client. A failed connect leaves its socket open, so it is destroyed here rather than left to linger. */
export const connect = async (url) => {
  const client = new AMQPClient(url);
  try {
    await client.connect();
    return client;
  } catch (error) {
    client.socket?.destroy();
    throw error;
  }
};

/** A passive declare: the queue's `messageCount` and `consumerCount`. A queue that does not exist closes the channel. */
export const inspect = (channel, queue) => channel.queueDeclare(queue, { passive: true });

/** Text as the bytes a message body carries. */
export const utf8 = (text) => new TextEncoder().encode(text);
