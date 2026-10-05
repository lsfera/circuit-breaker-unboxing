/**
 * The chaos harnesses' load generator: publishes `message_id: <run>:<n>` at a rate the parent sets over IPC, and on
 * `stop` returns a bitmap of every n the broker confirmed (unroutable excluded). What was in flight when the broker
 * went away is never confirmed. FORMAT: `json` (default), `protobuf`, or `mixed` (alternating).
 */

import { createRequire } from "node:module";
import { connect as open, utf8 } from "./amqp.mjs";

const protobuf = createRequire(new URL("../packages/rmq-producer/package.json", import.meta.url))("protobufjs");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const RUN = process.env.RUN_ID;
const QUEUE = process.env.QUEUE;
const API = process.env.API_ID;
const URL_ = process.env.AMQP_URL ?? "amqp://guest:guest@rabbitmq:5672";
const FORMAT = process.env.FORMAT ?? "json";

// The same message packages/rmq-producer writes: `message Work { string api_id = 1; int64 n = 2; }`.
const Work = protobuf.Type.fromJSON("Work", { fields: { apiId: { type: "string", id: 1 }, n: { type: "int64", id: 2 } } });
const asJson = (i) => [utf8(JSON.stringify({ apiId: API, n: i })), "application/json"];
const asProtobuf = (i) => [Work.encode({ apiId: API, n: i }).finish(), "application/x-protobuf"];
const encode = { json: asJson, protobuf: asProtobuf, mixed: (i) => (i % 2 === 0 ? asJson(i) : asProtobuf(i)) }[FORMAT];
if (!encode) throw new Error(`FORMAT must be json, protobuf or mixed, got ${FORMAT}`);

let bits = new Uint8Array(1 << 18);
const mark = (i) => {
  const b = i >> 3;
  if (b >= bits.length) {
    const grown = new Uint8Array(Math.max(bits.length * 2, b + 1));
    grown.set(bits);
    bits = grown;
  }
  bits[b] |= 1 << (i & 7);
};

let rate = 0;
let rateSince = Date.now();
let sentAtRate = 0;
let n = 0;
let confirmed = 0;
let nacked = 0;
let returned = 0;
let stopping = false;
let channel = null;
let client = null;
const unroutable = new Set();
/** Published and not yet confirmed, nacked or returned: the cap is the backpressure, as the socket's was. */
let inFlight = 0;
const MAX_IN_FLIGHT = 4000;

const connect = async () => {
  while (!stopping) {
    try {
      const conn = await open(URL_);
      const ch = await conn.channel();
      await ch.confirmSelect();
      // Unroutable with mandatory set: the broker returns it before it confirms it.
      ch.onReturn = (msg) => {
        returned++;
        unroutable.add(String(msg.properties.messageId ?? ""));
      };
      const lost = () => {
        if (channel === ch) channel = null;
      };
      ch.onerror = lost;
      conn.onerror = () => {};
      conn.ondisconnect = lost;
      client = conn;
      channel = ch;
      return;
    } catch {
      await sleep(1000);
    }
  }
};

const publishLoop = async () => {
  while (!stopping) {
    if (!channel || channel.closed) {
      channel = null;
      await connect();
      rateSince = Date.now();
      sentAtRate = 0;
      continue;
    }
    // A broker alarm blocks the connection, and the client refuses a publish meanwhile: wait it out.
    if (client.blocked) {
      await sleep(100);
      continue;
    }
    const due = Math.min(4000, Math.floor((rate * (Date.now() - rateSince)) / 1000) - sentAtRate);
    const ch = channel;
    for (let k = 0; k < due && channel === ch && inFlight < MAX_IN_FLIGHT; k++) {
      const i = n++;
      sentAtRate++;
      inFlight++;
      const key = `${RUN}:${i}`;
      const [body, contentType] = encode(i);
      // Resolves on the broker's confirm; rejects on a nack, or when the channel goes before it confirms.
      ch.basicPublish("", QUEUE, body, { deliveryMode: 2, messageId: key, contentType, type: "egress.work" }, true).then(
        () => {
          if (unroutable.has(key)) unroutable.delete(key);
          else {
            confirmed++;
            mark(i);
          }
        },
        () => void nacked++,
      ).finally(() => void inFlight--);
    }
    await sleep(10);
  }
};

process.on("message", async (msg) => {
  if (msg.type === "rate") {
    rate = msg.rate;
    rateSince = Date.now();
    sentAtRate = 0;
  }
  if (msg.type === "stop") {
    stopping = true;
    const until = Date.now() + 30_000;
    while (confirmed + nacked + returned < n && Date.now() < until) await sleep(100);
    process.send({
      type: "final",
      sent: n,
      confirmed,
      nacked,
      returned,
      unsettled: n - confirmed - nacked - returned,
      bits: bits.subarray(0, Math.ceil(n / 8)).toBase64(),
    });
    await sleep(200);
    process.exit(0);
  }
});

setInterval(() => process.send({ type: "stats", sent: n, confirmed, connected: channel !== null }), 1000).unref();
publishLoop();
