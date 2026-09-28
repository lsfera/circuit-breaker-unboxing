/**
 * The chaos harnesses' load generator: publishes `message_id: <run>:<n>` at a rate the parent sets over IPC, and on
 * `stop` returns a bitmap of every n the broker confirmed (unroutable excluded). What was in flight when the broker
 * went away is never confirmed. FORMAT: `json` (default), `protobuf`, or `mixed` (alternating).
 */

import { createRequire } from "node:module";

const amqp = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("amqplib");
const protobuf = createRequire(new URL("../packages/rmq-producer/package.json", import.meta.url))("protobufjs");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const RUN = process.env.RUN_ID;
const QUEUE = process.env.QUEUE;
const API = process.env.API_ID;
const URL_ = process.env.AMQP_URL ?? "amqp://guest:guest@rabbitmq:5672";
const FORMAT = process.env.FORMAT ?? "json";

// The same message packages/rmq-producer writes: `message Work { string api_id = 1; int64 n = 2; }`.
const Work = protobuf.Type.fromJSON("Work", { fields: { apiId: { type: "string", id: 1 }, n: { type: "int64", id: 2 } } });
const asJson = (i) => [Buffer.from(JSON.stringify({ apiId: API, n: i })), "application/json"];
const asProtobuf = (i) => [Buffer.from(Work.encode({ apiId: API, n: i }).finish()), "application/x-protobuf"];
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
const unroutable = new Set();

const connect = async () => {
  while (!stopping) {
    try {
      const conn = await amqp.connect(URL_);
      conn.on("error", () => {});
      const ch = await conn.createConfirmChannel();
      ch.on("error", () => {});
      // Unroutable with mandatory set: the broker returns it before it confirms it.
      ch.on("return", (msg) => {
        returned++;
        unroutable.add(String(msg.properties.messageId ?? ""));
      });
      const lost = () => {
        if (channel === ch) channel = null;
      };
      ch.on("close", lost);
      conn.on("close", lost);
      channel = ch;
      return;
    } catch {
      await sleep(1000);
    }
  }
};

const publishLoop = async () => {
  while (!stopping) {
    if (!channel) {
      await connect();
      rateSince = Date.now();
      sentAtRate = 0;
      continue;
    }
    const due = Math.min(4000, Math.floor((rate * (Date.now() - rateSince)) / 1000) - sentAtRate);
    const ch = channel;
    let blocked = false;
    for (let k = 0; k < due && channel === ch; k++) {
      const i = n++;
      sentAtRate++;
      const key = `${RUN}:${i}`;
      const [body, contentType] = encode(i);
      const ok = ch.sendToQueue(
        QUEUE,
        body,
        { persistent: true, mandatory: true, messageId: key, contentType, type: "egress.work" },
        (err) => {
          if (err) nacked++;
          else if (unroutable.has(key)) unroutable.delete(key);
          else {
            confirmed++;
            mark(i);
          }
        },
      );
      if (!ok) {
        blocked = true;
        break;
      }
    }
    if (blocked && channel === ch) await Promise.race([new Promise((r) => ch.once("drain", r)), sleep(1000)]);
    else await sleep(10);
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
      bits: Buffer.from(bits.subarray(0, Math.ceil(n / 8))).toString("base64"),
    });
    await sleep(200);
    process.exit(0);
  }
});

setInterval(() => process.send({ type: "stats", sent: n, confirmed, connected: channel !== null }), 1000).unref();
publishLoop();
