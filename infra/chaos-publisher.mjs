/**
 * The chaos harness's load generator, forked by infra/chaos-load.mjs: publishes
 * onto the work queue at a rate the parent changes over IPC, and remembers
 * exactly which messages the broker confirmed.
 *
 * Every message carries `<run>:<n>` as its AMQP message_id, the idempotency key
 * a consumer forwards to the third party. On `stop` it waits for
 * outstanding confirms, then sends back a bitmap with bit n set for every
 * message confirmed and not returned unroutable — the set the harness checks
 * against what the upstream actually processed.
 *
 * It reconnects when the broker goes away; what was in flight at that moment
 * is simply never confirmed, which is the honest answer.
 */

import { createRequire } from "node:module";

const amqp = createRequire(new URL("../packages/rmq/package.json", import.meta.url))("amqplib");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const RUN = process.env.RUN_ID;
const QUEUE = process.env.QUEUE;
const API = process.env.API_ID;
const URL_ = process.env.AMQP_URL ?? "amqp://guest:guest@rabbitmq:5672";

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
      const ok = ch.sendToQueue(
        QUEUE,
        Buffer.from(JSON.stringify({ apiId: API, n: i })),
        // The wire format @egress/rmq/ControlPlane.ts declares for a work message.
        { persistent: true, mandatory: true, messageId: key, contentType: "application/json", type: "egress.work" },
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
