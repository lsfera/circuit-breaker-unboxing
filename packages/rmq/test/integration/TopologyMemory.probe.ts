/**
 * What one API's queues cost an idle broker, for ADR 020. Not a test (no
 * `.test.ts`): run by hand, it takes about a minute.
 *
 *   node packages/rmq/test/integration/TopologyMemory.probe.ts
 *
 * Declares the per-API topology the daemons declare — five quorum queues, the
 * floor, and a control queue per daemon for a fleet of five — for 100, 250 and
 * 500 APIs on one node, and prints the broker's memory after each.
 */
import { GenericContainer, Wait } from "testcontainers";
import * as amqp from "amqplib";

const c = await new GenericContainer("rabbitmq:4.3-management-alpine")
  .withExposedPorts(5672).withWaitStrategy(Wait.forLogMessage(/Server startup complete/)).start();
const mem = async () => {
  const st = await c.exec(["sh", "-c", "rabbitmqctl -q status | grep -i -A2 'Memory' | head -5"]);
  return st.output.replace(/\n/g, " | ");
};
const conn = await amqp.connect(`amqp://guest:guest@${c.getHost()}:${c.getMappedPort(5672)}`);
const ch = await conn.createChannel();
const out: string[] = [];
out.push(`0 APIs: ${await mem()}`);
let apis = 0;
for (const target of [100, 250, 500]) {
  const t0 = Date.now();
  for (; apis < target; apis++) {
    for (const q of ["work", "work.dead", "work.parked", "probe-trigger", "redrive-trigger"]) {
      await ch.assertQueue(`api${apis}.${q}`, { durable: true, arguments: { "x-queue-type": "quorum" } });
    }
    await ch.assertQueue(`api${apis}.floor`, { durable: true });
    for (let d = 0; d < 5; d++) await ch.assertQueue(`api${apis}.control.d${d}`, { durable: true, arguments: { "x-expires": 600000 } });
  }
  await new Promise((r) => setTimeout(r, 5000));
  out.push(`${target} APIs (${target * 5} quorum, ${target * 6} classic), declared in ${Date.now() - t0} ms: ${await mem()}`);
}
console.log(out.join("\n"));
await conn.close();
await c.stop();
