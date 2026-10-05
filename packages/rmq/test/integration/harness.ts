import { Effect } from "effect";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";

/**
 * One RabbitMQ per test file, and one declaration of what "a RabbitMQ" is. Node runs each test file in its own
 * process, so each gets its own broker (see `DeadLetter.test.ts` for why sharing one is not viable), and the image
 * tag and wait strategy are declared once so two suites cannot silently test against different brokers.
 * Not named `*.test.ts`, so the runner's glob does not pick it up.
 */
export const broker = { host: "", port: 0, available: false };

export const startBroker = async (): Promise<void> => {
  try {
    container = await new GenericContainer("rabbitmq:4.3-management-alpine")
      .withExposedPorts(5672)
      .withWaitStrategy(Wait.forLogMessage(/Server startup complete/))
      .start();
  } catch {
    // No Docker daemon reachable in this environment — skip, don't fail.
    broker.available = false;
    return;
  }
  broker.host = container.getHost();
  broker.port = container.getMappedPort(5672);
  broker.available = true;
};

export const stopBroker = async (): Promise<void> => {
  await container?.stop().catch(() => {});
  container = null;
};

let container: StartedTestContainer | null = null;

/** A body read as UTF-8: bodies are `Uint8Array`, whose `toString` lists bytes rather than decoding them. */
export const text = (body: Uint8Array): string => new TextDecoder().decode(body);

/** Run a command inside the broker container — `rabbitmqctl`, in practice. */
export const brokerExec = (command: ReadonlyArray<string>): Promise<unknown> => container!.exec([...command]);

/**
 * Restart the broker and re-read where it landed: the mapped port changes, and a test that does not re-read it
 * reconnects to nothing.
 */
export const restartBroker = async (settleMs = 3000): Promise<void> => {
  await container!.restart();
  await new Promise((r) => setTimeout(r, settleMs));
  broker.host = container!.getHost();
  broker.port = container!.getMappedPort(5672);
};

export const skipIfNoDocker = (t: { skip: (reason: string) => void; }): boolean => {
  if (broker.available) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

/**
 * Poll until `done()` or the deadline, instead of sleeping a fixed amount and hoping: a fixed sleep stops being
 * generous when the test runs behind one that leaves the broker cleaning up thousands of stranded deliveries.
 * Waiting for the condition makes a test describe a property rather than a timing.
 */
export const waitFor = (done: () => boolean, timeoutMs = 15_000) =>
  Effect.promise(async () => {
    const deadline = Date.now() + timeoutMs;
    while (!done() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
  });
