import { GenericContainer, Wait } from "testcontainers";
import { Effect } from "effect";
import type { StartedTestContainer } from "testcontainers";

/**
 * One RabbitMQ per test file, and one declaration of what "a RabbitMQ" is.
 *
 * Node runs each test file in its own process, so each gets its own broker —
 * deliberately, since `Client.test.ts` induces a client bug that leaves the
 * broker unreliable afterwards (see that file's header). What is *not*
 * deliberate is declaring the image tag and the wait strategy once per file:
 * bumping one and not the other means two suites silently testing against
 * different brokers.
 *
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

/** Run a command inside the broker container — `rabbitmqctl`, in practice. */
export const brokerExec = (command: ReadonlyArray<string>): Promise<unknown> =>
  container!.exec([...command]);

/**
 * Restart the broker and re-read where it landed. The mapped port changes, and
 * a test that restarts without re-reading it reconnects to nothing — which is
 * why this is here rather than at the one call site that needs it.
 */
export const restartBroker = async (settleMs = 3000): Promise<void> => {
  await container!.restart();
  await new Promise((r) => setTimeout(r, settleMs));
  broker.host = container!.getHost();
  broker.port = container!.getMappedPort(5672);
};

export const skipIfNoDocker = (t: { skip: (reason: string) => void }): boolean => {
  if (broker.available) return false;
  t.skip("Docker is not available in this environment");
  return true;
};

/**
 * Poll until `done()` or the deadline, instead of sleeping a fixed amount and
 * hoping.
 *
 * A fixed sleep is fine for a test that is the only thing touching the broker
 * at that moment. It stops being fine for one running behind the stranding
 * test, which leaves the broker cleaning up thousands of stranded deliveries
 * across a dozen closed links — a budget that is generous in isolation is not
 * generous behind that. Waiting for the condition is what makes those tests
 * describe a property rather than a timing.
 */
export const waitFor = (done: () => boolean, timeoutMs = 15_000) =>
  Effect.promise(async () => {
    const deadline = Date.now() + timeoutMs;
    while (!done() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
  });
