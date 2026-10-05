import { ConfigProvider, Effect, Layer, Option as O, Tracer } from "effect";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "vitest";
import { tracingEndpoint, TracingLive } from "../src/Tracing.ts";

const endpointFrom = (env: Record<string, string>) =>
  Effect.runSync(
    Effect.provideService(
      Effect.gen(function*() {
        return yield* tracingEndpoint;
      }),
      ConfigProvider.ConfigProvider,
      ConfigProvider.fromEnv({ env })
    )
  );

test("the general endpoint alone is enough, with the traces path added", () => {
  assert.deepEqual(
    endpointFrom({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel:4318" }),
    O.some("http://otel:4318/v1/traces")
  );
  assert.deepEqual(
    endpointFrom({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel:4318/" }),
    O.some("http://otel:4318/v1/traces")
  );
});

test("the traces endpoint is used as given, and wins", () => {
  assert.deepEqual(
    endpointFrom({
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "http://t:4318/custom",
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel:4318"
    }),
    O.some("http://t:4318/custom")
  );
});

test("empty counts as unset, and falls through to the next", () => {
  assert.deepEqual(
    endpointFrom({ OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: " ", OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel:4318" }),
    O.some("http://otel:4318/v1/traces")
  );
  assert.deepEqual(endpointFrom({ OTEL_EXPORTER_OTLP_ENDPOINT: "" }), O.none());
  assert.deepEqual(endpointFrom({}), O.none());
});

type Exported = { readonly service: string; readonly spans: ReadonlyArray<string>; };

type OtlpBody = {
  readonly resourceSpans: ReadonlyArray<{
    readonly resource: {
      readonly attributes: ReadonlyArray<{ readonly key: string; readonly value: { readonly stringValue?: string; }; }>;
    };
    readonly scopeSpans: ReadonlyArray<{ readonly spans: ReadonlyArray<{ readonly name: string; }>; }>;
  }>;
};

/**
 * Runs `program` under `TracingLive` pointed at a collector on a local port, and returns what reached it. The
 * layer's scope closes before this returns, which flushes the batch: nothing waits on the export interval.
 */
const exported = async (env: Record<string, string>, program: Effect.Effect<unknown>): Promise<Array<Exported>> => {
  const received: Array<Exported> = [];
  const collector = createServer((request, response) => {
    const chunks: Array<Buffer> = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as OtlpBody;
      body.resourceSpans.forEach((resource) =>
        received.push({
          service: resource.resource.attributes.find((a) => a.key === "service.name")?.value.stringValue ?? "",
          spans: resource.scopeSpans.flatMap((scope) => scope.spans.map((span) => span.name))
        })
      );
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
    });
  });
  await new Promise<void>((resolve) => collector.listen(0, "127.0.0.1", resolve));
  const { port } = collector.address() as AddressInfo;
  try {
    await Effect.runPromise(
      Effect.scoped(Effect.provide(program, Layer.fresh(TracingLive("tracing-test")))).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({
            env: { EXPOSE_TELEMETRY: "true", OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}`, ...env }
          })
        )
      )
    );
  } finally {
    await new Promise((resolve) => collector.close(resolve));
  }
  return received;
};

const remoteParent = (sampled: boolean) =>
  Tracer.externalSpan({ traceId: "4bf92f3577b34da6a3ce929d0e0e4736", spanId: "00f067aa0ba902b7", sampled });

test("spans reach the OTLP endpoint, named for their service", async () => {
  const received = await exported({}, Effect.void.pipe(Effect.withSpan("child"), Effect.withSpan("root")));
  assert.deepEqual(received.map((r) => r.service), ["tracing-test"]);
  assert.deepEqual(received.flatMap((r) => r.spans).sort(), ["child", "root"]);
});

test("a ratio of 0 drops a root and everything under it", async () => {
  const received = await exported(
    { OTEL_TRACES_SAMPLER_ARG: "0" },
    Effect.void.pipe(Effect.withSpan("child"), Effect.withSpan("root"))
  );
  assert.deepEqual(received.flatMap((r) => r.spans), []);
});

test("the ratio is a root's alone: a span under a sampled remote parent is kept, under an unsampled one dropped", async () => {
  const received = await exported(
    { OTEL_TRACES_SAMPLER_ARG: "0" },
    Effect.all([
      Effect.void.pipe(Effect.withSpan("kept"), Effect.withParentSpan(remoteParent(true))),
      Effect.void.pipe(Effect.withSpan("dropped"), Effect.withParentSpan(remoteParent(false)))
    ])
  );
  assert.deepEqual(received.flatMap((r) => r.spans), ["kept"]);
});

test("off unless asked for: nothing is exported without EXPOSE_TELEMETRY", async () => {
  const received = await exported({ EXPOSE_TELEMETRY: "false" }, Effect.void.pipe(Effect.withSpan("root")));
  assert.deepEqual(received, []);
});
