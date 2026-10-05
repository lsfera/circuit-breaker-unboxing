import { ConfigProvider, Effect, Option as O } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { tracingEndpoint } from "../src/Tracing.ts";

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
