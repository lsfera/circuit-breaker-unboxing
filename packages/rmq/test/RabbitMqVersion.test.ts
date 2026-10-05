import assert from "node:assert/strict";
import { test } from "vitest";
import {
  assertSupportedRabbitMqVersion,
  MINIMUM_RABBITMQ_VERSION,
  UnsupportedRabbitMqVersionError
} from "../src/RabbitMqVersion.ts";

test("accepts RabbitMQ at or above the minimum version", () => {
  assert.equal(MINIMUM_RABBITMQ_VERSION, "4.3.0");
  for (const version of ["4.3.0", "4.3.1", "4.10.0", "5.0.0"]) {
    assert.doesNotThrow(() => assertSupportedRabbitMqVersion(version), version);
  }
});

test("rejects RabbitMQ versions below the minimum with an actionable error", () => {
  for (const version of ["3.13.7", "4.2.99"]) {
    assert.throws(
      () => assertSupportedRabbitMqVersion(version),
      (error) =>
        error instanceof UnsupportedRabbitMqVersionError
        && error.message.includes(`requires RabbitMQ ${MINIMUM_RABBITMQ_VERSION} or newer`)
        && error.message.includes(version)
    );
  }
});

test("rejects broker version strings it cannot verify", () => {
  for (const version of ["", "4.3", "4.3.0-rc.1", "unknown"]) {
    assert.throws(() => assertSupportedRabbitMqVersion(version), UnsupportedRabbitMqVersionError);
  }
});
