import assert from "node:assert/strict";
import { test } from "vitest";
import {
  assertSupportedRabbitMqVersion,
  MINIMUM_RABBITMQ_VERSION,
  rabbitMqVersionAtLeast,
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

test("rabbitMqVersionAtLeast gates a feature on a version, and stays off for one it cannot verify", () => {
  for (const version of ["4.4.0", "4.4.3", "4.10.0", "5.0.0", "4.4.0+beta.1"]) {
    assert.equal(rabbitMqVersionAtLeast(version, "4.4.0"), true, version);
  }
  for (const version of ["4.3.9", "4.3.0", "3.13.7", "", "unknown", "4.4", "4.4.0-rc.1"]) {
    assert.equal(rabbitMqVersionAtLeast(version, "4.4.0"), false, version);
  }
});
