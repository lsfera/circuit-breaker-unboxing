# 008 — Configuration is a boundary too

**Status**: decided 2026-09-07.

## Decision

Each process decodes its settings once, at boot, with `Config` and
`effect/unstable/cli`. A value the process cannot use stops it with a message
naming the variable, before any socket opens.

## Why

Settings were `Number(process.env.X ?? "5")`. Measured against the real code:

| Mistyped | Effect |
|---|---|
| `FLEET_SIZE` | `Array.from({ length: NaN })` is empty: every daemon idle, healthy, forever |
| `MAX_IN_FLIGHT` | `inFlight < NaN` is false: the first message waits for ever |
| `REDRIVE_MAX` | `moved >= NaN` is false: an unbounded pass |

None crashed. Booleans compared to `"true"` made `1`, `yes` and `TRUE` mean
off, and an unrecognised `--source` silently meant the simulator.

## Shape

- `@egress/config` holds the shared pieces: `PositiveInt` (zero capacity is the
  same stall as `NaN`), the `host:port` broker address, the metrics port.
- `read` returns an `Effect` failing with `SettingsUnreadable`; each `main.ts`
  wraps its graph in `Layer.unwrap`. A library never calls `process.exit`.
- Every entry point is a CLI command: unknown flags fail with a suggestion,
  `--help` lists everything, and container settings fall back to their
  environment variable (`Flag.withFallbackConfig`).
- Pure cores still take configuration as arguments; that is what keeps them
  testable with plain assertions.

## Gotcha

The Dockerfile lists workspace manifests one per line. A new package missing
from it fails at runtime with `ERR_MODULE_NOT_FOUND`, not at build.
