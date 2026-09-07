---
name: effect-testing
description: Write comprehensive tests using @effect/vitest for Effect code and vitest for pure functions. Use this skill when implementing tests for Effect-based applications, including services, layers, time-dependent effects, error handling, and property-based testing.
---

# Effect Testing Skill

This skill provides comprehensive guidance for testing Effect-based applications using `@effect/vitest` and standard `vitest`.

## Framework Selection

**CRITICAL**: Choose the correct testing framework based on the code being tested.

### Use @effect/vitest for Effect Code

Use `@effect/vitest` when testing:
- Functions that return `Effect<A, E, R>`
- Code that uses services and layers
- Time-dependent operations with `TestClock`
- Asynchronous operations coordinated with Effect
- STM (Software Transactional Memory) operations

```typescript
import { it, expect } from "@effect/vitest"
import { Effect } from "effect"

it.effect("should fetch user", () =>
  Effect.gen(function* () {
    const user = yield* fetchUser("123")
    expect(user.id).toBe("123")
  })
)
```

### Use Regular vitest for Pure Functions

Use standard `vitest` for:
- Pure functions with no Effect wrapper
- Simple data transformations
- Helper utilities
- Type constructors (brands, newtypes)

```typescript
import { describe, expect, it } from "vitest"

describe("Cents", () => {
  it("should add cents correctly", () => {
    const result = Cents.add(Cents.make(100n), Cents.make(50n))
    expect(result).toBe(150n)
  })
})
```

## Test Variants

### it.effect - Default Test Environment

Provides `TestContext` including `TestClock`, `TestRandom`, etc.

```typescript
import { it, expect } from "@effect/vitest"
import { Effect } from "effect"

it.effect("test name", () =>
  Effect.gen(function* () {
    const result = yield* someEffect
    expect(result).toBe(expected)
  })
)
```

### it.live - Live Environment

Uses real services (real clock, real random, etc.).

```typescript
import { it } from "@effect/vitest"
import { Effect, Clock } from "effect"

it.live("test with real time", () =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    // Uses actual system time
  })
)
```

### it.scoped - Resource Management

For tests requiring `Scope` to manage resource lifecycle.

```typescript
import { it } from "@effect/vitest"
import { Effect } from "effect"

it.scoped("test with resources", () =>
  Effect.gen(function* () {
    const resource = yield* Effect.acquireRelease(
      acquire,
      () => release
    )
    // Resource automatically cleaned up after test
  })
)
```

### it.scopedLive - Combined Scoped + Live

Uses live environment with scope for resource management.

## Assertions

### Use expect from @effect/vitest

```typescript
import { it, expect } from "@effect/vitest"
import { Effect } from "effect"

it.effect("assertions", () =>
  Effect.gen(function* () {
    const result = yield* computation
    expect(result).toBe(42)
    expect(result).toBeGreaterThan(0)
  })
)
```

### Effect-Specific Utilities

```typescript
import {
  assertSome,        // For Option.Some
  assertNone,        // For Option.None
  assertRight,       // For Either.Right
  assertLeft,        // For Either.Left
  assertSuccess,     // For Exit.Success
  assertFailure      // For Exit.Failure
} from "@effect/vitest/utils"
```

## Testing with Services and Layers

### Providing Services to Tests

Use `Effect.provide` to supply test implementations:

```typescript
import { it, expect } from "@effect/vitest"
import { Effect, Context, Layer } from "effect"

it.effect("should work with dependencies", () =>
  Effect.gen(function* () {
    const userService = yield* UserService
    const result = yield* userService.getUser("123")
    expect(result.name).toBe("John")
  }).pipe(Effect.provide(TestUserServiceLayer))
)
```

### Using `layer` Helper

Share a layer across multiple tests:

```typescript
import { layer, it, expect } from "@effect/vitest"
import { Effect, Context, Layer } from "effect"

class Database extends Context.Tag("Database")<Database, {
  query: (sql: string) => Effect.Effect<Array<unknown>>
}>() {
  static Test = Layer.succeed(Database, {
    query: (sql) => Effect.succeed([])
  })
}

layer(Database.Test)((it) => {
  it.effect("test 1", () =>
    Effect.gen(function* () {
      const db = yield* Database
      const results = yield* db.query("SELECT *")
      expect(results).toEqual([])
    })
  )

  it.effect("test 2", () =>
    Effect.gen(function* () {
      const db = yield* Database
      // Database available in all tests
    })
  )
})

// With describe block name
layer(Database.Test)("Database tests", (it) => {
  it.effect("query test", () => Effect.succeed(true))
})
```

### Nested Layers

```typescript
layer(DatabaseLayer)((it) => {
  it.layer(UserServiceLayer)("user tests", (it) => {
    it.effect("has both dependencies", () =>
      Effect.gen(function* () {
        const db = yield* Database
        const userService = yield* UserService
        // Both available
      })
    )
  })
})
```

## Time-Dependent Testing with TestClock

**Critical**: Always fork the effect BEFORE advancing TestClock.

### Basic TestClock Usage

```typescript
import { it, expect } from "@effect/vitest"
import { Effect, TestClock, Fiber } from "effect"

it.effect("should handle delays", () =>
  Effect.gen(function* () {
    // Fork FIRST
    const fiber = yield* Effect.fork(
      Effect.sleep("5 seconds").pipe(Effect.as("done"))
    )

    // Then advance time
    yield* TestClock.adjust("5 seconds")

    const result = yield* Fiber.join(fiber)
    expect(result).toBe("done")
  })
)
```

### Testing Recurring Effects

```typescript
it.effect("should execute every minute", () =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<number>()

    yield* Effect.fork(
      Queue.offer(queue, 1).pipe(
        Effect.delay("60 seconds"),
        Effect.forever
      )
    )

    const empty = yield* Queue.poll(queue)
    expect(Option.isNone(empty)).toBe(true)

    yield* TestClock.adjust("60 seconds")

    const value = yield* Queue.take(queue)
    expect(value).toBe(1)
  })
)
```

## Error Testing

### Testing Expected Failures

Use `Effect.flip` to convert failures to successes:

```typescript
it.effect("should fail with error", () =>
  Effect.gen(function* () {
    const error = yield* Effect.flip(failingOperation())
    expect(error).toBeInstanceOf(UserNotFoundError)
    expect(error.userId).toBe("123")
  })
)
```

### Testing with Exit

Use `Effect.exit` to capture both success and failure:

```typescript
it.effect("should handle success", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(divide(4, 2))
    expect(exit).toEqual(Exit.succeed(2))
  })
)

it.effect("should handle failure", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(divide(4, 0))
    expect(exit).toEqual(Exit.fail("Cannot divide by zero"))
  })
)
```

## Property-Based Testing

### Using it.prop for Pure Properties

```typescript
import { FastCheck } from "effect"
import { it } from "@effect/vitest"

it.prop(
  "addition is commutative",
  [FastCheck.integer(), FastCheck.integer()],
  ([a, b]) => a + b === b + a
)
```

### Using it.effect.prop for Effect Properties

```typescript
it.effect.prop(
  "database operations are idempotent",
  [FastCheck.string(), FastCheck.integer()],
  ([key, value]) =>
    Effect.gen(function* () {
      const db = yield* Database

      yield* db.set(key, value)
      const result1 = yield* db.get(key)

      yield* db.set(key, value)
      const result2 = yield* db.get(key)

      return result1 === result2
    })
)
```

## Test Control

```typescript
// Skip
it.effect.skip("not ready yet", () => Effect.gen(function* () {}))
it.effect.skipIf(condition)("conditional skip", () => Effect.gen(function* () {}))

// Only run this test
it.effect.only("debug this test", () => Effect.gen(function* () {}))

// Run conditionally
it.effect.runIf(process.env.INTEGRATION_TESTS)("integration test", () =>
  Effect.gen(function* () {})
)
```

## Logging in Tests

```typescript
// Default: logs are suppressed
it.effect("logs are suppressed", () =>
  Effect.gen(function* () {
    yield* Effect.log("This won't appear")
  })
)

// Enable logs
it.effect("logs visible", () =>
  Effect.gen(function* () {
    yield* Effect.log("This will appear")
  }).pipe(Effect.provide(Logger.pretty))
)
```

## Common Pitfalls

### Don't Mix expect with assert

```typescript
// ❌ Wrong
// assert.strictEqual(result, expected)

// ✅ Correct
import { it, expect } from "@effect/vitest"
expect(result).toBe(expected)
```

### Always Fork Before TestClock.adjust

```typescript
// ❌ Wrong - will hang
it.effect("test", () =>
  Effect.gen(function* () {
    yield* Effect.sleep("5 seconds")  // Blocks!
    yield* TestClock.adjust("5 seconds")
  })
)

// ✅ Correct
it.effect("test", () =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(Effect.sleep("5 seconds"))
    yield* TestClock.adjust("5 seconds")
    yield* Fiber.join(fiber)
  })
)
```

### Provide Layers to Effect, Not the Test Function

```typescript
// ❌ Wrong - providing to wrong level
it.effect("test", () =>
  Effect.gen(function* () {
    const result = yield* someEffect
    expect(result).toBe(expected)
  })
)  // Can't provide here

// ✅ Correct
it.effect("test", () =>
  Effect.gen(function* () {
    const result = yield* someEffect
    expect(result).toBe(expected)
  }).pipe(Effect.provide(layer))  // ✅ Provide to Effect
)
```

## Complete Example

```typescript
import { describe, expect, it, layer } from "@effect/vitest"
import { Effect, Context, Layer } from "effect"

class Counter extends Context.Tag("Counter")<Counter, {
  increment: () => Effect.Effect<void>
  value: () => Effect.Effect<number>
}>() {
  static Live = Layer.effect(
    Counter,
    Effect.gen(function* () {
      let count = 0
      return {
        increment: () => Effect.sync(() => { count++ }),
        value: () => Effect.succeed(count)
      }
    })
  )
}

layer(Counter.Live)("Counter", (it) => {
  it.effect("should start at 0", () =>
    Effect.gen(function* () {
      const counter = yield* Counter
      const value = yield* counter.value()
      expect(value).toBe(0)
    })
  )

  it.effect("should increment", () =>
    Effect.gen(function* () {
      const counter = yield* Counter
      yield* counter.increment()
      const value = yield* counter.value()
      expect(value).toBe(1)
    })
  )
})
```

## Testing Checklist

- [ ] Correct framework chosen (`@effect/vitest` vs `vitest`)
- [ ] Test variant appropriate (`effect`/`live`/`scoped`/`scopedLive`)
- [ ] Services provided via layers when needed
- [ ] TestClock used for time-dependent operations (fork first!)
- [ ] Errors tested with `Effect.flip` or `Effect.exit`
- [ ] Edge cases covered
- [ ] Tests are deterministic (no real time/random unless `it.live`)
- [ ] Resources properly scoped and cleaned up
- [ ] All tests pass
