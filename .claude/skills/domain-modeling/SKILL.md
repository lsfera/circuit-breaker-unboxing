---
name: domain-modeling
description: Create production-ready Effect domain models using Schema.TaggedStruct for ADTs, Schema.Data for automatic equality, with comprehensive predicates, orders, guards, and match functions. Use when modeling domain entities, value objects, or any discriminated union types.
---

# Effect Domain Modeling Skill

Use this skill when creating domain models, entities, value objects, or any types that represent core business concepts.

## Core Pattern: Schema.TaggedStruct + Schema.Data

The foundation combines three key features:

1. **Schema.TaggedStruct** — Automatic `_tag` discriminator for union types
2. **Schema.Data** — Automatic `Equal` implementation for structural equality
3. **Schema.decodeSync** — Type-safe constructors with validation

```typescript
import { Schema, Equal } from "effect"

export const Pending = Schema.TaggedStruct("pending", {
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromSelf,
}).pipe(
  Schema.Data,
  Schema.annotations({
    identifier: "Pending",
    title: "Pending Task",
    description: "A task that has been created but not yet started",
  })
)

export const Active = Schema.TaggedStruct("active", {
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromSelf,
  startedAt: Schema.DateTimeUtcFromSelf,
}).pipe(Schema.Data, Schema.annotations({ identifier: "Active", title: "Active Task", description: "Currently being worked on" }))

export const Completed = Schema.TaggedStruct("completed", {
  id: Schema.String,
  createdAt: Schema.DateTimeUtcFromSelf,
  completedAt: Schema.DateTimeUtcFromSelf,
}).pipe(Schema.Data, Schema.annotations({ identifier: "Completed", title: "Completed Task", description: "A task that has been finished" }))

export const Task = Schema.Union(Pending, Active, Completed).pipe(
  Schema.annotations({ identifier: "Task", title: "Task", description: "A task can be pending, active, or completed" })
)

export type Task = Schema.Schema.Type<typeof Task>
export type Pending = Schema.Schema.Type<typeof Pending>
export type Active = Schema.Schema.Type<typeof Active>
export type Completed = Schema.Schema.Type<typeof Completed>
```

## Mandatory Module Exports

Every domain model module MUST include:

### 1. Constructors Using Schema.decodeSync

```typescript
/**
 * Create a pending task. _tag is automatically applied by TaggedStruct.
 *
 * @category Constructors
 * @since 0.1.0
 * @example
 * import * as Task from "@/schemas/Task"
 * import * as DateTime from "effect/DateTime"
 *
 * const task = Task.makePending({ id: "task-123", createdAt: DateTime.unsafeNow() })
 */
export const makePending = Schema.decodeSync(Pending)
export const makeActive = Schema.decodeSync(Active)
export const makeCompleted = Schema.decodeSync(Completed)
```

> Why `decodeSync`? `Schema.Data` returns a schema that needs decoding. `decodeSync` creates a validated constructor that automatically applies the `_tag` discriminator.

### 2. Guards and Type Predicates

```typescript
/** Type guard for Task union. @category Guards */
export const isTask = Schema.is(Task)

/** Refine to Pending variant. @category Guards */
export const isPending = (self: Task): self is Pending => self._tag === "pending"

/** Refine to Active variant. @category Guards */
export const isActive = (self: Task): self is Active => self._tag === "active"

/** Refine to Completed variant. @category Guards */
export const isCompleted = (self: Task): self is Completed => self._tag === "completed"
```

### 3. Match Function (Pattern Matching)

```typescript
import * as Match from "effect/Match"

/**
 * Pattern match on Task using Match.typeTags.
 *
 * @category Pattern Matching
 * @example
 * const status = Task.match({
 *   pending: (t) => `Pending: ${t.id}`,
 *   active: (t) => `Active since ${t.startedAt}`,
 *   completed: (t) => `Completed at ${t.completedAt}`
 * })
 * const result = status(task)
 */
export const match = Match.typeTags<Task>()
```

### 4. Equivalence

```typescript
import * as Equivalence from "effect/Equivalence"

// Schema.Data provides structural Equal.equals() automatically.
// Only export custom equivalence when semantically meaningful:
export const EquivalenceById = Equivalence.mapInput(
  Equivalence.string,
  (task: Task) => task.id
)
```

## Conditional Module Exports

Include when semantically appropriate:

### Order Instances

```typescript
import * as Order from "effect/Order"
import * as DateTime from "effect/DateTime"

/** @category Orders */
export const OrderByTag: Order.Order<Task> = Order.mapInput(
  Order.number,
  (task) => ({ pending: 0, active: 1, completed: 2 }[task._tag])
)

/** @category Orders */
export const OrderByCreatedAt: Order.Order<Task> =
  Order.mapInput(DateTime.Order, (task) => task.createdAt)

/** Multi-criteria: sort by tag first, then date */
export const OrderByTagThenDate: Order.Order<Task> = Order.combine(
  OrderByTag,
  OrderByCreatedAt
)
```

### Destructors and Setters

```typescript
import { dual } from "effect/Function"

/** @category Destructors */
export const getId = (self: Task): string => self.id

/** @category Setters */
export const setId: {
  (id: string): (self: Task) => Task
  (self: Task, id: string): Task
} = dual(2, (self: Task, id: string): Task => ({ ...self, id }))
```

## Advanced Patterns

### Recursive Schemas with Schema.suspend

```typescript
const baseFields = { id: Schema.String, name: Schema.String }

interface Category extends Schema.Struct.Type<typeof baseFields> {
  readonly subcategories: ReadonlyArray<Category>
}

export const Category = Schema.Struct({
  ...baseFields,
  subcategories: Schema.Array(
    Schema.suspend((): Schema.Schema<Category> => Category)
  ),
}).pipe(Schema.Data, Schema.annotations({ identifier: "Category" }))

export type Category = Schema.Schema.Type<typeof Category>
```

### Branded Types

```typescript
import * as Brand from "effect/Brand"

export type Email = Brand.Branded<string, "Email">

export const Email = Brand.refined<Email>(
  (s) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s),
  (s) => Brand.error(`"${s}" is not a valid email`)
)

export const EmailSchema: Schema.BrandSchema<Email, string> =
  Schema.String.pipe(Schema.fromBrand(Email))
```

## Import Pattern

**CRITICAL**: Always use namespace imports:

```typescript
// ✅ CORRECT
import * as Task from "@/schemas/Task"
import * as DateTime from "effect/DateTime"

const task = Task.makePending({ id: "123", createdAt: DateTime.unsafeNow() })
const isPending = Task.isPending(task)

// ❌ WRONG - loses context, causes name clashes
import { makePending, isPending } from "@/schemas/Task"
```

## Temporal Data

Always use DateTime and Duration, never Date or number:

```typescript
// ✅ CORRECT
export const Task = Schema.TaggedStruct("task", {
  createdAt: Schema.DateTimeUtcFromSelf,  // UTC datetime
  duration: Schema.Duration,               // Duration type
}).pipe(Schema.Data)

// ❌ WRONG
export const TaskBad = Schema.TaggedStruct("task", {
  createdAt: Schema.Date,    // Native Date
  duration: Schema.Number,   // Number milliseconds
})
```

## Quality Checklist

### Mandatory

- [ ] Each variant uses `Schema.TaggedStruct`
- [ ] `.pipe(Schema.Data)` for automatic `Equal` implementation
- [ ] Schema annotations on all schemas (identifier, title, description)
- [ ] Constructors using `Schema.decodeSync`
- [ ] Type guard using `Schema.is` for union
- [ ] Refinement predicates for each variant (`isPending`, etc.)
- [ ] Match function using `Match.typeTags`
- [ ] All exports use namespace pattern (`import * as`)
- [ ] Full JSDoc with @category, @since, @example
- [ ] DateTime/Duration for temporal data (not Date/number)

### Conditional

- [ ] Identity values (`zero`, `empty`) when type has natural "empty" value
- [ ] Order instances using `Order.mapInput` for common sorting
- [ ] `Order.combine` for multi-criteria sorting
- [ ] Destructors/setters when frequently accessed
- [ ] `Schema.suspend` for recursive/self-referencing types
- [ ] Branded types for validation constraints

## Key Principles

1. `Schema.TaggedStruct` — for all tagged union variants
2. `Schema.Data` — automatic Equal implementation
3. `Schema.decodeSync` — type-safe constructors
4. `Schema.annotations` — document all schemas
5. `Order.mapInput` — compose orders from base orders
6. `Match.typeTags` — pattern match on discriminated unions
7. Namespace imports — always `import * as`
8. `DateTime`/`Duration` — never `Date`/`number` for temporal data
