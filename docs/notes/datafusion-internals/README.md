# DataFusion Internals

*How a modern, extensible, Arrow-native query engine works on the inside.*

---

This is a book about the **inside** of [Apache DataFusion](https://github.com/apache/datafusion) —
the crates, traits, data structures, and rewrite passes that turn a SQL string or a
DataFrame call into a stream of Arrow `RecordBatch` values.

It is written so that you can understand how DataFusion works **without reading the
source code end to end**. Each chapter builds intuition from first principles, traces a
concrete query through the subsystem it covers, and is explicit about the contracts each
layer must uphold and the trade-offs behind its design.

## Who this book is for

- **Contributors** who want a precise mental model of where behavior lives, how control
  moves between crates, and which interfaces are stable extension points before sending
  a PR.
- **Embedders** — anyone building a database, a data pipeline, or an analytics tool on
  top of DataFusion who needs to know where to plug in a custom table, function, or
  optimizer rule without forking the engine.
- **Systems engineers** curious how a columnar, Arrow-native query engine is actually
  built: logical/physical plan separation, vectorized expression evaluation, spillable
  hash aggregation, and pull-based streaming execution.

## Prerequisites

You will get the most out of this book if you are comfortable with:

- Basic SQL (SELECT, WHERE, GROUP BY, JOIN, window functions).
- General Rust: traits, `Arc`, iterators, and `async`/`await` at a conceptual level.
- The shape of Apache Arrow's columnar format (arrays, schemas, record batches) helps
  but is not required — [Chapter 1](./01-query-lifecycle.md) introduces what you need.

Terms in **bold** on first use are defined in the [glossary](./glossary.md).

## How to read it

Read [Chapter 1](./01-query-lifecycle.md) first — every later chapter assumes the
logical/physical plan split and the pipeline stages it establishes. After that, chapters
are largely self-contained; follow the parts in order, or jump to the subsystem you care
about.

Each chapter follows the same shape: *why it exists* → *the big picture* → *core
concepts* → *how it works* → *a worked example* → *invariants & edge cases* →
*trade-offs* → *key takeaways*.

## Table of contents

### Part I — Foundations

1. [The Query Lifecycle](./01-query-lifecycle.md) — the five representations a query
   passes through, and the logical/physical seam that matters most.

### Part II — Logical Planning

2. [SQL Parsing and Planning](./02-sql-planning.md) — turning a sqlparser AST into
   `LogicalPlan` and `Expr` trees.
3. [Logical Expressions and Logical Plans](./03-logical-plans.md) — DataFusion's
   semantic model of a query, independent of execution strategy.
4. [Analyzer and Logical Optimizer](./04-analyzer-optimizer.md) — making plans valid,
   then making them cheap, without changing what they mean.

### Part III — Physical Planning & Execution

5. [Physical Planning and Execution Plans](./05-physical-planning.md) — lowering
   `LogicalPlan` into the executable `ExecutionPlan` tree.
6. [Physical Expressions](./06-physical-expressions.md) — evaluating expressions
   against real Arrow arrays.
7. [Physical Optimizer](./07-physical-optimizer.md) — enforcing distribution and
   ordering requirements, choosing join strategies.

### Part IV — Functions

8. [Aggregate Functions](./08-aggregate-functions.md) — accumulators, partial/final
   aggregation, and grouped hash aggregation.
9. [Window Functions](./09-window-functions.md) — partition evaluators, frames, and
   streaming versus whole-partition evaluation.

### Part V — Data & Metadata

10. [Data Sources](./10-data-sources.md) — `TableProvider`, `FileFormat`, filter
    pushdown, and Parquet pruning.
11. [Catalog System](./11-catalog-system.md) — resolving SQL names into table
    providers through catalogs and schemas.

### Part VI — Runtime & APIs

12. [Execution Runtime](./12-execution-runtime.md) — memory pools, spilling, disk
    management, and object stores.
13. [Session Management and DataFrame API](./13-session-dataframe-api.md) — the
    user-facing orchestration layer that ties every other chapter together.

### Reference

- [Glossary](./glossary.md) — every cross-cutting term, defined once.
- [Appendix: Crate Map, Extension Points, and Learning Paths](./appendix-extension-points.md)
  — a terse, code-anchored index for readers who want to go straight from a concept to
  the source.

---

> **On accuracy and drift.** This book describes DataFusion's design and the reasoning
> behind it. Where it names a concrete file, struct, or trait, that detail was checked
> against the source at the time of writing but may drift as the code evolves. The
> [appendix](./appendix-extension-points.md) is the bridge to the current source.
