# Appendix — Crate Map, Extension Points, and Learning Paths

Companion to every chapter in this book. Where a chapter explains *what a
subsystem does and why*, this appendix is the terse, code-anchored index for
readers who already know what they want and just need the crate, trait, or
file to start from.

> **On accuracy and drift.** File paths and traits below were checked against
> the source at the time of writing but may drift as the code evolves. Treat
> this appendix as a starting point, not ground truth — confirm against the
> current source, or against `docs/source/contributor-guide/architecture.md`.

## Crate responsibilities

Foundational dependencies — Arrow, sqlparser-rs, Tokio, and `object_store` —
provide columnar memory, SQL parsing, async execution, and storage access.
They are external building blocks rather than DataFusion-specific query
logic.

`datafusion-common`, `datafusion-expr-common`, and
`datafusion-physical-expr-common` provide shared errors, tree traversal,
operators, scalar values, statistics, sort properties, accumulator traits,
and physical expression helpers. These crates avoid dependency cycles by
holding types needed by multiple higher-level crates.

`datafusion-expr` owns logical query representation. SQL planning, DataFrame
planning, analyzers, optimizers, and physical planners all depend on its
`Expr` and `LogicalPlan` definitions.

`datafusion-sql` owns SQL-specific translation. It depends on sqlparser-rs
and `datafusion-expr`, but it does not execute queries itself.

`datafusion-optimizer` owns logical analysis and optimization. It rewrites
`LogicalPlan` trees and depends on expression semantics, schemas, and
optimizer configuration.

`datafusion-physical-expr` owns executable expression trees. It is the
bridge between logical expressions and Arrow array evaluation.

`datafusion-physical-plan` owns executable operators and streams. It defines
`ExecutionPlan` and implements the operators used during query execution.

`datafusion-physical-optimizer` owns rewrites over executable plans. It
enforces physical requirements and improves operator choices after physical
planning.

`datafusion-datasource` and format-specific datasource crates own scan
abstractions, file grouping, format logic, pruning, and schema adaptation.

`datafusion-catalog` owns metadata traits and catalog implementations.
Planning uses it to resolve table names into table providers.

`datafusion-execution` owns runtime resources. It does not define query
plans; it provides the environment those plans use while running.

`datafusion-session` and `datafusion-core` connect the layers into
user-facing APIs. `datafusion-core` provides `SessionContext`, DataFrame
APIs, table registration helpers, and the default physical planner.

Function crates such as `datafusion-functions`,
`datafusion-functions-aggregate`, `datafusion-functions-window`, and
`datafusion-functions-nested` register built-in function implementations
while using the common UDF traits from `datafusion-expr`.

## Extension points

| Extension type | Main trait or API | Where it plugs in | Chapter |
|---|---|---|---|
| Custom table | `TableProvider` | `SessionContext::register_table` or a custom catalog | [10](./10-data-sources.md) |
| Custom file format | `FileFormat` and scan source types | Listing/table configuration and session state | [10](./10-data-sources.md) |
| Custom catalog | `CatalogProvider` and `SchemaProvider` | Session catalog APIs | [11](./11-catalog-system.md) |
| Scalar function | `ScalarUDF` and `ScalarUDFImpl` | `SessionContext::register_udf` | [13](./13-session-dataframe-api.md) |
| Aggregate function | `AggregateUDF` and `AggregateUDFImpl` | `SessionContext::register_udaf` | [8](./08-aggregate-functions.md) |
| Window function | `WindowUDF` and `WindowUDFImpl` | `SessionContext::register_udwf` | [9](./09-window-functions.md) |
| SQL expression planning | `ExprPlanner` | Planner extension configuration | [2](./02-sql-planning.md) |
| Logical optimizer rule | `OptimizerRule` | Session state optimizer configuration | [4](./04-analyzer-optimizer.md) |
| Physical optimizer rule | `PhysicalOptimizerRule` | Session state builder or custom optimizer setup | [7](./07-physical-optimizer.md) |
| Physical expression | `PhysicalExpr` | A physical expression planner or custom physical operator | [6](./06-physical-expressions.md) |
| Physical operator | `ExecutionPlan` | A custom physical planner or table provider scan | [5](./05-physical-planning.md) |
| Runtime resource policy | `RuntimeEnvBuilder`, `MemoryPool` | The runtime environment used by sessions | [12](./12-execution-runtime.md) |

## Learning paths

### Beginner: query construction and results

Start with `datafusion/core/src/execution/context/mod.rs`,
`datafusion/core/src/dataframe/mod.rs`, and the examples in
`datafusion-examples/`. Learn how SQL and DataFrame calls create lazy plans
and how `collect` or `execute_stream` triggers execution ([Chapter 13](./13-session-dataframe-api.md)).

Then read `datafusion/expr/src/logical_plan/plan.rs` and
`datafusion/expr/src/expr.rs`. These files define the structures that most
of the rest of the engine transforms ([Chapter 3](./03-logical-plans.md)).

### Intermediate: planning and optimization

Read `datafusion/sql/src/select.rs` and `datafusion/sql/src/expr/mod.rs` to
understand how SQL becomes logical plans ([Chapter 2](./02-sql-planning.md)).
Then read `datafusion/optimizer/src/optimizer.rs` and a small optimizer rule
such as `eliminate_filter.rs` before moving to larger rules such as
`push_down_filter.rs` or `decorrelate_predicate_subquery.rs`
([Chapter 4](./04-analyzer-optimizer.md)).

After that, read `datafusion/core/src/physical_planner.rs` and
`datafusion/physical-plan/src/execution_plan.rs`. These files explain how
logical plans become executable operators ([Chapter 5](./05-physical-planning.md)).

### Advanced: execution, sources, and extensibility

Read representative physical operators such as
`datafusion/physical-plan/src/filter.rs`,
`datafusion/physical-plan/src/projection.rs`, and
`datafusion/physical-plan/src/aggregates/mod.rs`. Pair those with
`datafusion/physical-expr/src/planner.rs` and
`datafusion/physical-expr/src/physical_expr.rs` to understand runtime
expression evaluation ([Chapter 6](./06-physical-expressions.md)).

For data source work, start with `datafusion/catalog/src/table.rs`,
`datafusion/datasource/src/file_format.rs`, and
`datafusion/core/src/datasource/listing/table.rs` ([Chapter 10](./10-data-sources.md)).
For runtime work, read `datafusion/execution/src/runtime_env.rs` and the
memory pool implementations ([Chapter 12](./12-execution-runtime.md)).

## Additional resources

- API documentation: https://docs.rs/datafusion
- Project website: https://datafusion.apache.org
- Contributor architecture guide: `docs/source/contributor-guide/architecture.md`
- Optimizer rule reference: `datafusion/core/src/optimizer_rule_reference.md`
- Examples: `datafusion-examples/`

This book is a contributor-oriented map of the codebase. For exact behavior,
the source files referenced throughout are the authoritative reference.
