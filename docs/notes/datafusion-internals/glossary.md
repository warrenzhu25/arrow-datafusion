# Glossary

Cross-cutting terms used throughout *DataFusion Internals*, defined once. Chapters link
here on first use. Terms are grouped loosely by the layer they belong to; within a group
they read best top to bottom.

## Query representations

**RecordBatch.** Apache Arrow's columnar batch of rows — a set of same-length arrays,
one per column, sharing a schema. The unit of data that flows through execution.

**SendableRecordBatchStream.** An async, `Send` stream of `RecordBatch` results (plus an
associated schema) returned by `ExecutionPlan::execute`. Execution is pull-based: nothing
runs until something polls this stream. See [Chapter 1](./01-query-lifecycle.md).

## Logical layer

**Expr.** DataFusion's logical expression tree — columns, literals, binary operators,
casts, function calls, subqueries, and similar constructs. Defined in
`datafusion/expr/src/expr.rs`. See [Chapter 3](./03-logical-plans.md).

**LogicalPlan.** The relational operator tree (projection, filter, aggregate, join, sort,
scan, ...) that contains `Expr` values. Represents query *meaning*, independent of
partition count, join algorithm, or memory strategy. See [Chapter 3](./03-logical-plans.md).

**DFSchema.** An Arrow `Schema` extended with relation qualifiers (which table a column
came from) and logical field metadata. Needed to resolve ambiguous column names, e.g. in
self-joins. Lives in `datafusion/common/src/dfschema.rs`. See [Chapter 3](./03-logical-plans.md).

**TreeNode.** DataFusion's generic tree-traversal API, used to visit and rewrite `Expr`
and `LogicalPlan` trees without hard-coding every node shape. Optimizer rules build on
this. See [Chapter 4](./04-analyzer-optimizer.md).

**PlannerContext.** Cross-node state the SQL planner carries that no single AST node can
hold on its own: CTEs, outer-query schemas (for correlated subqueries), generated
aliases, and prepared-statement parameter types. See [Chapter 2](./02-sql-planning.md).

**ExprPlanner / TypePlanner.** Extension traits that let embedders hook custom SQL
expression or type-planning behavior into `SqlToRel` without replacing the whole SQL
planner. See [Chapter 2](./02-sql-planning.md).

**Extension (LogicalPlan).** The escape-hatch `LogicalPlan` variant for custom relational
operators that are not one of DataFusion's built-in node types. See
[Chapter 3](./03-logical-plans.md).

**Analyzer.** The rule pass that runs before logical optimization and makes plans *valid*
— inserting casts, resolving grouping constructs, normalizing function forms — as opposed
to the optimizer, which makes valid plans *cheaper*. See [Chapter 4](./04-analyzer-optimizer.md).

**OptimizerRule / Transformed.** The trait contract for a logical rewrite pass
(`OptimizerRule`) and the wrapper type (`Transformed<T>`) every rule returns to report
whether it actually changed the plan, so the optimizer driver can skip redundant work.
See [Chapter 4](./04-analyzer-optimizer.md).

## Physical layer

**PhysicalExpr.** The executable counterpart to `Expr` — evaluates against a real
`RecordBatch` and returns a `ColumnarValue`. Defined in
`datafusion/physical-expr-common/src/physical_expr.rs`. See
[Chapter 6](./06-physical-expressions.md).

**ColumnarValue.** An enum wrapping either a full Arrow array (one value per row) or a
single scalar broadcast across a batch — the result type of `PhysicalExpr::evaluate`.
Keeps expressions like `a > 5` from materializing an array full of `5`s. See
[Chapter 6](./06-physical-expressions.md).

**ExecutionPlan.** The executable physical operator trait — `FilterExec`,
`ProjectionExec`, `HashJoinExec`, and so on all implement it. Its `execute(partition,
context)` method returns a `SendableRecordBatchStream` for one output partition. See
[Chapter 5](./05-physical-planning.md).

**PlanProperties.** An `ExecutionPlan` node's cached contract with the rest of the tree:
output schema, partitioning, ordering (equivalence properties), emission type, and
boundedness. The physical optimizer reads these to decide whether a plan already
satisfies a requirement. See [Chapter 5](./05-physical-planning.md).

**Partitioning.** How an operator's output rows are distributed across parallel output
streams — hash, round-robin, or unknown/unspecified. Determines whether downstream
operators (joins, grouped aggregates) see the right rows on the right partition. See
[Chapter 7](./07-physical-optimizer.md).

**PhysicalOptimizerRule.** The trait contract for a rewrite pass over an already
*executable* `ExecutionPlan` tree — as opposed to `OptimizerRule`, which rewrites the
still-abstract `LogicalPlan`. See [Chapter 7](./07-physical-optimizer.md).

## Functions

**Accumulator.** Runtime state for one aggregate group. Folds input rows via
`update_batch`, merges partial state from other partitions via `merge_batch`, and
produces the final value via `evaluate`. See [Chapter 8](./08-aggregate-functions.md).

**GroupsAccumulator.** A vectorized accumulator that tracks state for *all* groups in a
grouped aggregation at once, addressed by group index, instead of one `Accumulator` per
group. Used by `GroupedHashAggregateStream` for performance. See
[Chapter 8](./08-aggregate-functions.md).

**PartitionEvaluator.** The runtime evaluator for a window function over one partition of
rows. Exposes capability flags — whether it is causal, whether it uses the window frame,
whether it supports bounded/streaming execution — that determine how much of the
partition must be buffered before it can produce output. See
[Chapter 9](./09-window-functions.md).

**Window frame.** The sliding sub-range (`ROWS`, `RANGE`, or `GROUPS` units, with bounds
like `UNBOUNDED PRECEDING` or `1 FOLLOWING`) that a window function evaluates over for
each output row. See [Chapter 9](./09-window-functions.md).

## Data sources and catalog

**TableProvider.** The central table abstraction. Its `scan` method turns a projection,
filters, and a limit into an `ExecutionPlan`; its filter-pushdown method tells the
optimizer which predicates the provider can handle itself. See
[Chapter 10](./10-data-sources.md).

**TableSource.** The narrow, catalog-independent view of a table carried inside
`LogicalPlan::TableScan` — enough for logical planning without depending on the full
`TableProvider` scan machinery. See [Chapter 11](./11-catalog-system.md).

**FileFormat.** The abstraction for format-specific behavior (Parquet, CSV, JSON, Avro,
...): schema inference, statistics inference, and physical plan creation for a set of
files. See [Chapter 10](./10-data-sources.md).

**Filter pushdown (Exact / Inexact / Unsupported).** The three-way contract a
`TableProvider` uses to tell DataFusion whether a predicate it pushed down still needs to
be re-evaluated above the scan. *Exact* means the source fully applied it; *Inexact*
means the source narrowed the data but the filter must still run again; *Unsupported*
means DataFusion must apply it in full. See [Chapter 10](./10-data-sources.md).

**CatalogProvider / SchemaProvider.** The trait pair forming the catalog → schema → table
resolution hierarchy that sits above `TableProvider`. A `CatalogProvider` holds named
schemas; a `SchemaProvider` holds named tables. See [Chapter 11](./11-catalog-system.md).

## Runtime and session

**MemoryPool / MemoryReservation / MemoryConsumer.** The accounting trio operators use to
participate in a shared memory budget: a consumer registers, requests growth via
`try_grow` on a reservation, and the pool enforces the limit (and can trigger spilling)
across all registered consumers. See [Chapter 12](./12-execution-runtime.md).

**RuntimeEnv.** The process- or session-scoped bundle of shared execution resources: the
`MemoryPool`, `DiskManager` (for spill files), `CacheManager`, and object store registry.
See [Chapter 12](./12-execution-runtime.md).

**TaskContext.** The per-query-execution context passed into `ExecutionPlan::execute`. It
carries a reference to the session's `RuntimeEnv` plus query-specific configuration and
function registries, so operators can access memory pools and disk managers without a
back-reference to the whole session. See [Chapter 1](./01-query-lifecycle.md) and
[Chapter 12](./12-execution-runtime.md).

**SessionContext / SessionState.** `SessionContext` is the thin, user-facing handle
applications hold (`SessionContext::sql`, `register_table`, ...). `SessionState` is the
`RwLock`-guarded struct it wraps, holding the analyzer, optimizers, catalog list,
function registries, configuration, and `RuntimeEnv` — the actual query-planning state.
See [Chapter 13](./13-session-dataframe-api.md).
