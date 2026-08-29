# Chapter 13 — Session Management and DataFrame API

## Why this exists

Every chapter so far describes a piece of machinery: a planner, an
optimizer, a runtime. None of that machinery assembles itself. Something has
to own the analyzer, the optimizer, the physical planner, the catalog list,
the function registries, and the `RuntimeEnv`
([Chapter 12](./12-execution-runtime.md)), and hand out an API a user can
actually call. That something is `SessionContext` and `SessionState` — the
user-facing orchestration layer this whole book has been building toward.

## The big picture

```rust
// datafusion/core/src/execution/context/mod.rs
pub struct SessionContext {
    session_id: String,
    session_start_time: DateTime<Utc>,
    state: Arc<RwLock<SessionState>>,
}
```

`SessionContext` is deliberately thin — almost everything it does is a
method that acquires a read or write lock on `state` and delegates.
`SessionState` is where the real weight lives:

```rust
// datafusion/core/src/execution/session_state.rs
pub struct SessionState {
    session_id: String,
    analyzer: Analyzer,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    optimizer: Optimizer,
    physical_optimizers: PhysicalOptimizer,
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    catalog_list: Arc<dyn CatalogProviderList>,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    file_formats: HashMap<String, Arc<dyn FileFormatFactory>>,
    config: SessionConfig,
    table_options: TableOptions,
    execution_props: ExecutionProps,
    runtime_env: Arc<RuntimeEnv>,
    // ...additional registries for table functions, extension types,
    // serialization, and pluggable statistics as the engine has grown.
}
```

Every other chapter's central type is reachable from `SessionState`: the
`Analyzer` and `Optimizer` from [Chapter 4](./04-analyzer-optimizer.md), the
`PhysicalOptimizer` from [Chapter 7](./07-physical-optimizer.md), the
`catalog_list` from [Chapter 11](./11-catalog-system.md), the function
registries from Chapters [8](./08-aggregate-functions.md) and
[9](./09-window-functions.md), and the `RuntimeEnv` from
[Chapter 12](./12-execution-runtime.md). `SessionContext` is the handle a
user holds; `SessionState` is the graph of everything that handle can reach.

## Core concepts

### `SessionContext`: the user-facing entry point

```rust
impl SessionContext {
    pub fn new() -> Self;
    pub fn new_with_config(config: SessionConfig) -> Self;
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.sql_with_options(sql, SQLOptions::new()).await
    }
    pub fn register_table(
        &self,
        table_ref: impl Into<TableReference>,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
    pub fn register_udf(&self, f: ScalarUDF);
    pub fn register_udaf(&self, f: AggregateUDF);
    pub fn register_udwf(&self, f: WindowUDF);
}
```

`sql_with_options` is the more general form `sql` delegates to — it accepts
`SQLOptions`, which can forbid DDL, DML, or statements that mutate the
session, useful when SQL text comes from a less-trusted caller. Everything
that registers something (`register_table`, `register_udf`, and friends)
follows the same pattern: acquire a write lock on `SessionState`, mutate the
relevant registry, return whatever was previously registered under that
name.

### `SessionState` as query-planning state

Because `SessionState` holds the analyzer, optimizer, physical optimizer,
catalog list, and function registries together, a `LogicalPlanBuilder`
([Chapter 3](./03-logical-plans.md)) or the SQL planner
([Chapter 2](./02-sql-planning.md)) only needs a `&SessionState` to resolve
names, plan expressions, and eventually drive the full pipeline from
[Chapter 1](./01-query-lifecycle.md). `SessionState` is built and modified
through `SessionStateBuilder`, a builder-style API — this makes it practical
to create a *derived* state (same catalog list and runtime, plus one extra
`OptimizerRule` or `ScalarUDF`) without mutating a state other code might
still be relying on.

### `DataFrame`: a lazy plan builder

```rust
// datafusion/core/src/dataframe/mod.rs
pub struct DataFrame {
    session_state: Box<SessionState>,
    plan: LogicalPlan,
}
```

A `DataFrame` is a `LogicalPlan` plus the `SessionState` needed to optimize
and execute it. Its methods (`select`, `filter`, `aggregate`, `sort`, `join`,
and so on) each call into `LogicalPlanBuilder` to produce a *new*
`LogicalPlan`, and return a new `DataFrame` wrapping it — no I/O, no
optimization, no execution happens until a terminal method is called
(`collect`, `execute_stream`, `show`, `write_parquet`, and similar). SQL and
the DataFrame API are two builders for the same `LogicalPlan`; a query
built through `SessionContext::sql` and an equivalent query built through
`DataFrame` methods converge on the identical analyzer, optimizer, physical
planner, and physical optimizer once a terminal method runs.

## How it works: `SessionContext::sql` end to end

1. `SessionContext::sql(sql_text)` calls `sql_with_options` with default
   (permissive) `SQLOptions`.
2. Under a read lock on `SessionState`, sqlparser-rs parses `sql_text` into
   an AST, and `SqlToRel` (constructed from `&SessionState` acting as a
   `ContextProvider`) converts it into a `LogicalPlan`
   ([Chapter 2](./02-sql-planning.md)).
3. The resulting `LogicalPlan` is wrapped into a `DataFrame` alongside a
   clone of the `SessionState` at that moment — a `DataFrame` carries its
   own snapshot of state, so later mutations to the `SessionContext` (a new
   registered function, say) do not retroactively change how an
   already-built `DataFrame` will plan.
4. Nothing has executed yet. The caller can keep transforming the
   `DataFrame` (`.filter(...)`, `.select(...)`) exactly as if it had been
   built without SQL at all.
5. When the caller calls `.collect().await`, the `DataFrame` runs its stored
   `LogicalPlan` through the analyzer and optimizer
   ([Chapter 4](./04-analyzer-optimizer.md)), the physical planner
   ([Chapter 5](./05-physical-planning.md)), and the physical optimizer
   ([Chapter 7](./07-physical-optimizer.md)), then executes the resulting
   `ExecutionPlan` and gathers every `RecordBatch` from every partition into
   a `Vec<RecordBatch>`.

## A worked example: SQL and DataFrame converge

```rust
let ctx = SessionContext::new();
ctx.register_table("t", table_provider)?;

// Path A: SQL
let df_a = ctx.sql("SELECT c1, SUM(c2) FROM t WHERE c1 > 10 GROUP BY c1").await?;

// Path B: DataFrame API
let df_b = ctx.table("t").await?
    .filter(col("c1").gt(lit(10)))?
    .aggregate(vec![col("c1")], vec![sum(col("c2"))])?;
```

`df_a` and `df_b` are built through entirely different code paths — one
through sqlparser and `SqlToRel`, the other through direct
`LogicalPlanBuilder` calls invoked by `DataFrame` methods — but they produce
structurally equivalent `LogicalPlan` trees (a `TableScan`, wrapped in a
`Filter`, wrapped in an `Aggregate`) referencing the same registered table.
Calling `.collect().await` on either one drives it through the identical
analyzer → optimizer → physical planner → physical optimizer → execute
pipeline from [Chapter 1](./01-query-lifecycle.md). This convergence is
deliberate: it means every optimizer rule, every physical operator choice,
and every extension point in this book applies equally regardless of which
API a query started from.

## Invariants and edge cases

- **Building a `DataFrame` never touches data.** Every builder method
  (`select`, `filter`, `join`, ...) only manipulates the in-memory
  `LogicalPlan`; the first I/O happens inside a terminal method's call into
  physical planning and execution.
- **A `DataFrame` snapshots `SessionState` at creation, not at execution.**
  Registering a new UDF on the `SessionContext` after a `DataFrame` was
  built does not make that UDF visible to a plan already under construction
  on that `DataFrame`.
- **`SessionContext` methods that mutate `SessionState` take a write lock;
  planning and execution take a read lock.** Long-running queries do not
  block registering a new table on the same context, but two concurrent
  registrations do serialize against each other.
- **`SQLOptions` restrictions are enforced at parse/plan time, not at
  execution time.** A forbidden DDL statement fails before a `LogicalPlan`
  is even built, not partway through execution.

## Trade-offs

Keeping `SessionContext` thin and pushing essentially all state into
`SessionState` behind a single `RwLock` costs a lock acquisition on every
planning call, and means a very large number of concurrent short queries on
one context will contend on that lock during planning (though not during
execution, once a `TaskContext` has been handed off). In exchange, it gives
a strong, simple consistency story — a single point where "what does this
session currently know about tables, functions, and configuration" is
answered — and it makes `SessionState` cheaply forkable via its builder,
which is what lets tools construct a derived session (extra rule, extra
function) without deep-copying the catalog list or runtime environment.

## Key takeaways

- `SessionContext` is a thin, `Clone`-friendly handle; `SessionState` behind
  its `RwLock` is where the analyzer, optimizers, catalog list, function
  registries, and `RuntimeEnv` actually live.
- SQL and the DataFrame API are two front-ends over the same
  `LogicalPlanBuilder`-driven plan construction; both converge on identical
  planning and execution once a terminal method runs.
- `DataFrame` execution is lazy: nothing runs until `collect`,
  `execute_stream`, `show`, or a similar terminal call.
- `SessionStateBuilder` supports deriving a modified session (extra rules,
  extra functions) without disturbing a state other code depends on.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 11 — Catalog System](./11-catalog-system.md)
- [Chapter 12 — Execution Runtime](./12-execution-runtime.md)
- [Appendix: Extension Points & Learning Paths](./appendix-extension-points.md)
