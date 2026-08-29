# Chapter 1 — The Query Lifecycle

## Why this exists

Every later chapter in this book zooms into one stage of DataFusion's query
pipeline — the SQL planner, the optimizer, a physical operator, a data source.
None of that zooming makes sense without a map of the whole trip first: what
representation a query is in at each point, which crate owns that
representation, and which traits are the seams where the engine can be
extended or replaced. This chapter is that map. Read it first — every later
chapter assumes the stage names and data structures it introduces.

## The big picture

DataFusion turns a SQL string or a chain of DataFrame calls into a stream of
Arrow `RecordBatch` values. It does this in five representations, each owned
by a different layer of the codebase:

```
SQL text / DataFrame calls
        │
        ▼
   sqlparser AST            (external crate: sqlparser-rs)
        │
        ▼
   LogicalPlan + Expr        ("what the query means")
        │  (Analyzer, then Optimizer)
        ▼
   optimized LogicalPlan
        │  (physical planner)
        ▼
   ExecutionPlan tree        ("how to compute it")
        │  (Physical optimizer)
        ▼
   optimized ExecutionPlan
        │  (execute(partition, context))
        ▼
   SendableRecordBatchStream → Arrow RecordBatch values
```

The move from `LogicalPlan` to `ExecutionPlan` is the most important seam in
the whole engine. Everything above it — SQL syntax, name resolution, the
DataFrame builder API, logical rewrites — describes query *meaning* without
committing to an algorithm. Everything below it — which join algorithm,
how many partitions, whether to sort — describes *how* to compute that
meaning on real Arrow arrays, on real hardware, with a real memory budget.
Confusing the two is the single most common way to misunderstand the
codebase: a logical `Join` node says "these two relations are joined on this
key"; a physical `HashJoinExec` says "build a hash table from the smaller
side on this thread, probe it from the other side, spill to disk if you run
out of memory."

## Core concepts

### The logical layer: `Expr` and `LogicalPlan`

`Expr` (`datafusion/expr/src/expr.rs`) is DataFusion's expression tree —
columns, literals, binary operators, casts, scalar/aggregate/window function
calls, subqueries. `LogicalPlan` (`datafusion/expr/src/logical_plan/plan.rs`)
is the relational operator tree that contains those expressions —
projection, filter, aggregate, join, sort, table scan, and so on. Together
they are DataFusion's semantic model of a query: independent of partition
count, join algorithm, or memory strategy. [Chapter 3](./03-logical-plans.md)
covers this layer in depth.

### The physical layer: `PhysicalExpr` and `ExecutionPlan`

`PhysicalExpr` (`datafusion/physical-expr-common/src/physical_expr.rs`)
evaluates against real Arrow `RecordBatch` values and returns a
`ColumnarValue` — either a full array or a single scalar broadcast across
the batch. `ExecutionPlan` (`datafusion/physical-plan/src/execution_plan.rs`)
is the executable operator tree: each node reports its output partitioning,
ordering, and schema through `PlanProperties`, and its `execute(partition,
context)` method returns a `SendableRecordBatchStream` for one output
partition. [Chapter 5](./05-physical-planning.md) and
[Chapter 6](./06-physical-expressions.md) cover these in depth.

### Crates as extension seams

DataFusion is split into many crates specifically so that the parts an
embedder is likely to extend — table providers, functions, optimizer rules —
do not force a dependency on parts they are not touching. The boundaries
that matter most are traits, not modules:

| Trait | Owns | Defined in |
|---|---|---|
| `TableProvider` | how a table turns into a scan plan | `datafusion-catalog` |
| `FileFormat` / `FileSource` | format-specific scan planning | `datafusion-datasource` |
| `OptimizerRule` | logical plan rewrites | `datafusion-optimizer` |
| `PhysicalOptimizerRule` | physical plan rewrites | `datafusion-physical-optimizer` |
| `PhysicalExpr` | executable expression evaluation | `datafusion-physical-expr` |
| `ExecutionPlan` | physical operator behavior | `datafusion-physical-plan` |

A project that wants a custom table source implements `TableProvider` and
never needs to know how the optimizer works. A project that wants a new
optimization implements `OptimizerRule` and never needs to know how Parquet
pruning works. This is the same reason a shared-nothing key-value store
splits "shard owns its keyspace" from "proactor owns I/O" — narrow, stable
interfaces let subsystems evolve independently, and let contributors read
one layer without reading all of them.

## How it works: from SQL text to Arrow batches

1. **Entry.** `SessionContext::sql` (`datafusion/core/src/execution/context/mod.rs`)
   is the entry point for SQL; the DataFrame API builds `LogicalPlan` nodes
   directly through `datafusion-expr`, bypassing SQL parsing entirely.
2. **Parse.** sqlparser-rs turns SQL text into a statement AST. DataFusion's
   parser options and statement handling live in `datafusion/sql/src`.
3. **Plan.** `SqlToRel` (`datafusion/sql/src/planner.rs`) converts the AST
   into `LogicalPlan` and `Expr` values — resolving table names through the
   catalog, converting SQL expressions to logical expressions, and turning
   SELECT clauses into scan, filter, aggregate, sort, and projection nodes.
   See [Chapter 2](./02-sql-planning.md).
4. **Analyze.** The analyzer (`datafusion/optimizer/src/analyzer/`) performs
   semantic normalization: inserting casts, resolving grouping constructs,
   rewriting function forms, checking invariants later stages depend on.
   See [Chapter 4](./04-analyzer-optimizer.md).
5. **Optimize (logical).** The logical optimizer repeatedly applies
   rule-based rewrites — pushing filters and projections toward scans,
   simplifying expressions, decorrelating subqueries, removing redundant
   operators. See [Chapter 4](./04-analyzer-optimizer.md).
6. **Plan (physical).** The physical planner
   (`datafusion/core/src/physical_planner.rs`) lowers each logical node into
   an `ExecutionPlan`, choosing concrete operators such as `FilterExec`,
   `ProjectionExec`, `AggregateExec`, or `HashJoinExec`. See
   [Chapter 5](./05-physical-planning.md).
7. **Optimize (physical).** The physical optimizer rewrites the plan to
   satisfy distribution, ordering, and execution constraints — inserting
   repartitioning, enforcing or removing sorts, choosing join strategies,
   pushing limits. See [Chapter 7](./07-physical-optimizer.md).
8. **Execute.** A caller collects or streams a `DataFrame`. Each
   `ExecutionPlan::execute` call returns a `SendableRecordBatchStream` for
   one partition; operators evaluate physical expressions against incoming
   `RecordBatch` values and emit transformed batches.
9. **Consume.** The caller receives Arrow `RecordBatch` results. The same
   physical plan may run across many partitions and Tokio tasks at once,
   coordinated by `TaskContext` and `RuntimeEnv`
   ([Chapter 12](./12-execution-runtime.md)).

| Stage | Main representation | Main owning crate(s) |
|---|---|---|
| Parse SQL | sqlparser AST | `datafusion-sql` |
| Plan SQL | `LogicalPlan`, `Expr` | `datafusion-sql`, `datafusion-expr` |
| Analyze | validated `LogicalPlan` | `datafusion-optimizer` |
| Logical optimize | optimized `LogicalPlan` | `datafusion-optimizer` |
| Physical plan | `Arc<dyn ExecutionPlan>` | `datafusion-core`, `datafusion-physical-plan` |
| Physical optimize | optimized `ExecutionPlan` tree | `datafusion-physical-optimizer` |
| Execute | `SendableRecordBatchStream` | `datafusion-physical-plan`, `datafusion-execution` |

## A worked example: `SELECT` through the pipeline

Trace `SELECT c1, SUM(c2) FROM t WHERE c1 > 10 GROUP BY c1` end to end:

1. sqlparser produces a `Select` AST node with a `FROM`, a `WHERE`, a
   projection list, and a `GROUP BY`.
2. `SqlToRel` plans the `FROM` into a `TableScan` logical node, wraps it in a
   `Filter` for `c1 > 10`, then builds an `Aggregate` node with `c1` as the
   group expression and `SUM(c2)` as the aggregate expression.
3. The analyzer checks that `c1`'s type supports comparison with the integer
   literal `10` and that `SUM`'s argument type is valid, inserting casts if
   needed.
4. The logical optimizer's filter-pushdown rule notices the `Filter` sits
   directly above a `TableScan` and pushes the predicate into the scan's
   filter list, so the table provider can prune data before it is even read.
5. The physical planner lowers `Aggregate` into a two-phase
   `AggregateExec` (partial, then final after repartitioning by `c1`) and
   lowers `TableScan` into a file scan `ExecutionPlan` that carries the
   pushed-down predicate.
6. The physical optimizer inserts a `RepartitionExec` between the partial and
   final aggregate stages so rows with the same `c1` land in the same final
   partition — grouped aggregation is only correct if every row for a group
   reaches the accumulator that owns that group.
7. Execution streams `RecordBatch` values out of the file scan, filters and
   partially aggregates them per input partition, repartitions by hash of
   `c1`, and finalizes each group's `SUM` accumulator.

Every later chapter is a closer look at one of these seven steps.

## Invariants and edge cases

- **A `LogicalPlan` never encodes partition count, thread count, or memory
  budget.** If you find yourself wanting to express "run this on 4
  partitions" in a logical rule, that decision belongs in the physical
  optimizer, not the logical optimizer.
- **Optimizer rules — logical or physical — must not change query
  semantics.** A rule may only rewrite a plan into one that is provably
  equivalent (or, for the analyzer, into one that is valid where the input
  was invalid, such as missing casts).
- **Execution is pull-based, not push-based.** A physical operator's
  `execute` returns a stream; nothing runs until a downstream consumer polls
  it. This is why building a `DataFrame` does no I/O — only `collect`,
  `execute_stream`, and similar terminal calls do.
- **The same physical plan can be executed multiple times, and multiple
  partitions of it can run concurrently.** `ExecutionPlan` implementations
  must not assume `execute` is called exactly once, or that partitions run
  sequentially.

## Trade-offs

Splitting logical and physical planning into two distinct trees — rather
than annotating one tree with both semantic and physical detail — costs an
extra full plan traversal and a translation step (the physical planner) that
has to re-derive schema and property information the logical plan already
had. In exchange, it buys a much smaller surface for each optimizer: logical
rules reason only about relational semantics, physical rules reason only
about partitioning, ordering, and cost. It also means a `LogicalPlan` can be
`EXPLAIN`ed, serialized, or fed to a cost-based planner without any physical
executor in the loop — useful for tooling and for engines that embed
DataFusion's planner but supply their own execution.

## Key takeaways

- DataFusion moves a query through five representations: SQL AST →
  `LogicalPlan`/`Expr` → optimized `LogicalPlan` → `ExecutionPlan` →
  optimized `ExecutionPlan` → `RecordBatch` stream.
- The logical/physical split is the most important boundary in the codebase:
  logical describes *what*, physical describes *how*.
- Crate boundaries mostly follow trait boundaries — `TableProvider`,
  `OptimizerRule`, `PhysicalOptimizerRule`, `PhysicalExpr`, and
  `ExecutionPlan` are the seams extensions plug into.
- Execution is lazy and pull-based: nothing reads data until a stream is
  polled.

## Going deeper

- [Chapter 2 — SQL Parsing and Planning](./02-sql-planning.md)
- [Chapter 3 — Logical Expressions and Logical Plans](./03-logical-plans.md)
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md)
- [Appendix: Extension Points & Learning Paths](./appendix-extension-points.md)
