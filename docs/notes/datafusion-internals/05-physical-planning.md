# Chapter 5 — Physical Planning and Execution Plans

## Why this exists

[Chapter 1](./01-query-lifecycle.md) drew the line between *what a query
means* and *how to compute it*. Physical planning is where the engine
crosses that line: it takes the optimized `LogicalPlan` from
[Chapter 4](./04-analyzer-optimizer.md) and produces a tree of concrete,
runnable operators. Every operator you can name when you read an `EXPLAIN`
output — `FilterExec`, `HashJoinExec`, `DataSourceExec` — is a node this
layer creates. Understanding it is the difference between reading query
plans and only reading query results.

## The big picture

Physical planning is a one-shot, recursive lowering: each `LogicalPlan` node
maps to one (or a small constant number of) **`ExecutionPlan`** nodes, with
children lowered first so the parent can see their output schema and
properties.

```
LogicalPlan::Filter(predicate, input)
        │  physical planner lowers input first
        ▼
ExecutionPlan children: [ <lowered input> ]
        │  create_physical_expr(predicate) against input's schema
        ▼
FilterExec { predicate: Arc<dyn PhysicalExpr>, input, metrics, ... }
```

Unlike the logical optimizer, which repeatedly rewrites a tree in place, the
physical planner runs once, bottom-up, and *builds* a new tree — it does not
mutate `LogicalPlan` nodes into `ExecutionPlan` nodes. The physical
optimizer ([Chapter 7](./07-physical-optimizer.md)) is what rewrites the
result afterward.

## Core concepts

### `ExecutionPlan`: the physical operator interface

Every physical operator implements `ExecutionPlan`
(`datafusion/physical-plan/src/execution_plan.rs:96`):

```rust
pub trait ExecutionPlan: Any + Debug + DisplayAs + Send + Sync {
    fn name(&self) -> &str;

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().schema())
    }

    fn properties(&self) -> &PlanProperties;

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;
}
```

Two things stand out. First, `execute` takes a **partition index**, not "run
the whole plan" — DataFusion plans are inherently partitioned, and every
operator must be ready to produce just one partition's worth of output on
demand. Second, most of the trait is *declarative metadata*
(`required_input_distribution`, `required_input_ordering`,
`maintains_input_order`) rather than behavior. This metadata is what the
physical optimizer reads to decide whether a child's output already
satisfies a parent's needs, or whether a repartition or sort must be
inserted.

### `PlanProperties`: what an operator promises about its output

`PlanProperties` (`datafusion/physical-plan/src/execution_plan.rs:1108`)
bundles an operator's output **schema**, **`Partitioning`** (how many
partitions, and how rows are distributed across them — hash, round-robin,
unknown), **equivalence properties** (which columns are known-sorted or
known-equal, used to avoid redundant sorts), **emission type** (does the
operator emit incrementally or only after seeing all input), and
**boundedness** (can this operator run over an infinite/streaming input).
Every `ExecutionPlan` must compute and cache its own `PlanProperties` when
constructed — children's properties flow upward, transformed by what the
parent does to them (a `FilterExec` keeps its child's partitioning; a
`RepartitionExec` changes it).

### The default physical planner

`DefaultPhysicalPlanner` (`datafusion/core/src/physical_planner.rs:259`)
implements the `PhysicalPlanner` trait and is what `SessionState` uses
unless a caller supplies a custom one. It recursively lowers `LogicalPlan`
nodes to `ExecutionPlan` nodes, converts logical `Expr` trees into
`PhysicalExpr` trees via `create_physical_expr` (see
[Chapter 6](./06-physical-expressions.md)), and dispatches
`LogicalPlan::Extension` nodes to registered `ExtensionPlanner`
implementations — the hook that lets a project add a custom logical operator
without forking the planner.

## How it works: lowering a plan node

For a typical unary node such as `Filter`, the planner:

1. Recursively lowers the child `LogicalPlan` first, obtaining an
   `Arc<dyn ExecutionPlan>` with a concrete output schema.
2. Converts the logical predicate `Expr` into a `PhysicalExpr` tree using
   `create_physical_expr`, resolving each `Column` reference to a physical
   index in the child's schema.
3. Constructs `FilterExec::try_new(predicate, child)`, which computes its
   own `PlanProperties` from the child's (filtering does not change
   partitioning, and it can optionally preserve certain equivalence
   properties).
4. Returns the new node as `Arc<dyn ExecutionPlan>` to its own caller, which
   may be another lowering step or the top-level `create_physical_plan`
   entry point.

Binary and N-ary nodes (joins, unions) follow the same shape but lower every
child first. Nodes that read data — `TableScan` — instead call into the
table provider (see [Chapter 10](./10-data-sources.md)):
`TableProvider::scan(state, projection, filters, limit)` returns an
`ExecutionPlan` directly, so file-format-specific scan planning happens
inside the data source layer, not inside `DefaultPhysicalPlanner`.

Note that physical planning by itself does **not** guarantee the resulting
tree is executable as-is: it does not insert repartitioning to satisfy a
join's distribution requirement, or a sort to satisfy a window function's
ordering requirement. That is deliberate — the physical optimizer
([Chapter 7](./07-physical-optimizer.md)) is a separate pass specifically so
that "choose an operator" and "make the tree satisfy every operator's
requirements" can evolve independently.

## A worked example: `FilterExec`'s streaming loop

Once a `FilterExec` exists, its behavior at execution time lives in
`FilterExecStream::poll_next`
(`datafusion/physical-plan/src/filter.rs:1012-1090`):

```rust
impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                return self.metrics.baseline_metrics.record_poll(
                    Poll::Ready(Some(Ok(batch))),
                );
            }
            if self.batch_coalescer.is_finished() {
                return Poll::Ready(None);
            }
            match ready!(self.input.poll_next_unpin(cx)) {
                None => {
                    self.batch_coalescer.finish()?;
                    self.input = Box::pin(EmptyRecordBatchStream::new(self.input.schema()));
                }
                Some(Ok(batch)) => {
                    // evaluate predicate, filter_record_batch, push into coalescer,
                    // stop early on PushBatchStatus::LimitReached
                    // ...
                }
                other => return Poll::Ready(other),
            }
        }
    }
}
```

This is the concrete shape of "pull-based execution" from Chapter 1: a
`FilterExec` node does nothing until something calls `poll_next` on its
stream. Each call either returns a completed output batch immediately (from
an internal coalescer that merges small filtered batches into
larger ones), pulls one more batch from the child stream and evaluates the
predicate against it, or signals the end of the stream. Note the resource
cleanup on both the `None` (input exhausted) and `LimitReached` paths: the
stream replaces `self.input` with an empty stream so the child pipeline's
resources — buffers, open file handles further downstream — can be dropped
before this operator itself finishes.

## Invariants and edge cases

- **`execute` must be safe to call once per partition, possibly
  concurrently with other partitions.** An operator that keeps mutable
  cross-partition state (e.g. a shared hash table) must synchronize it
  itself; the trait gives no such guarantee for free.
- **A node's `PlanProperties` must be an honest description of its output**,
  not an aspiration. If a node claims an ordering it does not actually
  produce, downstream operators that trust that ordering (e.g. a
  streaming window function) will silently produce wrong results rather
  than fail loudly.
- **`with_new_children` must preserve the node's own configuration** while
  swapping only its children — this is how physical optimizer rules rewrite
  a tree node-by-node without reconstructing operators from scratch.
- **Extension logical nodes without a registered `ExtensionPlanner` fail
  physical planning** — the default planner has no generic fallback for
  `LogicalPlan::Extension`, by design, since it cannot know what physical
  behavior an arbitrary extension node should have.

## Trade-offs

Building a brand-new `ExecutionPlan` tree instead of converting `LogicalPlan`
nodes in place costs an extra allocation pass and means logical and physical
plans coexist in memory during planning. The payoff is separation of
concerns: `ExecutionPlan` types don't need to carry logical-plan fields they
don't use (no `Expr`, no `DFSchema` qualifiers), and `LogicalPlan` types
don't need to carry physical fields (no `PlanProperties`, no partition
count). It also means a `LogicalPlan` remains valid and inspectable
(`EXPLAIN`, serialization) even for engines that never execute it — the
physical tree is optional, derived state, not the source of truth.

## Key takeaways

- Physical planning lowers an optimized `LogicalPlan` into an
  `ExecutionPlan` tree, bottom-up, in one pass — it does not itself insert
  repartitioning or sorting to satisfy operator requirements.
- `ExecutionPlan` is mostly declarative metadata (`PlanProperties`,
  distribution/ordering requirements) plus one behavioral method,
  `execute(partition, context) -> SendableRecordBatchStream`.
- `PlanProperties` — schema, partitioning, equivalence, emission type,
  boundedness — is the contract the physical optimizer reads to decide what
  to insert.
- Custom logical operators plug in through `ExtensionPlanner`, registered on
  `SessionState`, without modifying `DefaultPhysicalPlanner` itself.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 4 — Analyzer and Logical Optimizer](./04-analyzer-optimizer.md)
- [Chapter 6 — Physical Expressions](./06-physical-expressions.md)
- [Chapter 7 — Physical Optimizer](./07-physical-optimizer.md)
- [Chapter 10 — Data Sources](./10-data-sources.md)
