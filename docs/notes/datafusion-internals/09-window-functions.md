# Chapter 9 — Window Functions

## Why this exists

A window function — `ROW_NUMBER()`, `LAG()`, `SUM(x) OVER (...)` — has to
solve a problem plain aggregates don't: it must produce **one output row per
input row**, not one row per group, while still reasoning about a group
(the `PARTITION BY`) and often a sliding sub-range of it (the frame). Some
window functions can answer as soon as they see the current row; others need
the entire partition buffered first. This chapter covers the evaluator API
that expresses that difference, and how it drives execution differently
depending on which kind of function it is.

## The big picture

Window planning has the same logical/physical split as the rest of the
engine ([Chapter 1](./01-query-lifecycle.md)): a `WindowUDFImpl` describes a
function's name, signature, and how to build a runtime evaluator; a
`PartitionEvaluator` is that runtime evaluator, handed one partition's worth
of argument arrays and asked to produce output. Physical planning picks one
of two execution strategies based on what the evaluator reports about
itself:

- **`WindowAggExec`** (`datafusion/physical-plan/src/windows/window_agg_exec.rs`)
  buffers a full partition, then evaluates — required for functions that
  need the whole partition before they can answer even the first row (most
  frame-based aggregates over `RANGE`, for instance).
- **`BoundedWindowAggExec`** (`datafusion/physical-plan/src/windows/bounded_window_agg_exec.rs`)
  can emit rows as they become determinable, without waiting for the whole
  partition — used for **causal** functions like `ROW_NUMBER` and `RANK`,
  and for `ROWS`-frame aggregates that support incremental (retractable)
  updates.

Reusing `Accumulator`/`GroupsAccumulator` from [Chapter 8](./08-aggregate-functions.md)
for frame-based window aggregates (`SUM(x) OVER (ROWS BETWEEN ...)`) is what
makes `retract_batch` necessary in the first place — a frame that slides
forward must remove rows that fell out the back as well as add rows that
entered the front.

## Core concepts

### `WindowUDFImpl`: what a function is

```rust
// datafusion/expr/src/udwf.rs
pub trait WindowUDFImpl: Debug + DynEq + DynHash + Send + Sync + Any {
    fn name(&self) -> &str;
    fn aliases(&self) -> &[String] { &[] }
    fn signature(&self) -> &Signature;

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        expr_args.input_exprs().into()
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>>;
}
```

This mirrors `AggregateUDFImpl` and `ScalarUDFImpl` deliberately: a logical
metadata trait whose main job is to construct the object that does the real
work at execution time.

### `PartitionEvaluator`: the runtime contract

```rust
// datafusion/expr/src/partition_evaluator.rs
pub trait PartitionEvaluator: Debug + Send + std::any::Any {
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> { Ok(()) }
    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> { ... }
    fn is_causal(&self) -> bool { false }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef>;
    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue>;

    fn uses_window_frame(&self) -> bool;
    fn supports_bounded_execution(&self) -> bool { false }
}
```

Four capability flags decide how an evaluator gets driven:

- **`uses_window_frame`** — does this function read from a `Range<usize>`
  frame at all, or does it ignore frame bounds entirely (e.g. `ROW_NUMBER`,
  which only cares about row position)?
- **`supports_bounded_execution`** — can this evaluator answer a row without
  having buffered the whole partition? If true, `BoundedWindowAggExec` can
  drive it incrementally.
- **`is_causal`** — does a row's answer depend only on rows at or before it
  (never on rows after)? A causal function can be emitted the moment its
  causal inputs are known; a non-causal one (e.g. `LEAD`, or a centered
  `RANGE` frame) must wait for enough of the future to arrive.
- **`get_range`** — for frame-based evaluators, computes the actual
  `Range<usize>` of rows in the current frame for row `idx`, given the
  partition's frame specification.

### Frames: `ROWS`, `RANGE`, `GROUPS`

`WindowFrame` (`datafusion/expr/src/window_frame.rs`) represents a SQL frame
clause in a planner-independent form. `WindowFrameUnits` is the frame type:
**`Rows`** counts physical rows from the current row; **`Range`** requires
exactly one `ORDER BY` term and includes every row whose value of that term
falls within a distance of the current row's value; **`Groups`** counts
distinct peer groups (rows sharing all `ORDER BY` values) rather than
individual rows. The unit matters for `get_range`: a `ROWS` frame is a
simple index arithmetic problem, while a `RANGE` frame requires comparing
ordering-column values to find where the range boundary actually falls.

## How it works: standard versus sliding-aggregate window expressions

Physical window expression construction bridges a logical window call to a
runtime evaluator (`datafusion/physical-expr/src/window/window_expr.rs`).
Two families of physical window expression cover most built-in functions:

- **`standard.rs`** implements ranking and navigation functions —
  `RANK`, `ROW_NUMBER`, `LAG`, `LEAD` — which mostly ignore frame bounds and
  are naturally causal or trivially computable from row position.
- **`sliding_aggregate.rs`** wraps an `Accumulator`/`GroupsAccumulator`
  (the same trait from [Chapter 8](./08-aggregate-functions.md)) so any
  aggregate can be used as a frame-based window function. As the frame slides
  forward one row at a time, the wrapper calls `update_batch` for rows
  entering the frame and `retract_batch` for rows leaving it, instead of
  recomputing the whole frame's aggregate from scratch on every row.

Physical planning also has to ensure the *input* to a window operator is
already sorted and partitioned correctly — the physical optimizer
([Chapter 7](./07-physical-optimizer.md)) checks that upstream ordering
satisfies each window expression's `PARTITION BY`/`ORDER BY` requirement and
inserts a sort if it does not.

## A worked example: `RANK() OVER (PARTITION BY dept ORDER BY salary)` vs. a sliding sum

Consider two window functions planned side by side:

- `RANK() OVER (PARTITION BY dept ORDER BY salary)`: `standard.rs`'s rank
  evaluator reports `uses_window_frame() == false` (rank ignores frame
  bounds — it only cares about peer groups in sort order) and
  `is_causal() == true` (a row's rank depends only on how many rows with a
  lower `salary` came before it in the same partition). `BoundedWindowAggExec`
  can emit each department's ranks incrementally as sorted input streams by,
  never buffering more than the current peer group.
- `SUM(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN 2
  PRECEDING AND 2 FOLLOWING)`: this goes through `sliding_aggregate.rs`
  wrapping `SUM`'s `Accumulator`. `uses_window_frame() == true`, and the
  evaluator supports bounded execution: for each row, `get_range` computes
  the five-row window, and the wrapper `retract_batch`es the row that just
  fell more than 2 rows behind and `update_batch`es the row that just came
  into range 2 ahead — an O(1) amortized update per row instead of
  re-summing five values every time.

Both plans still require the input already sorted by `(dept, salary)` before
reaching the window operator; that ordering requirement is what the physical
optimizer enforces upstream.

## Invariants and edge cases

- **Every window function preserves input row count.** Unlike
  `AggregateExec`, a window operator never reduces rows — this is the
  defining difference from [Chapter 8](./08-aggregate-functions.md)'s
  aggregates.
- **`is_causal() == true` is a correctness claim, not just a hint.**
  `BoundedWindowAggExec` uses it to decide when a row can be safely emitted;
  claiming causality for a function that actually depends on future rows
  (e.g. `LEAD`) produces wrong output, not just suboptimal streaming.
- **A `RANGE` frame requires exactly one `ORDER BY` expression.** This is a
  SQL-level restriction, not an implementation detail: "within 5 of the
  current row's value" is only well-defined for a single ordering key.
- **Non-causal, non-bounded functions must fall back to `WindowAggExec`.**
  Forcing such a function through the bounded/streaming path would require
  buffering the same amount of state anyway, just less legibly.

## Trade-offs

The capability-flag design (`is_causal`, `supports_bounded_execution`,
`uses_window_frame`) pushes the cost of correctness reasoning onto each
function implementation rather than trying to infer these properties
generically from a frame specification. This is more upfront work per
built-in function, but it lets the execution operator make a single cheap
dispatch decision — `BoundedWindowAggExec` versus `WindowAggExec` — instead
of re-deriving streaming eligibility from frame semantics at every query.
Reusing `Accumulator` for frame-based window aggregation (rather than a
separate window-aggregate trait) means every aggregate written for
`GROUP BY` gets `OVER (...)` support for free, at the cost of requiring
`retract_batch` — which some aggregates cannot implement efficiently, or at
all — for the sliding-frame case.

## Key takeaways

- `WindowUDFImpl` builds a `PartitionEvaluator`; the evaluator's capability
  flags (`is_causal`, `supports_bounded_execution`, `uses_window_frame`)
  determine whether execution can stream (`BoundedWindowAggExec`) or must
  buffer the whole partition (`WindowAggExec`).
- `ROWS`, `RANGE`, and `GROUPS` frames differ in how `get_range` computes
  frame boundaries — physical-row counting versus value-distance versus
  peer-group counting.
- Frame-based window aggregates reuse the `Accumulator` trait from
  [Chapter 8](./08-aggregate-functions.md), driven incrementally via
  `update_batch`/`retract_batch` as the frame slides.
- Window functions never change row count — the defining contrast with
  `GROUP BY` aggregation.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 8 — Aggregate Functions](./08-aggregate-functions.md) — the `Accumulator` trait reused for sliding frames
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md) — where `WindowAggExec`/`BoundedWindowAggExec` fit in the `ExecutionPlan` tree
- [Chapter 6 — Physical Expressions](./06-physical-expressions.md) — evaluating a window function's argument expressions
- [Chapter 7 — Physical Optimizer](./07-physical-optimizer.md) — enforcing the partition/order requirements windows depend on
