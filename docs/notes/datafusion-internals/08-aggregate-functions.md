# Chapter 8 — Aggregate Functions

## Why this exists

`SUM`, `COUNT`, `AVG`, and friends look like simple functions from SQL, but
they are the only kind of expression in DataFusion that must run in more
than one place at once and still produce one answer. A `GROUP BY` on a
partitioned table computes partial sums on every partition in parallel, then
merges those partial sums into a final result — the aggregate implementation
has to know how to do both halves, and how to serialize its half-finished
state so it can cross a partition boundary. This chapter covers the
trait stack that makes that possible, and the hash-based grouping algorithm
that drives it at runtime.

## The big picture

Aggregate support spans three layers, each with a different job:

- **Logical** (`datafusion-expr`): `AggregateUDFImpl` describes *what* the
  function is — its name, signature, return type — and knows how to build
  the runtime state object.
- **Physical expression** (`datafusion-physical-expr`): bridges a logical
  aggregate call to the accumulator construction physical operators need.
- **Physical execution** (`datafusion-physical-plan`): `AggregateExec` and
  its streams drive accumulators against real `RecordBatch` values, in
  **partial**, **final**, or **single-phase** mode.

This split exists because the same `AVG` implementation is reused whether it
runs as a single-partition global aggregate, a two-phase grouped aggregate
behind a repartition, or a streaming window aggregate ([Chapter 9](./09-window-functions.md)
reuses `Accumulator`, too). The implementation only ever has to reason about
"my state, updated from these rows" or "my state, merged with that state."

## Core concepts

### `AggregateUDFImpl`: what a function is

```rust
// datafusion/expr/src/udaf.rs
pub trait AggregateUDFImpl: Debug + DynEq + DynHash + Send + Sync + Any {
    fn name(&self) -> &str;
    fn aliases(&self) -> &[String] { &[] }
    fn signature(&self) -> &Signature;
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef>;

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>;
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>;

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool;
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>>;
}
```

`accumulator` builds one **Accumulator** — runtime state for a single group.
`create_groups_accumulator` builds a **GroupsAccumulator** — a vectorized
variant that tracks state for *every* group in one object, addressed by
group index, which is what real grouped queries use for performance
(`groups_accumulator_supported` lets a UDF opt out and fall back to
per-group `Accumulator`s).

### `Accumulator`: state for one group

```rust
// datafusion/expr-common/src/accumulator.rs
pub trait Accumulator: Send + Sync + Debug + std::any::Any {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;
    fn evaluate(&mut self) -> Result<ScalarValue>;
    fn size(&self) -> usize;
    fn state(&mut self) -> Result<Vec<ScalarValue>>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()>;
}
```

Five methods, four distinct jobs: `update_batch` folds raw input rows into
state (**partial** or **single-phase** aggregation). `state` serializes that
state into scalar values that can be shipped across a partition boundary as
ordinary columns. `merge_batch` folds *other accumulators'* serialized state
into this one (**final** aggregation, after repartitioning by group key).
`evaluate` reads out the finished answer. `retract_batch` undoes an
`update_batch` — needed only for sliding-window aggregation, where a row can
leave a frame as well as enter it ([Chapter 9](./09-window-functions.md)).

`AVG` is the clearest example of why `state` exists as its own method
distinct from `evaluate`: its state is `(sum, count)`, but its answer is
`sum / count`. Merging two partitions' averages by averaging the averages is
wrong; merging their `(sum, count)` pairs and dividing once at the end is
correct. Every stateful aggregate has to make the same distinction between
its internal state shape and its output shape.

## How it works: grouped hash aggregation

`GroupedHashAggregateStream` (`datafusion/physical-plan/src/aggregates/row_hash.rs`)
is the operator behind every `GROUP BY`. Its core loop,
`group_aggregate_batch` (`row_hash.rs:919`), runs once per input batch:

1. Evaluate the `GROUP BY` expressions against the batch to get the group
   key arrays.
2. Evaluate each aggregate's argument expressions (and any `FILTER (WHERE
   ...)` clause) against the batch.
3. **Intern** the group key arrays: `self.group_values.intern(...)` maps
   each row's key to an integer **group index**, using an internal hash
   table (`datafusion/physical-plan/src/aggregates/group_values/`). A key
   seen for the first time gets a new index; a key seen before gets its
   existing index back.
4. Feed the resulting `group_indices` array to every `GroupsAccumulator`.
   In **partial** mode this calls `update_batch(values, group_indices, ...)`;
   in **final** mode it calls `merge_batch(values, group_indices, ...)`
   instead, because `values` here is already-serialized partial state
   arriving from other partitions.

Step 4 is the performance-critical one: `GroupsAccumulator::update_batch`
updates *every* group's state for *every* row in one vectorized call,
indexed by `group_indices`, instead of dispatching to a `HashMap<GroupKey,
Box<dyn Accumulator>>` and calling `update_batch` once per group per row.

Ungrouped aggregation (no `GROUP BY`) skips group-key interning entirely —
`datafusion/physical-plan/src/aggregates/no_grouping.rs` keeps one
accumulator set per partition and emits exactly one output row.

## A worked example: `SELECT c1, SUM(c2) FROM t GROUP BY c1`

Picking up where [Chapter 1](./01-query-lifecycle.md)'s worked example left
off, once the physical optimizer has inserted a `RepartitionExec` between
two `AggregateExec` stages:

1. **Partial stage**, one instance per input partition. Each instance's
   `group_aggregate_batch` interns `c1` values into local group indices and
   calls `SUM`'s `GroupsAccumulator::update_batch` with the `c2` values and
   those indices. At the end of the partition, each distinct `c1` value has
   a partial sum.
2. **Emit partial state.** Each partial-stage accumulator's `state()` is
   read out as a `RecordBatch` — for `SUM`, this is just the running sum
   itself, one row per group.
3. **Repartition** by hash of `c1`, so every row for a given `c1` — now
   living in a different partition per partial-stage instance — converges
   on one final-stage partition.
4. **Final stage.** Each final-stage instance interns `c1` again (its own,
   fresh group index space) and calls `merge_batch` instead of
   `update_batch`, folding every incoming partial sum for a group into that
   group's running total.
5. **Evaluate.** Once a final-stage instance has seen all its input, it
   calls `evaluate()` on each group's accumulator and emits one output row
   per distinct `c1`.

If the query had been a plain `SELECT SUM(c2) FROM t` with no `GROUP BY`,
steps 1 and 4 would use `no_grouping.rs`'s single-accumulator path instead,
and there would be exactly one partial sum per partition to merge.

## Invariants and edge cases

- **`state()`'s output shape must exactly match `state_fields()`'s declared
  shape**, in both field count and type — `merge_batch` on another partition
  will be handed exactly those columns and must be able to interpret them
  without any side channel.
- **A `GroupsAccumulator` must produce the same result whether it sees one
  batch or many** for the same set of rows — grouped state has to be
  incrementally mergeable, not just batch-computable, because input arrives
  in arbitrarily many `RecordBatch`es.
- **`retract_batch` is only required for accumulators used in sliding window
  frames.** An aggregate that cannot support retraction (e.g. one relying on
  a data structure with no efficient "undo") must not be used behind a
  `ROWS BETWEEN ... AND ...` frame that can shrink from the start.
- Group indices are **local to one `GroupedHashAggregateStream` instance**;
  they are never the same across partial and final stages, or across
  parallel partitions of the same stage. Only the group *key* — not the
  index — is meaningful across the repartition boundary.

## Trade-offs

`GroupsAccumulator`'s vectorized, index-addressed API is considerably more
complex to implement than `Accumulator`'s one-state-per-group model — an
implementor has to manage a growable array of per-group state directly
instead of relying on one `Accumulator` instance per group. DataFusion pays
that complexity cost once, in the built-in aggregate implementations, to
avoid paying a per-row dynamic dispatch and hash-map-lookup cost on every
single aggregated row at query time. UDAF authors who skip
`create_groups_accumulator` still work correctly (falling back to per-group
`Accumulator`s via `groups_accumulator_supported() == false`); they just
give up the vectorized fast path.

## Key takeaways

- Aggregate support is layered: `AggregateUDFImpl` (what the function is) →
  `Accumulator`/`GroupsAccumulator` (runtime state) → `AggregateExec` (drives
  state against batches in partial/final/single-phase mode).
- `state()` and `evaluate()` are deliberately separate: intermediate state
  (e.g. `AVG`'s sum-and-count) is not always the same shape as the final
  answer.
- Grouped aggregation interns group keys into integer indices via a hash
  table, then updates all groups' state for a batch in one vectorized
  `GroupsAccumulator` call.
- Two-phase aggregation (partial → repartition by group key → final) is what
  makes `GROUP BY` correct and parallel at the same time; every row for a
  group must reach the one final-stage accumulator that owns that group.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md) — how `AggregateExec` fits into the `ExecutionPlan` tree
- [Chapter 6 — Physical Expressions](./06-physical-expressions.md) — how aggregate argument expressions are evaluated
- [Chapter 9 — Window Functions](./09-window-functions.md) — `Accumulator` and `retract_batch` reused for sliding frames
- [Chapter 12 — Execution Runtime](./12-execution-runtime.md) — how a hash aggregate spills to disk under memory pressure
