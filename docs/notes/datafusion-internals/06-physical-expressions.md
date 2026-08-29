# Chapter 6 — Physical Expressions

## Why this exists

[Chapter 5](./05-physical-planning.md) showed operators like `FilterExec`
holding a `PhysicalExpr` and calling `evaluate` on it once per batch. This
chapter is about what lives on the other side of that call: the executable
expression trees that actually touch Arrow arrays. Every predicate, every
projected column, every join key, every `SUM(x)` argument passes through
this layer. If [Chapter 3](./03-logical-plans.md)'s `Expr` is "what the
expression means," `PhysicalExpr` is "how to compute it on this batch, on
this machine, right now."

## The big picture

A logical `Expr` tree and its lowered `PhysicalExpr` tree have the same
shape, but different content. A logical `BinaryExpr { left: Column("a"),
op: Gt, right: Literal(5) }` becomes a physical tree that has already
resolved `Column("a")` to a schema *index* (no more name lookup at
evaluation time) and knows exactly which Arrow compute kernel implements
`Gt` for the resolved types:

```
Expr::BinaryExpr(Column("a"), Gt, Literal(5))
        │  create_physical_expr, given input schema
        ▼
Arc<dyn PhysicalExpr>  =  BinaryExpr {
    left:  Column { name: "a", index: 2 },
    op:    Gt,
    right: Literal(ScalarValue::Int32(Some(5))),
}
        │  evaluate(&record_batch)
        ▼
ColumnarValue::Array(BooleanArray)   // one bool per row
```

Everything expensive about name resolution, type coercion, and function
overload resolution happens once, during planning. Evaluation is meant to be
cheap and repeated — the same `PhysicalExpr` tree is evaluated once per
batch, potentially millions of times over a query's lifetime, so its
`evaluate` path is where DataFusion's per-row overhead actually lives.

## Core concepts

### `PhysicalExpr`: the runtime expression interface

Defined in `datafusion/physical-expr-common/src/physical_expr.rs:75`:

```rust
pub trait PhysicalExpr: Any + Send + Sync + Display + Debug + DynEq + DynHash {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field(input_schema)?.data_type().to_owned())
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field(input_schema)?.is_nullable())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> { ... }
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> { ... }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}
```

`data_type` and `nullable` both default to deriving from `return_field`,
which most implementations override directly (it lets an expression report
type, nullability, and field name/metadata in one call instead of three).
`children` and `with_new_children` give `PhysicalExpr` trees the same
generic tree-rewrite support (`TreeNode`) that `LogicalPlan` and `Expr` have
— optimizer rules that rewrite physical expressions (e.g. constant folding)
don't need a bespoke traversal for every expression variant.

`evaluate_selection` is worth calling out on its own: it lets a caller
evaluate an expression only for rows where a `BooleanArray` selection is
true, without allocating a full-size result for rows that will be discarded
anyway. It has three fast paths built in — skip filtering entirely if the
selection is all-true, skip evaluating entirely if the selection is
all-false, and only pay for `filter_record_batch` plus a scatter-back when
the selection is mixed. `FilterExec`-like operators evaluating a downstream
expression only for surviving rows use this instead of `evaluate`.

### `ColumnarValue`: array-or-scalar

Defined in `datafusion/expr-common/src/columnar_value.rs:96`:

```rust
pub enum ColumnarValue {
    Array(ArrayRef),
    Scalar(ScalarValue),
}
```

This is the single most important performance decision in the expression
layer. An expression's result is *either* a full Arrow array (one value per
row) *or* a scalar that logically applies to every row in the batch — and
the type system forces every consumer to handle both. Evaluating `a > 5`
evaluates `a` to `ColumnarValue::Array(...)` and `5` to
`ColumnarValue::Scalar(Int32(5))`; the binary-expression kernel can then
call an Arrow scalar-comparison kernel directly instead of first
materializing an array of five thousand `5`s just to compare against it.

### Converting `Expr` to `PhysicalExpr`

`create_physical_expr` (`datafusion/physical-expr/src/planner.rs:115`) is
the conversion entry point, called once per expression during physical
planning ([Chapter 5](./05-physical-planning.md)). It resolves `Column`
references against the input's physical schema by name, turning them into
index-based lookups; builds `expressions::binary::BinaryExpr`,
`expressions::cast::CastExpr`, and similar structs for operators and casts;
and delegates scalar function calls to
`datafusion/physical-expr/src/scalar_function.rs`, which resolves the
logical `ScalarUDF` into its physical evaluation form. Note that
`datafusion/physical-expr/src/physical_expr.rs` — despite the name — does
not define the trait itself (that's in `physical-expr-common`); it holds
planner-adjacent helpers such as converting `PhysicalExpr` and
`PhysicalSortExpr` between forms.

### Built-in expression kinds

`datafusion/physical-expr/src/expressions/` holds one file (or module) per
expression kind: `column.rs` (index-based lookup — no computation, just
returns the referenced array), `literal.rs` (returns
`ColumnarValue::Scalar` directly — no array materialization unless a caller
forces it), `binary.rs` (arithmetic/comparison/boolean, dispatching to
Arrow compute kernels and handling every `Array`/`Scalar` combination on
each side), `cast.rs`/`try_cast.rs` (Arrow type casts, with `try_cast`
returning null rather than erroring on an unrepresentable value), `case.rs`,
`like.rs`, `in_list`, and `dynamic_filters.rs` (predicates whose bounds are
filled in during execution rather than planning — used for late materialized
filters such as `SortExec`'s TopK pruning bound).

## How it works: evaluating a compound expression

Evaluating `PhysicalExpr::evaluate` on a tree recurses depth-first: a
`BinaryExpr` evaluates its `left` and `right` children first (each
returning a `ColumnarValue`), then applies its operator's Arrow kernel to
the two results. Every node in the tree runs against the *same* input
`RecordBatch` — there is no intermediate materialization of a "row" the way
a row-oriented engine would have one; each node's output is either a full
array aligned with the batch or a scalar that broadcasts across it.

For expressions with a mix of column and scalar operands, this recursion
naturally minimizes allocation: `a + 1` evaluates `a` (returns the existing
array, zero-copy) and `1` (returns a scalar, zero allocation), then the
`+` kernel allocates exactly one result array. A deeply nested expression
like `(a + b) * (c - d)` allocates one intermediate array per binary node —
no more, no fewer — which is why expression trees with excessive nesting
(e.g. auto-generated SQL with hundreds of `OR`ed predicates) show up as real
CPU cost, not just an aesthetic concern.

## A worked example: `WHERE a > 5 AND b = 'x'`

1. Physical planning calls `create_physical_expr` on the logical
   `(a > 5) AND (b = 'x')` expression against the scan's output schema,
   producing an `Arc<dyn PhysicalExpr>` tree: an `AND`-flavored `BinaryExpr`
   whose children are two more `BinaryExpr`s (`Gt` and `Eq`), whose
   children are `Column`/`Literal` leaves with `a` and `b` resolved to
   concrete schema indices.
2. `FilterExec` ([Chapter 5](./05-physical-planning.md)) calls
   `predicate.evaluate(&batch)` once per input batch.
3. The `AND` node evaluates its left child: the `Gt` node evaluates
   `Column("a")` (an O(1) array reference, no copy) and `Literal(5)`
   (a scalar), then calls Arrow's greater-than kernel to produce a
   `BooleanArray`.
4. The `AND` node evaluates its right child similarly, comparing `Column("b")`
   against `Literal("x")` using an Arrow string-equality kernel.
5. The `AND` node combines the two boolean arrays with Arrow's boolean-and
   kernel and returns the combined `ColumnarValue::Array(BooleanArray)`.
6. `FilterExec` calls `as_boolean_array` on the result and passes it to
   Arrow's `filter_record_batch` to produce the surviving rows.

No step in this trace does a name lookup, a function-registry lookup, or a
type check — all of that was resolved once, during `create_physical_expr`,
so the hot per-batch path is pure array computation.

## Invariants and edge cases

- **`evaluate`'s `ColumnarValue::Array` result must have exactly
  `batch.num_rows()` elements.** Callers rely on this to zip an expression's
  output against the batch without a length check on every use.
- **A `ColumnarValue::Scalar` result represents the *same* logical value for
  every row**, not "the value for row 0." Code that needs a materialized
  array from a scalar result calls `ColumnarValue::into_array(num_rows)`
  rather than assuming array shape.
- **`evaluate_selection` requires `selection.len() == batch.num_rows()`** —
  it validates this and returns an error rather than silently truncating or
  panicking on a mismatch.
- **Expressions must be side-effect-free and safe to re-evaluate.** The same
  `PhysicalExpr` instance is shared (via `Arc`) across every partition of a
  plan and evaluated concurrently; it must not mutate shared state without
  its own synchronization.

## Trade-offs

Resolving columns to indices and functions to concrete implementations
during planning — rather than resolving by name at every `evaluate` call —
pushes cost forward, so planning a query with hundreds of expressions is not
free. In exchange, the hot execution path never pays the name-resolution or
overload-resolution overhead per batch, only per query. The
`ColumnarValue` array-or-scalar split has a similar shape: it complicates
every kernel implementation and consumer, which must handle both variants
(and their combinations for binary operators), in exchange for avoiding
enormous amounts of wasted broadcast-array allocation on the majority of
real predicates that compare a column against a constant.

## Key takeaways

- `PhysicalExpr` trees mirror logical `Expr` trees but with names resolved
  to indices and functions resolved to concrete implementations — all
  resolution happens once, at `create_physical_expr` time.
- `ColumnarValue::Array`/`Scalar` lets binary and function kernels avoid
  materializing broadcast arrays for scalar operands.
- `evaluate_selection` is the filtered-evaluation fast path used when only
  some rows of a batch matter; it has explicit all-true/all-false/mixed
  branches.
- Expression trees are shared (`Arc`) and evaluated concurrently across
  partitions — they must be immutable and side-effect-free.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md)
- [Chapter 8 — Aggregate Functions](./08-aggregate-functions.md)
- [Chapter 9 — Window Functions](./09-window-functions.md)
