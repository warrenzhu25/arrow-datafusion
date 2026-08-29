# Chapter 3 — Logical Expressions and Logical Plans

## Why this exists

`Expr` and `LogicalPlan` are the data structures every other layer of
DataFusion reads or rewrites: [SQL planning](./02-sql-planning.md) builds
them, [the analyzer and optimizer](./04-analyzer-optimizer.md) rewrite them,
[physical planning](./05-physical-planning.md) lowers them, and the
DataFrame API constructs them directly without SQL at all. If you're going
to read one data structure definition in this codebase closely, it should be
these two — nearly every bug report and every optimizer rule is really a
statement about what a specific `Expr` or `LogicalPlan` shape means.

## The big picture

The logical layer is DataFusion's semantic model: it represents *what* a
query means, independent of *how* it will run. A logical `Join` node says
two relations are joined on some key — not which side builds a hash table,
not how many threads run it, not whether the join even needs a hash table at
all (a merge join might do). That independence is what lets the same
`LogicalPlan` be optimized, `EXPLAIN`ed, and eventually handed to physical
planning without any execution engine in the loop yet.

`Expr` values live inside `LogicalPlan` nodes: a `Projection` holds output
expressions, a `Filter` holds a predicate expression, an `Aggregate` holds
group and aggregate expressions, a `Join` holds join keys and a filter.
Neither type stands alone — a `LogicalPlan` tree is really a tree of `Expr`
trees, nested inside relational operator nodes.

## Core concepts

### `Expr`: the logical expression tree

**`Expr`** (`datafusion/expr/src/expr.rs:326`) enumerates every logical
expression form DataFusion's planner produces:

```rust
// datafusion/expr/src/expr.rs
pub enum Expr {
    Alias(Alias),
    Column(Column),
    ScalarVariable(FieldRef, Vec<String>),
    Literal(ScalarValue, Option<FieldMetadata>),
    BinaryExpr(BinaryExpr),
    Like(Like),
    Not(Box<Expr>),
    IsNotNull(Box<Expr>),
    Negative(Box<Expr>),
    Between(Between),
    Case(Case),
    Cast(Cast),
    TryCast(TryCast),
    ScalarFunction(ScalarFunction),
    AggregateFunction(AggregateFunction),
    WindowFunction(Box<WindowFunction>),
    InList(InList),
    Exists(Exists),
    InSubquery(InSubquery),
    ScalarSubquery(Subquery),
    Wildcard { qualifier: Option<TableReference>, options: Box<WildcardOptions> },
    GroupingSet(GroupingSet),
    Placeholder(Placeholder),
    OuterReferenceColumn(FieldRef, Column),
    Unnest(Unnest),
    // ...and more: SimilarTo, IsTrue/IsFalse/IsUnknown variants,
    // SetComparison, HigherOrderFunction, Lambda, LambdaVariable
}
```

Every rewrite pass in the optimizer walks this enum through DataFusion's
**`TreeNode`** traversal APIs (`datafusion-common`) rather than hand-writing
a recursive match over every variant. That is what lets a rule like
constant folding or common-subexpression elimination be written once and
apply correctly no matter how deeply an `Expr` is nested inside a
`LogicalPlan`.

### `LogicalPlan`: the relational operator tree

**`LogicalPlan`** (`datafusion/expr/src/logical_plan/plan.rs:206`) enumerates
DataFusion's relational operators:

```rust
// datafusion/expr/src/logical_plan/plan.rs
pub enum LogicalPlan {
    Projection(Projection),
    Filter(Filter),
    Window(Window),
    Aggregate(Aggregate),
    Sort(Sort),
    Join(Join),
    Repartition(Repartition),
    Union(Union),
    TableScan(TableScan),
    EmptyRelation(EmptyRelation),
    Subquery(Subquery),
    SubqueryAlias(SubqueryAlias),
    Limit(Limit),
    Values(Values),
    Explain(Explain),
    Extension(Extension),
    Distinct(Distinct),
    Dml(DmlStatement),
    Ddl(DdlStatement),
    Unnest(Unnest),
    RecursiveQuery(RecursiveQuery),
    // ...and more: Statement, Analyze, Copy, DescribeTable
}
```

`Extension` is the escape hatch: a project that needs a relational operator
DataFusion doesn't ship can wrap it as an `Extension` node and teach the
[physical planner](./05-physical-planning.md) how to lower it, without
forking `LogicalPlan` itself.

### `DFSchema`: schema plus SQL name resolution

**`DFSchema`** (`datafusion/common/src/dfschema.rs:112` — note this lives in
`datafusion-common`, not `datafusion-expr`, since both SQL planning and
physical planning depend on it) extends an Arrow `Schema` with relation
qualifiers. This matters because SQL allows two tables joined together to
each have a column literally named `id` — an Arrow `Schema` alone cannot
distinguish `left.id` from `right.id`, but `DFSchema` can. The
**`ExprSchemable`** trait (`datafusion/expr/src/expr_schema.rs:44`,
implemented for `Expr`) answers the questions every later stage needs
answered against a `DFSchema`: what data type does this expression return,
can its result be null, is it even valid against this schema. Analyzer and
optimizer rules depend on these answers *before* they rewrite a plan — you
cannot safely insert a cast without first knowing the expression's current
type.

### `LogicalPlanBuilder`: schema bookkeeping as a side effect

**`LogicalPlanBuilder`** (`datafusion/expr/src/logical_plan/builder.rs:127`)
is a fluent API used by the SQL planner, the DataFrame API, and tests alike.
Its real job is not convenience — it's that every method which adds a node
(`.filter(...)`, `.aggregate(...)`, `.project(...)`) also computes that
node's output `DFSchema` at construction time, so no caller has to
separately re-derive "what columns does this plan produce" by hand.

### Invariants as an explicit checked contract

`datafusion/expr/src/logical_plan/invariants.rs` defines functions like
`assert_always_invariants_at_current_node` and
`assert_executable_invariants` that validate a `LogicalPlan` is internally
consistent — schemas match, expression references are valid, subqueries are
well-formed (`check_subquery_expr`, `check_correlations_in_subquery`).
Optimizer passes call these to catch a broken rewrite immediately at the
node that broke it, rather than as a confusing failure three stages later.

## How it works: expressions as UDF metadata carriers

Scalar, aggregate, and window functions are represented as `Expr` variants
(`ScalarFunction`, `AggregateFunction`, `WindowFunction`) that wrap a
registered UDF implementation:

- `datafusion/expr/src/udf.rs` defines `ScalarUDF` and the `ScalarUDFImpl`
  trait — signature, return-type logic, volatility, documentation.
- `datafusion/expr/src/udaf.rs` defines the aggregate equivalent, adding
  accumulator construction and state-field metadata (covered in
  [Chapter 8](./08-aggregate-functions.md)).
- `datafusion/expr/src/udwf.rs` defines the window equivalent, adding
  partition-evaluator construction (covered in
  [Chapter 9](./09-window-functions.md)).

All three wrap implementation traits precisely so a logical `Expr` can carry
"call this function" without knowing anything about how that function
executes — that knowledge is added at the physical layer.

`datafusion/expr/src/table_source.rs` plays the same role for tables: it
defines the logical table abstraction SQL planning uses so a `TableScan`
node can exist without knowing the physical scan implementation
([Chapter 10](./10-data-sources.md) covers the physical side).

## A worked example: what one `Aggregate` node encodes

Consider `SELECT c1, SUM(c2) AS total FROM t WHERE c1 > 10 GROUP BY c1`
after SQL planning. The `LogicalPlan` tree is:

```
Aggregate: groupBy=[c1], aggr=[SUM(c2)]
  Filter: c1 > 10
    TableScan: t
```

The `Aggregate` node's `DFSchema` (built by `LogicalPlanBuilder::aggregate`)
has two fields: `c1` (from the group expression) and `SUM(c2)` (from the
aggregate expression, aliased to `total` by an outer `Projection` the
builder adds). Nothing in this tree says whether `SUM(c2)` will be computed
in one pass or as a two-phase partial/final aggregation across partitions —
that decision doesn't exist yet at the logical layer. It first appears when
[physical planning](./05-physical-planning.md) lowers this `Aggregate` node
into a concrete `AggregateExec`.

## Invariants and edge cases

- **A `LogicalPlan` node's `DFSchema` must always match what its expressions
  actually produce.** This is checked, not just assumed —
  `assert_expected_schema` exists specifically to catch a rewrite that
  changed output columns without updating the schema.
- **`Extension` nodes must implement their own schema and equality logic
  correctly**, or invariant checks and `TreeNode` traversal over them will
  silently do the wrong thing since the framework cannot infer either from
  an opaque extension.
- **`OuterReferenceColumn` only appears inside a subquery's `Expr` tree**,
  never at the top level of a non-nested plan — it exists specifically to
  represent a correlated reference to an outer query's column before
  decorrelation ([Chapter 4](./04-analyzer-optimizer.md)) resolves it.
- **UDF implementations are matched by trait object identity/equality
  (`DynEq`/`DynHash` on `ScalarUDFImpl`/`AggregateUDFImpl`/`WindowUDFImpl`),
  not just by name** — two UDFs with the same name but different
  implementations are not interchangeable inside a single plan.

## Trade-offs

Representing every expression form as a variant of one large `Expr` enum
(rather than, say, a trait-object tree of heterogeneous expression types)
makes exhaustive `match`-based reasoning possible and keeps `TreeNode`
traversal uniform — but it also means adding a genuinely new expression
*form* (not just a new function) touches a central, widely-matched enum.
DataFusion mitigates this by pushing as much variability as possible into
UDF traits (`ScalarUDFImpl`, `AggregateUDFImpl`, `WindowUDFImpl`) instead of
new `Expr` variants — a new function is a new UDF implementation, not a new
enum arm, and only genuinely new *syntax* needs a new variant.

## Key takeaways

- `Expr` is the logical expression tree; `LogicalPlan` is the relational
  operator tree that nests `Expr` trees inside it.
- `DFSchema` (in `datafusion-common`) adds relation-qualified name
  resolution on top of Arrow `Schema`, which plain Arrow schemas cannot do
  for self-joins or ambiguous column names.
- `LogicalPlanBuilder` computes each node's schema as a side effect of
  construction, so schema bookkeeping doesn't have to be redone by every
  caller.
- Functions are UDF trait objects wrapped in `Expr` variants, not new `Expr`
  variants themselves — this is the deliberate seam that keeps the enum from
  growing with every new function.
- `invariants.rs` makes plan consistency a checked contract, not a
  convention, so broken rewrites fail where they break, not three stages
  later.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 2 — SQL Parsing and Planning](./02-sql-planning.md)
- [Chapter 4 — Analyzer and Logical Optimizer](./04-analyzer-optimizer.md)
- [Chapter 8 — Aggregate Functions](./08-aggregate-functions.md)
- [Chapter 9 — Window Functions](./09-window-functions.md)
