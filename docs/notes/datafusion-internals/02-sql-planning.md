# Chapter 2 — SQL Parsing and Planning

## Why this exists

Before DataFusion can optimize or execute a query, it has to know what the
query *means*. SQL text arrives as an untyped, syntax-only AST from
sqlparser-rs — a `SELECT` node has no idea yet whether `c1` is a column of
table `t`, what type it has, or whether `SUM(c2)` is even legal in this
clause. The SQL planning layer's entire job is to close that gap: turn a
syntax tree into a [`LogicalPlan`](./03-logical-plans.md) that is faithful to
SQL semantics, so that later stages ([Chapter 4](./04-analyzer-optimizer.md))
can validate and improve it. It deliberately does *not* try to optimize —
that separation of concerns keeps this layer simple enough to reason about
clause by clause.

## The big picture

SQL planning is a translation, not a rewrite. Given `SELECT c1, SUM(c2) FROM
t WHERE c1 > 10 GROUP BY c1`, the planner does not decide whether to push the
filter into the scan or how to execute the aggregate — it just builds the
`LogicalPlan` tree that says "scan `t`, filter by `c1 > 10`, group by `c1`,
aggregate `SUM(c2)`" in the order SQL clauses imply. Correctness here means
preserving SQL's sometimes-surprising name-resolution rules: aliases defined
in `SELECT` are visible in `GROUP BY`/`HAVING`/`ORDER BY` but not in `WHERE`,
positional references like `GROUP BY 1` must resolve against the `SELECT`
list, and aggregate expressions can be scattered across `SELECT`, `HAVING`,
and `ORDER BY` but must all feed one `Aggregate` node.

## Core concepts

### `SqlToRel`: the planning coordinator

**`SqlToRel`** (`datafusion/sql/src/planner.rs`) is the central struct that
owns SQL-to-`LogicalPlan` translation. It is generic over a `ContextProvider`
— the trait through which the planner asks its embedder for table schemas,
registered functions, and configuration, without depending on a concrete
catalog implementation.

```rust
// datafusion/sql/src/planner.rs
pub struct SqlToRel<'a, S: ContextProvider> {
    pub(crate) context_provider: &'a S,
    pub(crate) options: ParserOptions,
    pub(crate) ident_normalizer: IdentNormalizer,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub fn new(context_provider: &'a S) -> Self;
    pub fn new_with_options(context_provider: &'a S, options: ParserOptions) -> Self;
    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema>;
}
```

### `PlannerContext`: state no single AST node can hold

A single sqlparser AST node cannot carry cross-node planning state — which
CTEs are in scope, what an outer query's schema looks like for a correlated
subquery, what type a `$1` placeholder should have. **`PlannerContext`**
(`datafusion/sql/src/planner.rs:257`) carries exactly that:

```rust
// datafusion/sql/src/planner.rs
pub struct PlannerContext {
    prepare_param_data_types: Arc<Vec<Option<FieldRef>>>,
    ctes: HashMap<String, Arc<LogicalPlan>>,
    outer_queries_schemas_stack: Vec<DFSchemaRef>,
    outer_from_schema: Option<DFSchemaRef>,
    create_table_schema: Option<DFSchemaRef>,
    set_expr_left_schema: Option<DFSchemaRef>,
    lambda_parameters: HashMap<String, FieldRef>,
}
```

This is why correlated subqueries and `WITH` clauses are planned correctly
even though sqlparser's AST has no notion of "the schema of the query I'm
nested inside of" — `PlannerContext` is threaded through every planning call
and pushed/popped as the planner descends into subqueries.

### `ExprPlanner` and `TypePlanner`: the extension points

SQL planning is extensible without forking `SqlToRel`. **`ExprPlanner`**
(`datafusion/expr/src/planner.rs:152`) lets an embedder intercept expression
forms — a custom binary operator, a custom `foo.bar` field-access
convention — before the default planning logic runs:

```rust
// datafusion/expr/src/planner.rs
pub trait ExprPlanner: Debug + Send + Sync {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>>;

    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>>;
}
```

Each hook returns a `PlannerResult` that is either `Original` (fall through
to default planning) or `Planned` (use this `Expr` instead). **`TypePlanner`**
(`datafusion/expr/src/planner.rs:439`) is the same idea for SQL type syntax
that doesn't map onto a built-in Arrow `DataType`.

## How it works: from statement to logical plan

- **`datafusion/sql/src/statement.rs`** dispatches top-level sqlparser
  `Statement` variants — separating query statements from DDL, DML, `COPY`,
  and `EXPLAIN` — and routes each to the planner path that produces a
  `LogicalPlan` or statement plan node.
- **`datafusion/sql/src/select.rs`** implements `SELECT` planning:
  `FROM`/joins, `WHERE`, `GROUP BY`, `HAVING`, projection, `DISTINCT`,
  `ORDER BY`, `LIMIT` — built in SQL clause order while preserving alias
  visibility and aggregate validation rules.
- **`datafusion/sql/src/expr/mod.rs`** recursively converts sqlparser
  expressions into `datafusion_expr::Expr` — identifiers, literals,
  operators, `CASE`, `BETWEEN`, casts, subqueries — delegating function calls
  to `expr/function.rs`, identifiers to `expr/identifier.rs`, and subqueries
  to `expr/subquery.rs`.
- **`datafusion/sql/src/expr/function.rs`** resolves SQL function calls into
  scalar, aggregate, window, or special-form expressions, handling argument
  normalization and `DISTINCT`/`ORDER BY` inside aggregate calls.
- **`datafusion/sql/src/expr/subquery.rs`** converts SQL subqueries into
  logical subquery expressions, which [Chapter 4](./04-analyzer-optimizer.md)
  later decorrelates into joins where possible.

The extension points in this layer are `ExprPlanner` and `TypePlanner`; use
them when SQL syntax should produce custom logical expressions or type
behavior without replacing the SQL planner outright.

## A worked example: `select_to_plan`

`select_to_plan` (`datafusion/sql/src/select.rs:76`) is the entry point for
planning a `SELECT`, and its handling of `HAVING`/`GROUP BY` aliasing shows
why this layer needs so much bookkeeping:

```rust
pub(super) fn select_to_plan(...) -> Result<LogicalPlan> {
    // FROM clause establishes the base schema
    let plan = self.plan_from_tables(select.from, planner_context)?;
    // WHERE clause
    let base_plan = self.plan_selection(select.selection, plan, planner_context)?;
    // SELECT expressions
    let select_exprs = self.prepare_select_exprs(&base_plan, select.projection, ...)?;

    // Build alias map so HAVING/GROUP BY can reference SELECT aliases.
    // SELECT MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10
    //   rewrites HAVING to: MAX(c2) > 10
    let alias_map = extract_aliases(&select_exprs);

    let having_expr_opt = select.having.map(|having_expr| {
        let having_expr = self.sql_expr_to_logical_expr(having_expr, &combined_schema, ...)?;
        let having_expr = resolve_aliases_to_exprs(having_expr, &alias_map)?;
        normalize_col(having_expr, &projected_plan)
    }).transpose()?;

    // GROUP BY: aliases from projection can conflict with input column names
    let group_by_exprs = exprs.into_iter().map(|e| {
        let group_by_expr = self.sql_expr_to_logical_expr(e, &combined_schema, ...)?;
        let mut alias_map = alias_map.clone();
        for f in base_plan.schema().fields() {
            alias_map.remove(f.name()); // input columns win over SELECT aliases
        }
        let group_by_expr = resolve_aliases_to_exprs(group_by_expr, &alias_map)?;
        let group_by_expr = resolve_positions_to_exprs(group_by_expr, &select_exprs)?; // GROUP BY 1
        normalize_col(group_by_expr, &projected_plan)
    }).collect::<Result<Vec<Expr>>>()?;

    // Aggregates are collected from SELECT, HAVING, QUALIFY, and ORDER BY
    // into one unified Aggregate node.
    let select_having_qualify_aggrs = find_aggregate_exprs(
        select_exprs.iter().chain(having_expr_opt.iter()).chain(qualify_expr_opt.iter()),
    );
    let order_by_aggrs = find_aggregate_exprs(order_by_rex.iter().map(|s| &s.expr));
    let mut aggr_exprs = select_having_qualify_aggrs;
    for order_by_aggr in order_by_aggrs {
        if !aggr_exprs.iter().any(|e| e == &order_by_aggr) {
            aggr_exprs.push(order_by_aggr);
        }
    }

    let result = if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
        self.aggregate(&base_plan, &select_exprs, having_expr_opt.as_ref(), ...)?
    } else if having_expr_opt.is_some() {
        return plan_err!("HAVING clause must appear in GROUP BY or aggregate function");
    } else {
        // ... no aggregation needed
    };
    // ... apply DISTINCT, ORDER BY, LIMIT
}
```

Three things make this dense: aliases must resolve *after* input columns of
the same name so a `SELECT` alias never shadows a real column; positional
`GROUP BY 1` must resolve against the already-planned `SELECT` list, not the
raw AST; and aggregates from four different clauses (`SELECT`, `HAVING`,
`QUALIFY`, `ORDER BY`) must be deduplicated into a single `Aggregate` node so
each aggregate function is computed exactly once regardless of how many
clauses reference it.

## Invariants and edge cases

- **`WHERE` cannot see `SELECT` aliases; `GROUP BY`/`HAVING`/`ORDER BY`
  can.** This mirrors standard SQL clause evaluation order and is why
  `plan_selection` runs before `alias_map` is built.
- **Input column names take priority over conflicting `SELECT` aliases in
  `GROUP BY`.** The alias map has base-plan field names explicitly removed
  before resolving `GROUP BY` expressions.
- **`HAVING` without `GROUP BY` and without aggregates is a planning
  error**, not a silently-accepted always-true/false filter.
- **Correlated subqueries need the outer query's schema *while planning the
  inner query*.** `PlannerContext::outer_queries_schemas_stack` exists
  because sqlparser's AST gives no other way to see "outward" during a
  recursive descent.

## Trade-offs

Keeping SQL planning free of optimization decisions means some plans built
here are deliberately non-optimal — a `WHERE` clause becomes a `Filter`
directly above its `TableScan` even when the table provider could evaluate
it during the scan. The alternative — optimizing while planning — would
tangle SQL-clause bookkeeping (alias maps, positional references,
aggregate collection) with cost-based decisions, making both harder to test
in isolation. DataFusion instead accepts a mechanically "obvious" first
`LogicalPlan` and relies entirely on the [analyzer and optimizer](./04-analyzer-optimizer.md)
to make it cheap — a clean handoff at the cost of an extra rewrite pass.

## Key takeaways

- SQL planning translates AST to `LogicalPlan` without optimizing; that is
  the analyzer/optimizer's job, not this layer's.
- `SqlToRel` coordinates planning; `PlannerContext` carries state — CTEs,
  outer schemas, parameter types — that no single AST node can hold.
- `ExprPlanner` and `TypePlanner` are the supported extension points for
  custom SQL expression or type behavior.
- `select_to_plan` is dense because SQL's clause semantics are dense:
  alias visibility, positional references, and multi-clause aggregate
  collection all have to agree.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 3 — Logical Expressions and Logical Plans](./03-logical-plans.md)
- [Chapter 4 — Analyzer and Logical Optimizer](./04-analyzer-optimizer.md)
