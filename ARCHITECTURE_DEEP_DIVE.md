# Apache DataFusion: Architecture Deep Dive and Learning Guide

This guide explains DataFusion's architecture through the files that implement
the main query-engine logic. It is intended for contributors who want to know
where behavior lives, how control moves between crates, and which interfaces are
stable extension points.

Each component section starts with reference code for the main interface or data
shape, followed by detailed explanation of the key files and the logic they own.

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Query Execution Pipeline](#2-query-execution-pipeline)
3. [Component Deep Dives](#3-component-deep-dives)
   - [SQL Parsing and Planning](#31-sql-parsing-and-planning)
   - [Logical Expressions and Logical Plans](#32-logical-expressions-and-logical-plans)
   - [Analyzer and Logical Optimizer](#33-analyzer-and-logical-optimizer)
   - [Physical Planning and Execution Plans](#34-physical-planning-and-execution-plans)
   - [Physical Expressions](#35-physical-expressions)
   - [Physical Optimizer](#36-physical-optimizer)
   - [Aggregate Functions](#37-aggregate-functions)
   - [Window Functions](#38-window-functions)
   - [Data Sources](#39-data-sources)
   - [Catalog System](#310-catalog-system)
   - [Execution Runtime](#311-execution-runtime)
   - [Session Management and DataFrame API](#312-session-management-and-dataframe-api)
4. [Crate Responsibilities](#4-crate-responsibilities)
5. [Extension Points Summary](#5-extension-points-summary)
6. [Learning Path Recommendations](#6-learning-path-recommendations)

## 1. Architecture Overview

DataFusion is a modular query engine built around Apache Arrow's columnar memory
format. SQL text and DataFrame API calls are converted into a logical plan,
validated, optimized, lowered into a physical execution plan, optimized again,
and finally executed as streams of Arrow `RecordBatch` values.

The architecture is split into many crates so that the public expression model,
optimizer rules, execution operators, runtime resources, table abstractions, and
user-facing APIs can evolve independently. Most important boundaries are traits:
`TableProvider` for tables, `FileFormat` and `FileSource` for files,
`OptimizerRule` for logical rewrites, `PhysicalOptimizerRule` for physical
rewrites, `PhysicalExpr` for executable expressions, and `ExecutionPlan` for
physical operators.

The core design rules are:

1. The logical layer describes what a query means without committing to a
   concrete execution algorithm.
2. The optimizer may rewrite plans only when the new plan is semantically
   equivalent or when the analyzer is making an invalid plan valid.
3. The physical layer describes how data is partitioned, ordered, streamed, and
   evaluated.
4. Execution is pull-based: each physical operator returns a stream for one
   partition, and downstream operators poll upstream streams.
5. Runtime resources such as memory pools, disk spill locations, object stores,
   and function registries are accessed through session and task contexts.

## 2. Query Execution Pipeline

1. A user submits SQL through `SessionContext::sql` or builds a query with the
   DataFrame API. SQL starts in `datafusion-sql`; DataFrame calls usually build
   `LogicalPlan` nodes directly through `datafusion-expr`.
2. SQL text is parsed by sqlparser-rs into an AST. DataFusion-specific parser
   options and statement handling live in `datafusion/sql/src`.
3. `SqlToRel` converts the sqlparser AST into `LogicalPlan` and `Expr` values.
   During this step, table names are resolved through the catalog, SQL
   expressions are converted into logical expressions, and SELECT clauses become
   logical operators such as scan, filter, aggregate, sort, and projection.
4. The analyzer runs semantic normalization and validation. It inserts casts,
   resolves grouping constructs, rewrites function forms, and checks invariants
   that later stages rely on.
5. The logical optimizer repeatedly applies rule-based rewrites. Rules push
   filters and projections closer to scans, simplify expressions, decorrelate
   subqueries, remove redundant operators, and choose more efficient equivalent
   logical forms.
6. The physical planner lowers each logical node into an `ExecutionPlan`. This
   chooses concrete operators such as `FilterExec`, `ProjectionExec`,
   `AggregateExec`, `HashJoinExec`, or file scan execution plans.
7. The physical optimizer rewrites the physical plan to satisfy distribution,
   ordering, and execution constraints. It may insert repartitioning, enforce or
   remove sorts, choose join strategies, push limits, and validate the final
   plan.
8. Execution starts when a caller collects or streams a DataFrame. Each
   `ExecutionPlan::execute` call produces a `SendableRecordBatchStream` for a
   partition. Operators evaluate physical expressions against incoming
   `RecordBatch` values and emit transformed batches.
9. The caller receives Arrow `RecordBatch` results. The same physical plan may be
   executed across multiple partitions and Tokio tasks, while memory and spill
   behavior are coordinated by `TaskContext` and `RuntimeEnv`.

| Stage | Main representation | Main owning crates |
|-------|---------------------|--------------------|
| Parse SQL | sqlparser AST | `datafusion-sql` |
| Plan SQL | `LogicalPlan`, `Expr` | `datafusion-sql`, `datafusion-expr` |
| Analyze | validated `LogicalPlan` | `datafusion-optimizer` |
| Logical optimize | optimized `LogicalPlan` | `datafusion-optimizer` |
| Physical plan | `Arc<dyn ExecutionPlan>` | `datafusion-core`, `datafusion-physical-plan` |
| Physical optimize | optimized `ExecutionPlan` tree | `datafusion-physical-optimizer` |
| Execute | `SendableRecordBatchStream` | `datafusion-physical-plan`, `datafusion-execution` |

## 3. Component Deep Dives

### 3.1 SQL Parsing and Planning

The SQL layer translates sqlparser-rs AST nodes into DataFusion's logical model.
Its main job is not optimization; it creates a faithful logical representation
that later analyzer and optimizer stages can validate and improve.

#### Reference code

The SQL planner is coordinated by `SqlToRel` and carries cross-node planning
state in `PlannerContext`. Planner extensions use `ExprPlanner` and
`TypePlanner`.

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

pub trait TypePlanner: Debug + Send + Sync {
    fn plan_type_field(
        &self,
        sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<FieldRef>>;
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/sql/src/planner.rs` | Defines `SqlToRel`, `PlannerContext`, parser/planner options, and the high-level AST-to-plan entry points. `SqlToRel` owns name resolution, catalog access, relation planning, expression planning delegation, CTE tracking, and statement-to-logical-plan orchestration. It is the central coordinator for SQL planning. |
| `datafusion/sql/src/statement.rs` | Dispatches top-level SQL statements. It separates query statements from DDL, DML, COPY, EXPLAIN, and other statement families. Its main logic is routing each sqlparser `Statement` variant to the planner path that can produce a `LogicalPlan` or statement plan node. |
| `datafusion/sql/src/select.rs` | Implements SELECT planning. It handles FROM items, joins, WHERE, GROUP BY, HAVING, SELECT projection, DISTINCT, ORDER BY, LIMIT, and related clauses. It builds the logical operator stack in SQL clause order while preserving SQL semantics such as alias visibility and aggregate validation. |
| `datafusion/sql/src/expr/mod.rs` | Converts sqlparser expressions into `datafusion_expr::Expr`. It owns the recursive conversion for identifiers, literals, unary and binary operators, CASE, BETWEEN, casts, subqueries, grouping expressions, and function calls delegated to specialized modules. |
| `datafusion/sql/src/expr/function.rs` | Resolves SQL function calls into scalar, aggregate, window, or special-form logical expressions. It handles function argument normalization, DISTINCT and ORDER BY inside aggregate calls, window function syntax, and planner hooks for extension-provided functions. |
| `datafusion/sql/src/expr/identifier.rs` | Converts SQL identifiers into DataFusion columns, compound identifiers, wildcard references, or qualified names. This is where many user-visible name-resolution details start before schema validation finishes them. |
| `datafusion/sql/src/expr/subquery.rs` | Converts SQL subqueries into logical subquery expressions. The resulting `Expr` variants are later decorrelated or planned as subquery execution nodes. |

#### Implementation Example

The `select_to_plan()` method in `datafusion/sql/src/select.rs` (lines 76-294)
shows the full complexity of SQL SELECT planning, including alias resolution,
aggregate detection, and the interaction between HAVING, GROUP BY, and ORDER BY:

```rust
pub(super) fn select_to_plan(...) -> Result<LogicalPlan> {
    // Process `from` clause - establishes base schema
    let plan = self.plan_from_tables(select.from, planner_context)?;

    // Process `where` clause
    let base_plan = self.plan_selection(select.selection, plan, planner_context)?;

    // Process the SELECT expressions
    let select_exprs = self.prepare_select_exprs(&base_plan, select.projection, ...)?;

    // Build alias map for HAVING/GROUP BY to reference SELECT aliases
    // Example: SELECT MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10
    // rewrites to: HAVING MAX(c2) > 10
    let alias_map = extract_aliases(&select_exprs);

    let having_expr_opt = select.having.map(|having_expr| {
        let having_expr = self.sql_expr_to_logical_expr(having_expr, &combined_schema, ...)?;
        // Dereference aliases in HAVING clause
        let having_expr = resolve_aliases_to_exprs(having_expr, &alias_map)?;
        normalize_col(having_expr, &projected_plan)
    }).transpose()?;

    // GROUP BY expressions - aliases from projection can conflict with input columns
    let group_by_exprs = exprs.into_iter().map(|e| {
        let group_by_expr = self.sql_expr_to_logical_expr(e, &combined_schema, ...)?;
        // Remove aliases that conflict with base plan column names
        let mut alias_map = alias_map.clone();
        for f in base_plan.schema().fields() {
            alias_map.remove(f.name());
        }
        let group_by_expr = resolve_aliases_to_exprs(group_by_expr, &alias_map)?;
        // Handle positional references like GROUP BY 1, 2
        let group_by_expr = resolve_positions_to_exprs(group_by_expr, &select_exprs)?;
        normalize_col(group_by_expr, &projected_plan)
    }).collect::<Result<Vec<Expr>>>()?;

    // Collect aggregates from SELECT, HAVING, QUALIFY, and ORDER BY
    let select_having_qualify_aggrs = find_aggregate_exprs(
        select_exprs.iter().chain(having_expr_opt.iter()).chain(qualify_expr_opt.iter()),
    );
    let order_by_aggrs = find_aggregate_exprs(order_by_rex.iter().map(|s| &s.expr));

    // Combine aggregates, avoiding duplicates
    let mut aggr_exprs = select_having_qualify_aggrs;
    for order_by_aggr in order_by_aggrs {
        if !aggr_exprs.iter().any(|e| e == &order_by_aggr) {
            aggr_exprs.push(order_by_aggr);
        }
    }

    // Build aggregate plan if GROUP BY or aggregates present
    let result = if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
        self.aggregate(&base_plan, &select_exprs, having_expr_opt.as_ref(), ...)?
    } else {
        // HAVING without GROUP BY is an error
        if having_expr_opt.is_some() {
            return plan_err!("HAVING clause must appear in GROUP BY or aggregate function");
        }
        // ...
    };
    // ... apply DISTINCT, ORDER BY, LIMIT
}
```

This demonstrates the intricate interplay between SQL clauses: alias maps enable
HAVING/GROUP BY to reference SELECT aliases while avoiding conflicts with input
columns. Positional references (`GROUP BY 1`) are resolved against SELECT
expressions. Aggregates are collected from multiple clauses to build a unified
aggregate node that computes all needed values.

The main SELECT logic begins when `statement.rs` identifies a query statement
and calls into SELECT planning. `select.rs` first plans table references from the
FROM clause, producing scan or join logical nodes. It then adds filters from the
WHERE clause, builds aggregate nodes when GROUP BY or aggregate functions are
present, applies HAVING, projects SELECT expressions, and finally applies
distinctness, ordering, and limits. Expression conversion is delegated to
`expr/mod.rs`, so clause-level planning stays separate from expression syntax.

`PlannerContext` carries state that cannot be represented by a single AST node:
CTEs, outer query references, generated aliases, parameter data types, and
planning scopes. This state is important for correlated subqueries and for SQL
features where names are visible only in specific clauses.

The extension points in this layer are `ExprPlanner` and `TypePlanner` from
`datafusion-expr`, plus parser and planner configuration. Custom planners use
these hooks when SQL syntax should produce custom logical expressions or custom
type behavior without replacing the entire SQL planner.

### 3.2 Logical Expressions and Logical Plans

The logical layer is DataFusion's semantic model. It represents query meaning in
a way that is independent of physical execution details such as partition counts,
join algorithms, or memory strategy.

#### Reference code

`Expr` is the logical expression tree. `LogicalPlan` is the relational operator
tree that contains those expressions.

```rust
// datafusion/expr/src/expr.rs
pub enum Expr {
    Alias(Alias),
    Column(Column),
    ScalarVariable(FieldRef, Vec<String>),
    Literal(ScalarValue, Option<FieldMetadata>),
    BinaryExpr(BinaryExpr),
    Like(Like),
    SimilarTo(Like),
    Not(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsNull(Box<Expr>),
    IsTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    IsUnknown(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsNotFalse(Box<Expr>),
    IsNotUnknown(Box<Expr>),
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
    SetComparison(SetComparison),
    ScalarSubquery(Subquery),
    Wildcard {
        qualifier: Option<TableReference>,
        options: Box<WildcardOptions>,
    },
    GroupingSet(GroupingSet),
    Placeholder(Placeholder),
    OuterReferenceColumn(FieldRef, Column),
    Unnest(Unnest),
    HigherOrderFunction(HigherOrderFunction),
    Lambda(Lambda),
    LambdaVariable(LambdaVariable),
}

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
    Statement(Statement),
    Values(Values),
    Explain(Explain),
    Analyze(Analyze),
    Extension(Extension),
    Distinct(Distinct),
    Dml(DmlStatement),
    Ddl(DdlStatement),
    Copy(CopyTo),
    DescribeTable(DescribeTable),
    Unnest(Unnest),
    RecursiveQuery(RecursiveQuery),
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/expr/src/expr.rs` | Defines the `Expr` enum and expression data structures. It represents columns, literals, binary expressions, casts, scalar functions, aggregate functions, window functions, subqueries, wildcards, aliases, and other logical expression forms. It also contains helper methods used by planners and optimizers to inspect, rewrite, and display expressions. |
| `datafusion/expr/src/expr_schema.rs` | Implements type and nullability inference for expressions. Its logic answers questions such as what data type an expression returns, whether the result can be null, and whether an expression is valid against a `DFSchema`. Analyzer and optimizer rules depend on these answers before rewriting plans. |
| `datafusion/expr/src/logical_plan/plan.rs` | Defines the `LogicalPlan` enum and logical operator structs such as scan, projection, filter, aggregate, join, sort, limit, union, distinct, subquery alias, DML, and extension nodes. This file is the primary definition of the logical plan tree. |
| `datafusion/expr/src/logical_plan/builder.rs` | Provides `LogicalPlanBuilder`, a fluent API for constructing logical plans. The SQL planner, DataFrame API, examples, and tests use this to add projection, filter, aggregate, join, sort, and limit nodes while preserving schema updates. |
| `datafusion/expr/src/logical_plan/invariants.rs` | Defines checks that ensure a logical plan remains internally consistent. These checks catch mismatched schemas, invalid expression references, and plan forms that later stages are not expected to handle. |
| `datafusion/expr/src/udf.rs` | Defines scalar UDF wrappers and implementation traits. It records signatures, return-type behavior, volatility, documentation, and how logical scalar function calls are represented. |
| `datafusion/expr/src/udaf.rs` | Defines aggregate UDF wrappers and implementation traits. It describes accumulator construction, state fields, return types, aliases, and aggregate-specific metadata. |
| `datafusion/expr/src/udwf.rs` | Defines window UDF wrappers and implementation traits. It provides the logical function metadata needed before physical window evaluators are built. |
| `datafusion/expr/src/table_source.rs` | Defines the logical table abstraction used during planning. SQL planning can work with table metadata without knowing the physical scan implementation. |

`Expr` values form trees inside `LogicalPlan` nodes. A projection contains output
expressions, a filter contains a predicate expression, an aggregate contains
group expressions and aggregate expressions, and a join contains join key and
filter expressions. Optimizer rules use the `TreeNode` traversal APIs to visit
and transform these nested expressions without hard-coding every possible tree
shape.

`DFSchema` extends Arrow schemas with relation qualifiers and logical field
metadata. This matters because SQL name resolution must distinguish columns with
the same field name from different tables. The expression schema logic is also
where nullability and type coercion assumptions become concrete enough for
physical planning.

`LogicalPlanBuilder` is a convenience API, but it also centralizes schema
construction. When a builder method adds a projection or aggregate, it computes
the output schema that downstream nodes will see. This keeps SQL, DataFrame, and
test code from each manually constructing logical plan internals.

### 3.3 Analyzer and Logical Optimizer

The analyzer and optimizer both transform logical plans, but they have different
contracts. Analyzer rules make plans valid and explicit. Optimizer rules preserve
semantics while trying to make plans cheaper to execute.

#### Reference code

Logical optimizer rules identify themselves, declare how they should be applied,
and return whether a plan was transformed.

```rust
// datafusion/optimizer/src/optimizer.rs
pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>>;
}

pub struct Optimizer {
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl Optimizer {
    pub fn new() -> Self;
    pub fn optimize(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
        observer: impl FnMut(&LogicalPlan, &dyn OptimizerRule),
    ) -> Result<LogicalPlan>;
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/optimizer/src/analyzer/mod.rs` | Defines the analyzer framework and applies analyzer rules. It owns rule sequencing for semantic rewrites that must happen before optimization, such as type coercion and function normalization. |
| `datafusion/optimizer/src/analyzer/type_coercion.rs` | Inserts casts and resolves compatible data types for expressions. This logic ensures binary expressions, function arguments, CASE branches, UNION inputs, and similar constructs have consistent types. |
| `datafusion/optimizer/src/analyzer/resolve_grouping_function.rs` | Resolves SQL grouping functions after aggregate semantics are known. It rewrites grouping-specific expressions into forms later stages can plan. |
| `datafusion/optimizer/src/analyzer/function_rewrite.rs` | Normalizes function expressions that need planner-level rewriting before normal optimization. This keeps special SQL function behavior out of lower execution code. |
| `datafusion/optimizer/src/optimizer.rs` | Defines the logical optimizer driver, rule trait integration, apply order, fixed-point behavior, and optimizer configuration access. It is the main file to read to understand when and how rules are invoked. |
| `datafusion/optimizer/src/push_down_filter.rs` | Moves filters toward scans when doing so is semantically valid. It splits predicates, analyzes which expressions can cross projections, joins, aggregates, and other nodes, and exposes more predicates to table providers for pushdown. |
| `datafusion/optimizer/src/common_subexpr_eliminate.rs` | Detects repeated expressions and rewrites plans so repeated computations can be evaluated once. Its main logic is expression fingerprinting and projection rewriting. |
| `datafusion/optimizer/src/decorrelate_predicate_subquery.rs` | Rewrites correlated `IN`, `EXISTS`, and related predicate subqueries into join-like logical plans when possible. This allows the physical planner to use normal join machinery instead of nested per-row subquery evaluation. |
| `datafusion/optimizer/src/scalar_subquery_to_join.rs` | Rewrites scalar subqueries into joins plus projection expressions when the query semantics allow it. This is important for converting SQL subquery syntax into relational operators. |
| `datafusion/optimizer/src/eliminate_cross_join.rs` | Converts cross joins plus predicates into inner joins when predicates identify join keys. This gives later stages a real join condition that can use hash or sort-merge join algorithms. |
| `datafusion/optimizer/src/eliminate_outer_join.rs` | Simplifies outer joins when filters make unmatched rows impossible. This can turn outer joins into inner joins or reduce the required join type. |
| `datafusion/optimizer/src/push_down_limit.rs` | Moves limits closer to data sources and through operators where row-count semantics remain correct. This can reduce the amount of data read or sorted. |

#### Implementation Example

The `push_down_all_join()` function in `datafusion/optimizer/src/push_down_filter.rs`
(lines 399-521) shows the complexity of pushing filters through joins, where
predicates must be classified by which side they reference and whether they can
become join conditions:

```rust
fn push_down_all_join(
    predicates: Vec<Expr>,
    inferred_join_predicates: Vec<Expr>,
    mut join: Join,
    on_filter: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    let is_inner_join = join.join_type == JoinType::Inner;
    let (left_preserved, right_preserved) = lr_is_preserved(join.join_type);

    // Predicates are classified into three categories:
    // 1) Can push through join to left or right child
    // 2) Can become join conditions (inner join only)
    // 3) Must be kept as filter above the join
    let left_schema_columns = schema_columns(join.left.schema().as_ref());
    let right_schema_columns = schema_columns(join.right.schema().as_ref());

    let mut left_push = vec![];
    let mut right_push = vec![];
    let mut keep_predicates = vec![];
    let mut join_conditions = vec![];
    let mut checker = ColumnChecker::new(left_schema, right_schema);

    for predicate in predicates {
        if left_preserved && checker.is_left_only(&predicate) {
            left_push.push(predicate);
        } else if right_preserved && checker.is_right_only(&predicate) {
            right_push.push(predicate);
        } else if is_inner_join && can_evaluate_as_join_condition(&predicate)? {
            // Convert to join condition - ExtractEquijoinPredicate will
            // later extract equi-predicates for hash/merge join
            join_conditions.push(predicate);
        } else {
            keep_predicates.push(predicate);
        }
    }

    // Push inferred predicates (derived from join keys) to appropriate sides
    for predicate in inferred_join_predicates {
        if checker.is_left_only(&predicate) {
            left_push.push(predicate);
        } else if checker.is_right_only(&predicate) {
            right_push.push(predicate);
        }
    }

    // Extract pushable clauses from OR expressions
    // Example: (a < 20 AND a = c) OR (b > 10 AND b = d)
    // Can extract (a < 20) OR (b > 10) to push to left side
    if left_preserved {
        left_push.extend(extract_or_clauses_for_join(&keep_predicates, &left_schema_columns));
    }
    if right_preserved {
        right_push.extend(extract_or_clauses_for_join(&keep_predicates, &right_schema_columns));
    }

    // Insert filters below join
    if let Some(predicate) = conjunction(left_push) {
        join.left = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.left)?));
    }
    if let Some(predicate) = conjunction(right_push) {
        join.right = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.right)?));
    }

    // Remaining predicates become join filter or stay above
    join.filter = conjunction(join_conditions);
    let plan = LogicalPlan::Join(join);
    let plan = if let Some(predicate) = conjunction(keep_predicates) {
        LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(plan))?)
    } else {
        plan
    };
    Ok(Transformed::yes(plan))
}
```

This demonstrates join-aware filter pushdown: predicates are analyzed to
determine which join side(s) they reference using `ColumnChecker`. The function
respects join semantics (`left_preserved`/`right_preserved` depend on join type),
converts eligible predicates to join conditions for inner joins, and extracts
pushable clauses from complex OR expressions. Inferred predicates (derived from
join key equalities) are also pushed down.

The optimizer driver applies rules according to each rule's declared traversal
order and repeats rule batches until a fixed point or configured pass limit is
reached. Each rule returns whether it changed the plan, which lets the optimizer
avoid unnecessary additional work.

Most rule files follow the same pattern: inspect a `LogicalPlan`, recursively
rewrite children, transform expressions when needed, and rebuild only the nodes
that changed. Rules must preserve schema and semantics. When a rule pushes a
predicate through a projection, for example, it must rewrite column references so
the predicate still refers to the correct input fields.

Subquery decorrelation is one of the most important optimizer responsibilities.
The SQL planner preserves subqueries as logical expressions because that is the
direct representation of the SQL AST. Optimizer rules then convert many of those
subqueries into joins, aggregates, and projections so the physical planner can
use normal relational execution operators.

### 3.4 Physical Planning and Execution Plans

Physical planning chooses concrete execution operators for a logical plan. It is
the boundary where DataFusion moves from query meaning to executable algorithms.

#### Reference code

Every physical operator implements `ExecutionPlan`. The trait exposes output
properties, child plans, physical expressions, child replacement, and partitioned
execution.

```rust
// datafusion/physical-plan/src/execution_plan.rs
pub trait ExecutionPlan: Any + Debug + DisplayAs + Send + Sync {
    fn name(&self) -> &str;

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().schema())
    }

    fn properties(&self) -> &Arc<PlanProperties>;

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool>;

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion>;

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

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/core/src/physical_planner.rs` | Defines the default physical query planner. It recursively lowers `LogicalPlan` nodes to `ExecutionPlan` nodes, creates physical expressions, chooses built-in physical operators, plans scans through table providers, and handles custom logical extension planning hooks. |
| `datafusion/physical-plan/src/execution_plan.rs` | Defines the `ExecutionPlan` trait, `PlanProperties`, execution modes, partitioning/order properties, and child replacement APIs. Every physical operator implements this trait. |
| `datafusion/physical-plan/src/filter.rs` | Implements `FilterExec`. Its main logic evaluates a boolean `PhysicalExpr` against each input batch and uses Arrow filter kernels to keep matching rows. |
| `datafusion/physical-plan/src/projection.rs` | Implements `ProjectionExec`. It evaluates a list of physical expressions against input batches and builds output batches with the projected schema. |
| `datafusion/physical-plan/src/aggregates/mod.rs` | Defines aggregate execution structures and shared aggregate planning support. It coordinates grouping expressions, aggregate expressions, aggregate modes, accumulator state, and output construction. |
| `datafusion/physical-plan/src/aggregates/no_grouping.rs` | Handles aggregation without GROUP BY keys. This path can keep one accumulator set per partition and produce one output row for global aggregates. |
| `datafusion/physical-plan/src/aggregates/group_values/mod.rs` | Implements group-key storage used by grouped aggregation. It maps incoming group key values to group indexes so accumulators can update the correct state. |
| `datafusion/physical-plan/src/windows/window_agg_exec.rs` | Implements full window aggregation execution when the input partition must be evaluated with window partitioning and ordering semantics. |
| `datafusion/physical-plan/src/windows/bounded_window_agg_exec.rs` | Implements bounded or streaming-friendly window execution for window functions that do not require the entire partition before producing output. |
| `datafusion/physical-plan/src/memory.rs` | Implements in-memory table execution. It is the physical operator used for registered memory batches and many tests. |
| `datafusion/physical-plan/src/stream.rs` | Defines stream helpers and `RecordBatchStream` plumbing used by execution operators. |
| `datafusion/physical-plan/src/metrics.rs` | Defines metric collection for physical execution. Operators use this to report elapsed time, output rows, memory, spills, and other runtime counters. |

#### Implementation Example

The `FilterExecStream::poll_next()` method in `datafusion/physical-plan/src/filter.rs`
(lines 1012-1090) shows the actual filtering logic with predicate evaluation,
batch coalescing, and early termination on limit:

```rust
impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.metrics.baseline_metrics.elapsed_compute().clone();
        loop {
            // Return completed batch if ready
            if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                self.metrics.selectivity.add_part(batch.num_rows());
                return self.metrics.baseline_metrics.record_poll(Poll::Ready(Some(Ok(batch))));
            }

            if self.batch_coalescer.is_finished() {
                return Poll::Ready(None);
            }

            // Pull next batch from input stream
            match ready!(self.input.poll_next_unpin(cx)) {
                None => {
                    self.batch_coalescer.finish()?;
                    // Release input pipeline resources early
                    self.input = Box::pin(EmptyRecordBatchStream::new(self.input.schema()));
                }
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();

                    // Evaluate predicate, apply projection, filter rows
                    let status = self.predicate.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                        .and_then(|(array, batch)| {
                            match as_boolean_array(&array) {
                                Ok(filter_array) => {
                                    self.metrics.selectivity.add_total(batch.num_rows());
                                    let batch = filter_record_batch(&batch, filter_array)?;
                                    self.batch_coalescer.push_batch(batch)
                                }
                                Err(_) => internal_err!("Non-boolean predicate")
                            }
                        })?;
                    timer.done();

                    match status {
                        PushBatchStatus::Continue => { /* keep pulling */ }
                        PushBatchStatus::LimitReached => {
                            // Stop early when fetch limit reached
                            self.batch_coalescer.finish()?;
                            self.input = Box::pin(EmptyRecordBatchStream::new(self.input.schema()));
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
```

This demonstrates the streaming execution model: `poll_next()` implements async
stream polling using Rust's `Future`/`Poll` pattern. Key aspects include: (1)
batch coalescing to combine small filtered batches into larger output batches,
(2) predicate evaluation via `PhysicalExpr::evaluate()` returning a boolean
array, (3) Arrow's `filter_record_batch()` kernel to select matching rows, (4)
early termination when `LimitReached` by releasing the input stream, and (5)
metrics tracking for selectivity and compute time.

The central `ExecutionPlan` method is `execute(partition, context)`. The
partition argument tells an operator which output partition to produce. Leaf
operators read only the data for that partition. Unary operators call
`execute` on their child for the same or derived partition and transform the
stream. Operators that change distribution, such as repartitioning or
coalescing, coordinate multiple child streams.

Physical planning must also compute plan properties. `PlanProperties` describe
the output partitioning, ordering, equivalence properties, emission type, and
boundedness. Physical optimizer rules use these properties to decide whether a
plan already satisfies a requirement or whether extra operators must be inserted.

Extension logical nodes are handled through custom physical planners. This lets
projects add new logical operators while still returning normal
`Arc<dyn ExecutionPlan>` values to the rest of DataFusion.

### 3.5 Physical Expressions

Physical expressions evaluate logical expression semantics against actual Arrow
arrays. They are used inside filters, projections, joins, aggregates, window
operators, sorts, and file-pruning logic.

#### Reference code

`PhysicalExpr` is the runtime expression interface. It reports its output type
and evaluates against a `RecordBatch`.

```rust
// datafusion/physical-expr-common/src/physical_expr.rs
pub trait PhysicalExpr:
    Any + Send + Sync + Display + Debug + DynEq + DynHash
{
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field(input_schema)?.data_type().to_owned())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef>;

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue>;

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

// datafusion/expr-common/src/columnar_value.rs
pub enum ColumnarValue {
    Array(ArrayRef),
    Scalar(ScalarValue),
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/physical-expr/src/physical_expr.rs` | Defines the `PhysicalExpr` trait. Implementations report data type and nullability, evaluate against a `RecordBatch`, expose child expressions, and support child replacement for optimizer rewrites. |
| `datafusion/physical-expr/src/planner.rs` | Converts logical `Expr` values into executable `PhysicalExpr` trees. It resolves input column indexes, creates function expressions, plans casts, handles subqueries, and applies physical expression planner extensions. |
| `datafusion/physical-expr/src/expressions/column.rs` | Implements column lookup by schema index. During evaluation it returns the referenced input array without recomputing values. |
| `datafusion/physical-expr/src/expressions/literal.rs` | Implements scalar literal expressions. Literals usually evaluate to `ColumnarValue::Scalar`, which can be broadcast by callers instead of materializing a full array eagerly. |
| `datafusion/physical-expr/src/expressions/binary.rs` | Implements binary arithmetic, comparison, and boolean expressions. It dispatches to Arrow kernels and handles scalar-versus-array combinations through `ColumnarValue`. |
| `datafusion/physical-expr/src/expressions/cast.rs` | Implements runtime casts between Arrow data types. It contains the execution-side behavior for casts inserted by the analyzer or written by the user. |
| `datafusion/physical-expr/src/scalar_function.rs` | Implements scalar function expression evaluation. It evaluates arguments, invokes scalar UDF implementations, and returns array or scalar results. |
| `datafusion/physical-expr/src/aggregate.rs` | Defines physical aggregate expression support. It bridges logical aggregate calls to accumulator construction and aggregate state management used by physical aggregate operators. |
| `datafusion/physical-expr/src/window/window_expr.rs` | Defines physical window expression interfaces used by window execution operators. It connects logical window calls to partition evaluators. |
| `datafusion/expr-common/src/columnar_value.rs` | Defines `ColumnarValue`, the shared representation for either an Arrow array or a scalar value. Physical expression evaluation relies on this to avoid unnecessary materialization. |

The main physical expression path starts in the physical planner. For each
logical expression, `create_physical_expr` resolves field references against the
input physical schema and returns an executable tree. At runtime, operators pass
incoming `RecordBatch` values to `PhysicalExpr::evaluate`. The result may be an
array with one value per row or a scalar that represents the same value for all
rows in the batch.

This array-or-scalar distinction is important for performance. A predicate like
`a > 5` evaluates the column `a` as an array and `5` as a scalar. The binary
expression can combine them without first allocating an array full of fives.

### 3.6 Physical Optimizer

The physical optimizer rewrites executable plans while respecting physical
properties such as partitioning, ordering, boundedness, and emission behavior.
It is responsible for making the plan executable under each operator's input
requirements.

#### Reference code

Physical optimizer rules take an executable plan and return an executable plan.
They may use only configuration or richer optimizer context.

```rust
// datafusion/physical-optimizer/src/optimizer.rs
pub trait PhysicalOptimizerRule: Debug + std::any::Any {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize(plan, context.config_options())
    }

    fn name(&self) -> &str;

    fn schema_check(&self) -> bool;
}

pub struct PhysicalOptimizer {
    pub rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/physical-optimizer/src/optimizer.rs` | Defines the physical optimizer driver and `PhysicalOptimizerRule` integration. It applies rules in configured order and passes optimizer configuration into rule logic. |
| `datafusion/physical-optimizer/src/enforce_distribution.rs` | Inserts repartitioning and coalescing operators when child output partitioning does not satisfy a parent's required input distribution. It is central to parallel execution correctness. |
| `datafusion/physical-optimizer/src/join_selection.rs` | Chooses join implementations and build/probe sides using join type, ordering, partitioning, and available statistics. It can replace a generic join choice with hash join, sort-merge join, or other suitable physical forms. |
| `datafusion/physical-optimizer/src/output_requirements.rs` | Tracks ordering and distribution requirements propagated through a physical plan. It supports rules that must reason about whether child properties satisfy parent requirements. |
| `datafusion/physical-optimizer/src/limit_pushdown.rs` | Pushes local and global limits toward scans and expensive operators where row-count semantics remain valid. |
| `datafusion/physical-optimizer/src/projection_pushdown.rs` | Pushes physical projections closer to scans and removes unused columns from execution where possible. |
| `datafusion/physical-optimizer/src/pushdown_sort.rs` | Pushes sort requirements toward data sources or through compatible operators. It helps avoid unnecessary full sorts when an input can already provide ordering. |
| `datafusion/physical-optimizer/src/topk_aggregation.rs` | Rewrites eligible aggregation plus ordering plus limit patterns into more efficient top-k aggregation forms. |
| `datafusion/physical-optimizer/src/sanity_checker.rs` | Validates the final physical plan. It catches property mismatches and invalid physical structures before execution starts. |
| `datafusion/physical-optimizer/src/ensure_coop.rs` | Adds cooperative scheduling wrappers where needed so long-running execution remains friendly to Tokio scheduling. |

Physical optimizer rules depend heavily on `ExecutionPlan` properties. For
example, a join may require hash-partitioned inputs on join keys. If the child
plans do not provide that distribution, `enforce_distribution.rs` inserts the
needed repartition operators. Similarly, sorting rules check whether existing
ordering is already strong enough before adding a `SortExec`.

Join selection is both a correctness and performance rule. It must preserve join
semantics, but it can choose which side becomes the build side for hash join or
whether sorted inputs allow sort-merge join. The choice influences memory use,
parallelism, and spill behavior.

### 3.7 Aggregate Functions

Aggregate support is split between logical function definitions, physical
aggregate expressions, and execution operators. This separation allows the same
SQL aggregate to be planned, optimized, and executed in partial and final phases.

#### Reference code

Aggregate UDFs define metadata and accumulator construction. Accumulators hold
the runtime state used during partial, final, and single-phase aggregation.

```rust
// datafusion/expr/src/udaf.rs
pub trait AggregateUDFImpl:
    Debug + DynEq + DynHash + Send + Sync + Any
{
    fn name(&self) -> &str;
    fn aliases(&self) -> &[String] { &[] }
    fn signature(&self) -> &Signature;
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef>;
    fn is_nullable(&self) -> bool { true }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>>;

    fn state_fields(
        &self,
        args: StateFieldsArgs,
    ) -> Result<Vec<FieldRef>>;

    fn groups_accumulator_supported(
        &self,
        args: AccumulatorArgs,
    ) -> bool;

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>>;
}

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

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/expr/src/udaf.rs` | Defines logical aggregate UDF metadata. It records function signature, return type logic, state field definitions, accumulator construction, aliases, and documentation. |
| `datafusion/functions-aggregate/src/planner.rs` | Registers and resolves built-in aggregate functions during expression planning. It maps function names to aggregate UDF implementations. |
| `datafusion/functions-aggregate/src/count.rs` | Implements COUNT variants, including count-all and count-expression behavior. Its main logic tracks non-null counts and supports accumulator state merging. |
| `datafusion/functions-aggregate/src/sum.rs` | Implements SUM for supported numeric and decimal types. It handles return-type selection, overflow-sensitive behavior, null handling, and state merging. |
| `datafusion/functions-aggregate/src/average.rs` | Implements AVG using state that tracks sum and count. Final evaluation divides merged state to produce the correct average. |
| `datafusion/functions-aggregate/src/min_max.rs` | Implements MIN and MAX. It updates accumulator state by comparing incoming values and preserves SQL null behavior. |
| `datafusion/functions-aggregate/src/first_last.rs` | Implements FIRST_VALUE and LAST_VALUE aggregate variants. It handles order-sensitive state and null-treatment options. |
| `datafusion/functions-aggregate-common/src/aggregate.rs` | Provides shared aggregate helpers and traits used by aggregate implementations. It reduces duplication across built-in aggregate files. |
| `datafusion/physical-expr/src/aggregate.rs` | Converts aggregate UDF calls into physical aggregate expressions that can create accumulators and expose state fields to physical aggregate execution. |
| `datafusion/physical-plan/src/aggregates/mod.rs` | Coordinates physical aggregate execution across modes such as partial, final, and single-phase aggregation. |

#### Implementation Example

The `group_aggregate_batch()` method in `datafusion/physical-plan/src/aggregates/row_hash.rs`
(lines 919-1015) shows the core grouped aggregation algorithm that maps input
rows to groups and updates accumulators:

```rust
impl GroupedHashAggregateStream {
    /// Perform group-by aggregation for the given [`RecordBatch`].
    fn group_aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Evaluate the grouping expressions (e.g., columns in GROUP BY)
        let group_by_values = evaluate_group_by(&self.group_by, batch)?;

        // Evaluate the aggregation expressions (e.g., the column in SUM(col))
        let input_values = evaluate_many(&self.aggregate_arguments, batch)?;

        // Evaluate filter expressions for filtered aggregates like COUNT(*) FILTER (WHERE x > 0)
        let filter_values = evaluate_optional(&self.filter_expressions, batch)?;

        for group_values in &group_by_values {
            // Intern group keys and get group indices for each row
            // group_values is an array of group key values
            // current_group_indices maps each row to its group index
            let starting_num_groups = self.group_values.len();
            self.group_values.intern(group_values, &mut self.current_group_indices)?;
            let group_indices = &self.current_group_indices;

            // Update ordering information for streaming emission of completed groups
            let total_num_groups = self.group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering.new_groups(group_values, group_indices, total_num_groups)?;
            }

            // Update each accumulator with the batch data and group assignments
            let t = self.accumulators.iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                // Partial aggregation: update with raw input values
                if self.mode.input_mode() == AggregateInputMode::Raw {
                    acc.update_batch(values, group_indices, opt_filter, total_num_groups)?;
                } else {
                    // Final aggregation: merge partial states from other partitions
                    acc.merge_batch(values, group_indices, None, total_num_groups)?;
                }
            }
        }
        Ok(())
    }
}
```

This demonstrates the hash aggregation algorithm: (1) `group_values.intern()`
maps group key arrays to integer group indices using an internal hash table,
(2) `group_ordering` tracks which groups can be emitted early when input is
sorted, (3) `GroupsAccumulator::update_batch()` updates accumulator state for
all rows in a batch at once using vectorized operations with the group indices
array, and (4) the mode determines whether to aggregate raw values (partial) or
merge pre-aggregated states (final). This batched, vectorized approach is much
faster than per-row accumulation.

The main aggregate logic is state-based. Each aggregate implementation creates
an `Accumulator` or grouped accumulator. `update_batch` consumes input arrays and
updates state. `state` serializes intermediate state for partial aggregation.
`merge_batch` combines states from other partitions. `evaluate` produces the
final scalar result.

Partial aggregation lets each input partition reduce many rows into compact
state. Final aggregation merges those states after repartitioning by group keys
or coalescing data as required. This is why every aggregate must define not only
its final return type but also its intermediate state fields.

Grouped aggregation adds group-key management. The physical aggregate operator
uses group-value storage to assign each input row to a group index, then updates
the accumulator state for that group. No-grouping aggregation uses a simpler
single state per aggregate expression.

### 3.8 Window Functions

Window functions compute values over partitions of rows while preserving row
cardinality. They are sensitive to partitioning, ordering, frame bounds, and
whether a function can produce results before seeing the whole partition.

#### Reference code

Window UDFs create partition evaluators. Evaluators describe whether a function
can stream, whether it needs frame boundaries, and how to compute output arrays.

```rust
// datafusion/expr/src/udwf.rs
pub trait WindowUDFImpl:
    Debug + DynEq + DynHash + Send + Sync + Any
{
    fn name(&self) -> &str;
    fn aliases(&self) -> &[String] { &[] }
    fn signature(&self) -> &Signature;

    fn expressions(
        &self,
        expr_args: ExpressionArgs,
    ) -> Vec<Arc<dyn PhysicalExpr>>;

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>>;
}

// datafusion/expr/src/partition_evaluator.rs
pub trait PartitionEvaluator: Debug + Send + std::any::Any {
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()>;
    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>>;
    fn is_causal(&self) -> bool { false }

    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef>;

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue>;

    fn uses_window_frame(&self) -> bool;
    fn supports_bounded_execution(&self) -> bool { false }
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/expr/src/udwf.rs` | Defines logical window UDF metadata and construction. It represents SQL window functions before they are lowered into physical evaluators. |
| `datafusion/expr/src/window_frame.rs` | Defines window frame units and bounds. This logic represents SQL frame clauses such as ROWS, RANGE, and GROUPS in a planner-independent form. |
| `datafusion/expr/src/partition_evaluator.rs` | Defines evaluator interfaces for functions that compute over a partition or frame. Window implementations use this to expose batch and per-frame evaluation behavior. |
| `datafusion/functions-window/src/planner.rs` | Registers and resolves built-in window functions during planning. It maps SQL function names such as rank, lag, and row_number to implementations. |
| `datafusion/functions-window-common/src/expr.rs` | Defines common window expression helpers shared by window function crates and physical planning. |
| `datafusion/functions-window-common/src/partition.rs` | Provides partition-related helpers used by window evaluators. |
| `datafusion/physical-expr/src/window/window_expr.rs` | Defines physical window expression behavior. It connects logical window definitions to executable partition evaluators. |
| `datafusion/physical-expr/src/window/standard.rs` | Implements standard ranking and navigation window expression support. |
| `datafusion/physical-expr/src/window/sliding_aggregate.rs` | Supports aggregate-style window evaluation over sliding frames, where state can be updated as frame boundaries move. |
| `datafusion/physical-plan/src/windows/window_agg_exec.rs` | Executes full window aggregation. It evaluates partitions according to partition keys, ordering, and frame semantics. |
| `datafusion/physical-plan/src/windows/bounded_window_agg_exec.rs` | Executes window functions that support bounded or streaming-friendly behavior. |

Window planning starts with logical expressions that capture the function,
arguments, partition keys, order keys, and frame. Physical planning then creates
window expressions and a window execution operator. The physical optimizer must
ensure that the input ordering and distribution are compatible with the window
specification.

The evaluator API exposes capability methods such as whether a function is
causal, whether it uses the window frame, and whether it supports bounded
execution. These flags are important because some functions, such as
`row_number`, can stream through ordered input, while others may need complete
partition state.

### 3.9 Data Sources

Data source code bridges logical table references and physical scan execution.
It covers table providers, file formats, file grouping, schema adaptation,
statistics, object store paths, and scan sources.

#### Reference code

Tables create scan plans. File formats provide format-specific schema,
statistics, source, reader, and writer behavior.

```rust
// datafusion/catalog/src/table.rs
pub trait TableProvider: Any + Debug + Sync + Send {
    fn schema(&self) -> SchemaRef;
    fn constraints(&self) -> Option<&Constraints> { None }
    fn table_type(&self) -> TableType;
    fn get_table_definition(&self) -> Option<&str> { None }
    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> { None }
    fn get_column_default(&self, column: &str) -> Option<&Expr> { None }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>>;
}

// datafusion/datasource/src/file_format.rs
pub trait FileFormat: Any + Send + Sync + fmt::Debug {
    fn get_ext(&self) -> String;
    fn compression_type(&self) -> Option<FileCompressionType>;

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics>;

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource>;
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/catalog/src/table.rs` | Defines `TableProvider`, the central table abstraction. Its `scan` method creates a physical plan for projected columns, filters, and limits; its filter-pushdown method tells the optimizer which predicates can be handled by the provider. |
| `datafusion/datasource/src/file_format.rs` | Defines `FileFormat`, the abstraction for format-specific schema inference, statistics inference, and physical plan creation. CSV, JSON, Avro, Arrow, and Parquet implementations plug in here. |
| `datafusion/datasource/src/file.rs` | Defines file scan configuration and file-related data source structures. It carries object store URLs, file groups, projected schemas, statistics, limits, and table partition columns into physical planning. |
| `datafusion/datasource/src/source.rs` | Defines lower-level source abstractions used by file scans. It separates scan source behavior from higher-level table registration. |
| `datafusion/datasource/src/file_groups.rs` | Groups files into partitions for parallel scanning. Its logic affects how many physical scan partitions are produced and which files each partition reads. |
| `datafusion/datasource/src/statistics.rs` | Defines statistics structures and helpers used for optimizer decisions and pruning. |
| `datafusion/datasource/src/schema_adapter.rs` | Adapts file schemas to table schemas. This handles cases where physical files have compatible but not identical field layouts or types. |
| `datafusion/core/src/datasource/listing/table.rs` | Implements listing-table behavior for object-store-backed file tables. It discovers files, applies listing options, and delegates format-specific planning to `FileFormat`. |
| `datafusion/core/src/datasource/file_format/parquet.rs` | Wires Parquet format options into core table registration paths. |
| `datafusion/datasource-parquet/src/file_format.rs` | Implements Parquet-specific planning, schema/statistics handling, pruning support, and creation of Parquet scan execution. |
| `datafusion/datasource-csv/src/file_format.rs` | Implements CSV-specific schema inference and scan planning. |
| `datafusion/datasource-json/src/file_format.rs` | Implements JSON and NDJSON-specific schema inference and scan planning. |

The main scan path starts when a logical table scan is physically planned.
DataFusion calls `TableProvider::scan` with projection, filters, and limit
information. A file-backed provider turns those inputs into a file scan
configuration, asks the `FileFormat` to create a physical plan, and returns that
plan to the physical planner.

Filter pushdown is negotiated rather than assumed. The optimizer asks a table
provider whether each predicate is exact, inexact, unsupported, or otherwise
pushable. Exact pushdown means DataFusion does not need to reapply the filter
above the scan. Inexact pushdown means the source may reduce data but the filter
must still be evaluated later for correctness.

For Parquet, pruning can use row group statistics, page indexes, bloom filters,
and row-level predicates during decode. These optimizations live behind the same
provider and file-format abstractions, so SQL and logical planning do not need
Parquet-specific branches.

### 3.10 Catalog System

The catalog system stores metadata for tables, schemas, catalogs, views, streams,
and information schema. It is the layer SQL planning uses to resolve names into
table providers.

#### Reference code

Catalogs contain schemas, and schemas contain tables. Planning walks these
interfaces to resolve SQL table names.

```rust
// datafusion/catalog/src/catalog.rs
pub trait CatalogProvider: Any + Debug + Sync + Send {
    fn as_any(&self) -> &dyn Any;
    fn schema_names(&self) -> Vec<String>;
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>>;

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>>;
}

// datafusion/catalog/src/schema.rs
pub trait SchemaProvider: Any + Debug + Sync + Send {
    fn as_any(&self) -> &dyn Any;
    fn table_names(&self) -> Vec<String>;

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>>;

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/catalog/src/catalog.rs` | Defines catalog provider traits and in-memory catalog implementations. Catalogs contain schemas and are addressed by catalog name. |
| `datafusion/catalog/src/schema.rs` | Defines `SchemaProvider` and schema implementations. Schemas own table registration, lookup, deregistration, and table-name listing. |
| `datafusion/catalog/src/table.rs` | Defines `TableProvider` and table metadata behavior. The same trait is used by memory tables, listing tables, views, and custom external table providers. |
| `datafusion/catalog/src/listing_schema.rs` | Implements schema behavior for discovering tables from object store listings. This supports dynamic table discovery instead of only explicit registration. |
| `datafusion/catalog/src/information_schema.rs` | Implements SQL-standard metadata views such as tables, columns, and routines. It exposes catalog metadata as queryable tables. |
| `datafusion/catalog/src/view.rs` | Defines view table providers. Views store logical SQL or logical plans and expose them through table-like interfaces. |
| `datafusion/catalog/src/default_table_source.rs` | Bridges catalog table providers into logical table sources used by planning. |

Name resolution moves from catalog name to schema name to table name. If a query
uses an unqualified table name, session configuration determines default catalog
and schema lookup. Once a `TableProvider` is found, planning can ask for its
schema and later physical planning can ask it to produce a scan plan.

The catalog layer is intentionally trait-based. Applications can replace the
in-memory catalog with one backed by an external metastore, remote service, or
dynamic discovery mechanism while keeping the SQL planner and physical planner
on the same interfaces.

### 3.11 Execution Runtime

The execution runtime owns resources that physical operators need while running:
memory pools, temporary disk locations, object stores, caches, task contexts,
and executor integration.

#### Reference code

The runtime environment stores shared execution resources. Memory pools account
for operator allocations and enforce memory policy.

```rust
// datafusion/execution/src/runtime_env.rs
pub struct RuntimeEnv {
    pub memory_pool: Arc<dyn MemoryPool>,
    pub disk_manager: Arc<DiskManager>,
    pub cache_manager: Arc<CacheManager>,
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
}

// datafusion/execution/src/memory_pool/mod.rs
pub trait MemoryPool: Any + Send + Sync + std::fmt::Debug + Display {
    fn name(&self) -> &str;
    fn register(&self, consumer: &MemoryConsumer) {}
    fn unregister(&self, consumer: &MemoryConsumer) {}
    fn grow(&self, reservation: &MemoryReservation, additional: usize);
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize);
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()>;
    fn reserved(&self) -> usize;
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Unknown
    }
}

pub enum MemoryLimit {
    Infinite,
    Finite(usize),
    Unknown,
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/execution/src/runtime_env.rs` | Defines `RuntimeEnv` and its builder. It stores process-level execution resources such as the memory pool, disk manager, cache manager, and object store registry. |
| `datafusion/execution/src/memory_pool/mod.rs` | Defines memory pool traits and implementations. Memory pools arbitrate reservations and enforce memory limits across operators. |
| `datafusion/execution/src/memory_pool/pool.rs` | Implements built-in memory pool policies such as `UnboundedMemoryPool`, `GreedyMemoryPool`, `FairSpillPool`, and `TrackConsumersPool`. It owns the allocation strategies used when operators request or release memory. |
| `datafusion/execution/src/disk_manager.rs` | Manages temporary files used by spilling operators. It controls where spill files are created and cleaned up. |
| `datafusion/execution/src/object_store.rs` | Registers and resolves object stores used by file scans. File data sources depend on this to read local files, cloud object stores, and other storage backends through one interface. |
| `datafusion/execution/src/task.rs` | Defines task-level execution context used by physical operators. It carries session configuration, runtime resources, and function registries into `ExecutionPlan::execute`. |

#### Implementation Example

The `GroupedHashAggregateStream::spill()` method in
`datafusion/physical-plan/src/aggregates/row_hash.rs` (lines 1237-1298) shows
how operators interact with memory pools during spilling:

```rust
impl GroupedHashAggregateStream {
    /// Emit all intermediate aggregation states, sort them, and store them on disk.
    fn spill(&mut self) -> Result<()> {
        // Emit intermediate state as a RecordBatch
        let Some(emit) = self.emit(EmitTo::All, true)? else {
            return Ok(());
        };

        // Free accumulated state BEFORE reserving sort memory
        // This gives the pool room for the sort operation
        self.clear_shrink(0);
        self.update_memory_reservation()?;

        // Calculate memory needed for sorting (worst case: 2X buffer size)
        let batch_size_ratio = self.batch_size as f32 / emit.num_rows() as f32;
        let batch_memory = get_record_batch_memory_size(&emit);
        let sort_memory = (batch_memory
            + (emit.get_sliced_size()? as f32 * batch_size_ratio) as usize)
            .min(batch_memory * 2);

        // Reserve memory for sort - if this fails, we cannot spill
        self.reservation.try_grow(sort_memory).map_err(|err| {
            resources_datafusion_err!(
                "Failed to reserve memory for sort during spill: {err}"
            )
        })?;

        // Sort and write to disk
        let sorted_iter = IncrementalSortIterator::new(
            emit,
            self.spill_state.spill_expr.clone(),
            self.batch_size,
        );
        let spillfile = self.spill_state.spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "HashAggSpill",
            )?;

        // Release sort memory now that sorting is complete
        self.reservation.shrink(sort_memory);

        // Track spill file for later merge
        if let Some((spillfile, max_batch_memory)) = spillfile {
            self.spill_state.spills.push(SortedSpillFile {
                file: spillfile,
                max_record_batch_memory: max_batch_memory,
            });
        }
        Ok(())
    }
}
```

This demonstrates the complete spill cycle: (1) emit in-memory state to a batch,
(2) release hash table memory to make room for sorting, (3) reserve temporary
memory for the sort operation via `try_grow()`, (4) sort data by group keys and
write to disk through `SpillManager`, (5) release sort memory via `shrink()`,
and (6) track the spill file for later streaming merge. The careful memory
accounting ensures the pool stays within limits while still completing the spill.

Operators request memory through `MemoryConsumer` reservations. A sort or hash
aggregate can grow its reservation while building in-memory state. If the memory
pool rejects growth and the operator can spill, the operator writes intermediate
state to a temporary file through the disk manager, shrinks its reservation, and
continues with less memory pressure.

`RuntimeEnv` is shared across sessions when desired, while task contexts are
created for individual query execution tasks. This split lets multiple sessions
share object stores and memory limits while still carrying query-specific
configuration and function registries.

### 3.12 Session Management and DataFrame API

Session code is the user-facing orchestration layer. It connects configuration,
catalogs, function registries, planners, optimizers, runtime resources, SQL, and
the DataFrame API.

#### Reference code

`SessionContext` is the user entry point. `SessionState` stores the planners,
optimizers, catalogs, functions, configuration, and runtime resources used by
that context.

```rust
// datafusion/core/src/execution/context/mod.rs
pub struct SessionContext {
    session_id: String,
    session_start_time: DateTime<Utc>,
    state: Arc<RwLock<SessionState>>,
}

impl SessionContext {
    pub fn new() -> Self;
    pub fn new_with_config(config: SessionConfig) -> Self;
    pub async fn sql(&self, sql: &str) -> Result<DataFrame>;
    pub fn register_table(
        &self,
        table_ref: impl Into<TableReference>,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
    pub fn register_udf(&self, udf: ScalarUDF);
    pub fn register_udaf(&self, udaf: AggregateUDF);
    pub fn register_udwf(&self, udwf: WindowUDF);
}

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
}
```

#### Key files and main logic

| File | Main logic |
|------|------------|
| `datafusion/session/src/session.rs` | Defines session traits and session-level abstractions shared across core planning and execution code. It allows lower crates to depend on session behavior without depending on all of `datafusion-core`. |
| `datafusion/core/src/execution/context/mod.rs` | Defines `SessionContext`, the primary user-facing API. It owns high-level methods for SQL, table registration, file reading, UDF registration, and DataFrame creation. |
| `datafusion/core/src/execution/session_state.rs` | Defines `SessionState`, which stores planners, optimizers, catalog lists, runtime environment, configuration, and function registries. It is the main query-planning state object. |
| `datafusion/core/src/dataframe/mod.rs` | Defines the DataFrame API and lazy query-building methods. DataFrame operations create or transform logical plans until execution is requested. |
| `datafusion/core/src/dataframe/parquet.rs` | Adds DataFrame write/read behavior related to Parquet. It demonstrates how high-level APIs delegate to data source and execution layers. |
| `datafusion/core/src/lib.rs` | Re-exports core user-facing types and documents common entry points. It is the crate boundary most applications import. |
| `datafusion/core/src/prelude.rs` | Provides convenient imports for typical application code, including expression constructors and context types. |

`SessionContext::sql` parses and plans SQL using the session state's catalog and
configuration. DataFrame methods usually avoid SQL parsing and directly build a
new logical plan from the previous one. Both routes converge on the same
analyzer, optimizer, physical planner, and physical optimizer before execution.

`SessionState` is immutable-by-convention through builder-style updates and
shared through `Arc` where needed. This makes it possible to create derived
states with additional rules, functions, or configuration while sharing common
runtime resources.

Execution is lazy. A DataFrame can be transformed many times without reading
data. Work begins when the caller asks to collect, stream, explain, write, or
otherwise execute the plan.

## 4. Crate Responsibilities

Foundational dependencies such as Arrow, sqlparser-rs, Tokio, and object_store
provide columnar memory, SQL parsing, async execution, and storage access. They
are external building blocks rather than DataFusion-specific query logic.

`datafusion-common`, `datafusion-expr-common`, and
`datafusion-physical-expr-common` provide shared errors, tree traversal,
operators, scalar values, statistics, sort properties, accumulator traits, and
physical expression helpers. These crates avoid dependency cycles by holding
types needed by multiple higher-level crates.

`datafusion-expr` owns logical query representation. SQL planning, DataFrame
planning, analyzers, optimizers, and physical planners all depend on its `Expr`
and `LogicalPlan` definitions.

`datafusion-sql` owns SQL-specific translation. It depends on sqlparser-rs and
`datafusion-expr`, but it does not execute queries itself.

`datafusion-optimizer` owns logical analysis and optimization. It rewrites
`LogicalPlan` trees and depends on expression semantics, schemas, and optimizer
configuration.

`datafusion-physical-expr` owns executable expression trees. It is the bridge
between logical expressions and Arrow array evaluation.

`datafusion-physical-plan` owns executable operators and streams. It defines
`ExecutionPlan` and implements the operators used during query execution.

`datafusion-physical-optimizer` owns rewrites over executable plans. It enforces
physical requirements and improves operator choices after physical planning.

`datafusion-datasource` and format-specific datasource crates own scan
abstractions, file grouping, format logic, pruning, and schema adaptation.

`datafusion-catalog` owns metadata traits and catalog implementations. Planning
uses it to resolve table names into table providers.

`datafusion-execution` owns runtime resources. It does not define query plans;
it provides the environment those plans use while running.

`datafusion-session` and `datafusion-core` connect the layers into user-facing
APIs. `datafusion-core` provides `SessionContext`, DataFrame APIs, table
registration helpers, and the default physical planner.

Function crates such as `datafusion-functions`,
`datafusion-functions-aggregate`, `datafusion-functions-window`, and
`datafusion-functions-nested` register built-in function implementations while
using the common UDF traits from `datafusion-expr`.

## 5. Extension Points Summary

| Extension type | Main trait or API | Where it plugs in |
|----------------|-------------------|-------------------|
| Custom table | `TableProvider` | Register with `SessionContext::register_table` or a custom catalog. |
| Custom file format | `FileFormat` and scan source types | Register through listing/table configuration and session state. |
| Custom catalog | `CatalogProvider` and `SchemaProvider` | Register with session catalog APIs. |
| Scalar function | `ScalarUDF` and `ScalarUDFImpl` | Register with `SessionContext::register_udf`. |
| Aggregate function | `AggregateUDF` and `AggregateUDFImpl` | Register with `SessionContext::register_udaf`. |
| Window function | `WindowUDF` and `WindowUDFImpl` | Register with `SessionContext::register_udwf`. |
| SQL expression planning | `ExprPlanner` | Configure planner extensions for custom expression lowering. |
| Logical optimizer rule | `OptimizerRule` | Add to session state optimizer configuration. |
| Physical optimizer rule | `PhysicalOptimizerRule` | Add through session state builder or custom optimizer setup. |
| Physical expression | `PhysicalExpr` | Return from a physical expression planner or custom physical operator. |
| Physical operator | `ExecutionPlan` | Return from a custom physical planner or table provider scan. |
| Runtime resource policy | `RuntimeEnvBuilder`, `MemoryPool` | Configure the runtime environment used by sessions. |

## 6. Learning Path Recommendations

### Beginner: Query Construction and Results

Start with `datafusion/core/src/execution/context/mod.rs`,
`datafusion/core/src/dataframe/mod.rs`, and the examples in
`datafusion-examples/`. Learn how SQL and DataFrame calls create lazy plans and
how `collect` or `execute_stream` triggers execution.

Then read `datafusion/expr/src/logical_plan/plan.rs` and
`datafusion/expr/src/expr.rs`. These files define the structures that most of
the rest of the engine transforms.

### Intermediate: Planning and Optimization

Read `datafusion/sql/src/select.rs` and `datafusion/sql/src/expr/mod.rs` to
understand how SQL becomes logical plans. Then read
`datafusion/optimizer/src/optimizer.rs` and a small optimizer rule such as
`eliminate_filter.rs` before moving to larger rules such as
`push_down_filter.rs` or `decorrelate_predicate_subquery.rs`.

After that, read `datafusion/core/src/physical_planner.rs` and
`datafusion/physical-plan/src/execution_plan.rs`. These files explain how
logical plans become executable operators.

### Advanced: Execution, Sources, and Extensibility

Read representative physical operators such as
`datafusion/physical-plan/src/filter.rs`,
`datafusion/physical-plan/src/projection.rs`, and
`datafusion/physical-plan/src/aggregates/mod.rs`. Pair those with
`datafusion/physical-expr/src/planner.rs` and
`datafusion/physical-expr/src/physical_expr.rs` to understand runtime
expression evaluation.

For data source work, start with `datafusion/catalog/src/table.rs`,
`datafusion/datasource/src/file_format.rs`, and
`datafusion/core/src/datasource/listing/table.rs`. For runtime work, read
`datafusion/execution/src/runtime_env.rs` and the memory pool implementations.

## Additional Resources

- API documentation: https://docs.rs/datafusion
- Project website: https://datafusion.apache.org
- Contributor architecture guide: `docs/source/contributor-guide/architecture.md`
- Optimizer rule reference: `datafusion/core/src/optimizer_rule_reference.md`
- Examples: `datafusion-examples/`

This guide is a contributor-oriented map of the codebase. For exact behavior,
the source files listed above are the authoritative reference.
