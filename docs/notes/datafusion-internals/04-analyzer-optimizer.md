# Chapter 4 — Analyzer and Logical Optimizer

## Why this exists

[SQL planning](./02-sql-planning.md) deliberately produces a mechanically
"obvious" `LogicalPlan` — a `WHERE` clause becomes a `Filter` directly above
a `TableScan`, even when pushing the predicate into the scan would be
cheaper. Two distinct passes turn that obvious plan into one that is both
*valid* and *cheap*: the analyzer, which makes a plan explicit and legal,
and the logical optimizer, which makes a legal plan cheaper without changing
what it means. Keeping these as separate passes with separate contracts is
what lets each individual rule stay small enough to review and test in
isolation.

## The big picture

The analyzer and the optimizer both transform `LogicalPlan` trees, but they
answer different questions. The analyzer asks "is this plan well-typed and
explicit?" — inserting casts, resolving grouping constructs, normalizing
function forms. The optimizer asks "is there a provably-equivalent plan
that's cheaper to execute?" — pushing filters toward scans, eliminating
redundant operators, decorrelating subqueries into joins. The analyzer
*must* run first: optimizer rules like type-sensitive constant folding
assume every expression already has a resolved, coerced type, which is
exactly what the analyzer guarantees.

Both passes share one crucial constraint: neither may change what a query
returns. The analyzer may turn an *invalid* plan into a *valid* one with the
same intended meaning (inserting a cast is not a semantic change — it's
making an implicit conversion explicit). The optimizer may only rewrite a
*valid* plan into another plan that is provably equivalent for every
possible input.

## Core concepts

### `OptimizerRule`: the shared contract for logical rewrites

**`OptimizerRule`** (`datafusion/optimizer/src/optimizer.rs:73`) is the
trait every logical rewrite implements — both true optimizations and (via a
parallel `AnalyzerRule` trait with the same shape) analyzer passes:

```rust
// datafusion/optimizer/src/optimizer.rs
pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;

    fn apply_order(&self) -> Option<ApplyOrder> {
        None // rule handles its own recursion
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>>;
}

pub struct Optimizer {
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}
```

Returning `Transformed<LogicalPlan>` rather than a bare `LogicalPlan` matters:
a rule reports whether it actually changed anything, which lets the
**`Optimizer`** driver skip unnecessary further work and detect when a batch
of rules has reached a fixed point. `apply_order` is an opt-in convenience —
if a rule returns `Some(ApplyOrder::TopDown)` or `BottomUp`, the driver
handles recursing through the plan tree for it; if it returns `None`, the
rule is trusted to recurse itself (needed for rules that must see multiple
levels of the tree at once, like subquery decorrelation).

### The analyzer's rules

`datafusion/optimizer/src/analyzer/mod.rs:72` defines **`Analyzer`**, which
sequences rules that must run before optimization:

- **`type_coercion.rs`** inserts casts and resolves compatible types so
  binary expressions, function arguments, `CASE` branches, and `UNION`
  inputs all have consistent, resolved types.
- **`resolve_grouping_function.rs`** resolves SQL grouping functions
  (`GROUPING`, `GROUPING_ID`) once aggregate semantics — which columns are
  actually grouped — are known.
- **`function_rewrite.rs`** normalizes function expressions that need
  planner-level rewriting before generic optimization, keeping special-case
  SQL function behavior out of lower execution code.

### The logical optimizer's rules

`datafusion/optimizer/src/optimizer.rs` is the driver; individual rule files
each own one rewrite:

| Rule file | What it rewrites |
|---|---|
| `push_down_filter.rs` | Moves filters toward scans across projections, joins, aggregates |
| `common_subexpr_eliminate.rs` | Fingerprints repeated expressions so they're computed once |
| `decorrelate_predicate_subquery.rs` | Rewrites correlated `IN`/`EXISTS` subqueries into joins |
| `scalar_subquery_to_join.rs` | Rewrites scalar subqueries into joins plus projections |
| `eliminate_cross_join.rs` | Converts cross join + predicate into inner join with a real key |
| `eliminate_outer_join.rs` | Simplifies outer joins when filters make unmatched rows impossible |
| `push_down_limit.rs` | Moves limits toward scans and through row-count-preserving operators |

Subquery decorrelation deserves special mention: SQL planning
([Chapter 2](./02-sql-planning.md)) preserves subqueries as logical
expressions because that's the faithful representation of the SQL AST. The
optimizer then converts as many of those subqueries as possible into joins,
aggregates, and projections — so [physical planning](./05-physical-planning.md)
can use ordinary join operators instead of evaluating a nested subquery
per outer row, which would be correct but far slower.

## How it works: rule sequencing and fixed points

The `Optimizer` driver applies rules in the sequence they're registered,
according to each rule's declared `apply_order`, and repeats batches of
rules until either a fixed point (no rule reports a change) or a configured
pass limit is reached. Most rule files share one shape: inspect a
`LogicalPlan` node, recursively rewrite children, transform expressions
where needed via `TreeNode`, and rebuild only the nodes that actually
changed — an unchanged subtree is left as the same `Arc`, which keeps
repeated optimizer passes cheap.

Rules must preserve schema and semantics as they rewrite. Pushing a
predicate through a `Projection`, for example, requires rewriting the
predicate's column references so it still refers to the correct *input*
columns of the projection rather than its (possibly renamed) output columns
— get this wrong and the rewritten plan is not just slower, it's incorrect.

## A worked example: `push_down_all_join`

**`push_down_all_join`** (`datafusion/optimizer/src/push_down_filter.rs:400`)
shows why join-aware filter pushdown is one of the optimizer's more
intricate rules: a predicate above a join has to be classified by which side
it references *and* whether the join type still lets it be pushed at all.

```rust
fn push_down_all_join(
    predicates: Vec<Expr>,
    inferred_join_predicates: Vec<Expr>,
    mut join: Join,
    on_filter: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    let is_inner_join = join.join_type == JoinType::Inner;
    let (left_preserved, right_preserved) = lr_is_preserved(join.join_type);

    // Each predicate is classified into one of three buckets:
    //  1) push through to the left or right child
    //  2) fold into the join condition itself (inner joins only)
    //  3) must stay as a Filter above the join
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
            join_conditions.push(predicate);
        } else {
            keep_predicates.push(predicate);
        }
    }

    // Predicates inferred from join-key equalities get the same treatment
    for predicate in inferred_join_predicates {
        if checker.is_left_only(&predicate) { left_push.push(predicate); }
        else if checker.is_right_only(&predicate) { right_push.push(predicate); }
    }

    // Even inside an OR, a pushable clause can be extracted:
    // (a < 20 AND a = c) OR (b > 10 AND b = d)
    //   → (a < 20) OR (b > 10) can push to the left side
    if left_preserved {
        left_push.extend(extract_or_clauses_for_join(&keep_predicates, &left_schema_columns));
    }
    if right_preserved {
        right_push.extend(extract_or_clauses_for_join(&keep_predicates, &right_schema_columns));
    }

    if let Some(predicate) = conjunction(left_push) {
        join.left = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.left)?));
    }
    if let Some(predicate) = conjunction(right_push) {
        join.right = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.right)?));
    }

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

`lr_is_preserved` (`push_down_filter.rs:168`) is what makes this
join-type-aware rather than join-shape-aware: for a `LEFT OUTER JOIN`, the
right side is *not* preserved, so a predicate referencing only right-side
columns cannot be pushed below the join — doing so would filter out the
`NULL`-padded rows outer join semantics require to survive. This is a rule
that would be silently wrong for outer joins if it treated every join like
an inner join.

## Invariants and edge cases

- **The analyzer must fully run before the logical optimizer.** Optimizer
  rules assume expressions are already type-coerced; running them first
  would mean reasoning about not-yet-legal expressions.
- **Filter pushdown through an outer join must respect which side is
  "preserved."** Predicates on the non-preserved side change join semantics
  if pushed below the join rather than kept above it or folded into the
  join condition.
- **A rule that returns `Transformed::no` must not have changed the plan.**
  The optimizer driver relies on this to detect a fixed point; a rule that
  silently mutates but reports "no change" causes reapplication to loop or
  to skip a needed later pass.
- **`Extension` logical nodes are opaque to built-in rules.** A rule that
  walks children generically will not reach inside a custom extension's
  private state — extensions that want optimizer participation must expose
  their internals through the normal `LogicalPlan`/`TreeNode` APIs.

## Trade-offs

Running rules to a fixed point (rather than a single fixed pass) means a
rewrite enabled by an *earlier* rule — say, filter pushdown exposing a new
opportunity for cross-join elimination — gets picked up without every rule
needing to anticipate every other rule's output. The cost is that pass count
is not statically bounded; a pass-limit configuration exists specifically to
cap pathological cases. Splitting decorrelation into two rules
(`decorrelate_predicate_subquery` for `IN`/`EXISTS`, `scalar_subquery_to_join`
for scalar subqueries) rather than one general subquery rule keeps each
rule's correctness argument tractable, at the cost of some shared logic
being duplicated between them.

## Key takeaways

- The analyzer makes a plan valid and explicit (casts, grouping resolution,
  function normalization); the optimizer makes a valid plan cheaper. The
  analyzer must run first.
- Every logical rewrite — analyzer or optimizer — implements `OptimizerRule`
  and must report accurately via `Transformed` whether it changed the plan.
- Subquery decorrelation converts SQL subquery syntax into joins so physical
  execution can use normal join operators instead of per-row subquery
  evaluation.
- Filter pushdown through joins must consult join-type-aware preservation
  rules (`lr_is_preserved`), not just which side of the join a predicate
  references.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 2 — SQL Parsing and Planning](./02-sql-planning.md)
- [Chapter 3 — Logical Expressions and Logical Plans](./03-logical-plans.md)
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md)
- [Chapter 7 — Physical Optimizer](./07-physical-optimizer.md)
