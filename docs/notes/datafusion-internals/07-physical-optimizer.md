# Chapter 7 — Physical Optimizer

## Why this exists

[Chapter 5](./05-physical-planning.md) built an `ExecutionPlan` tree but
deliberately left it possibly *unrunnable*: a join whose children aren't
partitioned on the join key, a window function whose input isn't sorted, a
scan reading more columns than are actually needed downstream. The physical
optimizer is the pass that closes those gaps — and, along the way, picks
better algorithms than the physical planner's default choices. Where the
logical optimizer ([Chapter 4](./04-analyzer-optimizer.md)) can only reason
about relational semantics, the physical optimizer reasons about
partitioning, ordering, and cost — the things that were explicitly
out-of-scope for the logical layer.

## The big picture

`PhysicalOptimizer` runs a fixed, ordered list of `PhysicalOptimizerRule`s
over the tree exactly once each, in a specific sequence — unlike the
logical optimizer, there is no repeated fixed-point loop by default. Order
matters enormously here: a rule that changes join implementation
(`JoinSelection`) has to run before the rule that adds repartitioning
(`EnforceDistribution`), because the chosen join algorithm determines what
distribution is required in the first place.

```
ExecutionPlan (from physical planner, possibly unrunnable)
    │  OutputRequirements (add) → AggregateStatistics → JoinSelection
    │  → LimitedDistinctAggregation → FilterPushdown
    │  → EnforceDistribution → CombinePartialFinalAggregate
    │  → EnforceSorting → OptimizeAggregateOrder → WindowTopN
    │  → ProjectionPushdown → OutputRequirements (remove)
    │  → TopKAggregation → LimitPushPastWindows → HashJoinBuffering
    │  → LimitPushdown → TopKRepartition → ProjectionPushdown
    │  → PushdownSort → EnsureCooperative
    │  → FilterPushdown (post-optimization) → SanityCheckPlan
    ▼
ExecutionPlan (runnable: distribution/ordering requirements satisfied)
```

The last rule, `SanityCheckPlan`, changes nothing — it is a pure gatekeeper
that rejects a plan it cannot certify as runnable, so every earlier rule's
mistakes surface as one clear error instead of a confusing runtime panic.

## Core concepts

### `PhysicalOptimizerRule`

Defined in `datafusion/physical-optimizer/src/optimizer.rs:96`:

```rust
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
```

`schema_check` is a small but important signal: most rules must not change a
plan's output schema, and the framework validates that automatically. A
rule that legitimately changes nullability (for example, a rule that proves
a filtered column can no longer be null) returns `false` here to opt out of
that validation rather than being forced to work around it.

`PhysicalOptimizer` (`datafusion/physical-optimizer/src/optimizer.rs`) is
just `{ rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> }` plus a
`new()` constructor that returns the recommended rule list in the order
shown above. `SessionState::add_physical_optimizer_rule` is how a project
adds its own rule to that list without replacing the built-in ones.

### The rules that matter most

- **`JoinSelection`** (`join_selection.rs`) — chooses concrete join
  implementations (which side builds a hash table, whether a broadcast join
  applies) using join type, ordering, partitioning, and statistics, and
  handles plans with unbounded/infinite sources. It runs early because its
  choice changes what distribution and ordering later rules must enforce.
- **`EnforceDistribution`** (`enforce_distribution.rs`) — inserts
  `RepartitionExec`/coalescing operators wherever a child's output
  `Partitioning` does not satisfy its parent's `required_input_distribution`.
  This is the rule that actually makes parallel, correct grouped
  aggregation and joins possible ([Chapter 8](./08-aggregate-functions.md)).
- **`EnforceSorting`** — the ordering analog of `EnforceDistribution`:
  inserts `SortExec` nodes wherever a child's ordering doesn't satisfy a
  parent's `required_input_ordering`, and removes redundant sorts where an
  input is already sufficiently ordered.
- **`ProjectionPushdown`** (runs twice, at different points) — pushes
  projections toward sources, merges consecutive projections, and can make
  a projection disappear entirely once it reaches a source that can apply
  it itself. Narrowing columns early reduces the row width every downstream
  operator (especially joins) has to move through memory.
- **`TopKAggregation`** / **`WindowTopN`** / **`TopKRepartition`** — detect
  `ORDER BY ... LIMIT k` patterns above an aggregation or window function
  and rewrite them into bounded top-k execution, so the operator only needs
  to track `k` rows or groups instead of materializing the full result
  before sorting and truncating.
- **`SanityCheckPlan`** (`sanity_checker.rs`) — the final gate. It rejects
  plans that use a pipeline-breaking operator (one that must see all of its
  input before producing output, like a full sort or hash-build) on an
  unbounded source, and verifies every node's order/distribution
  requirements are actually satisfied. It makes no rewrites; a `SanityCheckPlan`
  failure means an earlier rule left the plan unrunnable.

## How it works: satisfying a requirement

Every rule that inserts operators to satisfy a requirement follows the same
shape: read the child's `PlanProperties` (partitioning, ordering,
equivalence — [Chapter 5](./05-physical-planning.md)), read the parent's
declared requirement (`required_input_distribution` or
`required_input_ordering`), and compare. If the child already satisfies the
requirement, nothing is inserted — this is why `EnforceDistribution` runs
*after* `JoinSelection`: if the join's own physical properties already
happen to satisfy the requirement (e.g. a broadcast join needs no specific
distribution on its probe side), no repartitioning operator is added at
all, and the plan stays cheaper.

When the child does *not* satisfy the requirement, the rule wraps the child
in the appropriate operator — `RepartitionExec::new(child, target_partitioning)`
for distribution, `SortExec::new(sort_exprs, child)` for ordering — and
replaces the child pointer via `ExecutionPlan::with_new_children`
([Chapter 5](./05-physical-planning.md)), preserving everything else about
the parent.

## A worked example: `SELECT c1, SUM(c2) FROM t WHERE c1 > 10 GROUP BY c1`

Continuing the trace from [Chapter 1](./01-query-lifecycle.md), after
physical planning the tree is roughly `AggregateExec(mode=Partial) →
FilterExec → DataSourceExec` wrapped again in
`AggregateExec(mode=Final)` — a naive lowering with no repartitioning yet.

1. `JoinSelection` and `AggregateStatistics` run first but have nothing to
   do here — there is no join, and no aggregate-specific statistics rewrite
   applies.
2. `FilterPushdown` may push the `c1 > 10` predicate from `FilterExec`
   directly into `DataSourceExec`'s scan configuration if the source
   supports it, letting the scan prune data before it is even decoded
   ([Chapter 10](./10-data-sources.md)).
3. `EnforceDistribution` inspects the final `AggregateExec`'s
   `required_input_distribution` — hash-partitioned on `c1`, because a
   grouped aggregate's final phase is only correct if every row for a given
   group reaches the same partition's accumulator. The partial
   `AggregateExec`'s output partitioning doesn't satisfy that, so this rule
   inserts a `RepartitionExec` with `Partitioning::Hash([c1], n)` between the
   partial and final aggregate stages.
4. `CombinePartialFinalAggregate` checks whether the partial/final split is
   actually necessary given the now-final distribution and may combine them
   into a single-phase `AggregateExec` if partitioning already guaranteed
   correctness without a partial pre-aggregation step.
5. `EnforceSorting` has nothing to add — nothing downstream requires a
   specific order.
6. `SanityCheckPlan` walks the final tree, confirms the aggregate's
   distribution requirement is now satisfied, confirms no pipeline-breaking
   operator sits on an unbounded source, and lets the plan through.

## Invariants and edge cases

- **Rule order is load-bearing, not incidental.** The source comments in
  `PhysicalOptimizer::new()` document specific ordering constraints (e.g.
  `EnforceSorting` must run after `EnforceDistribution`, because
  repartitioning can break an ordering that was already satisfied — see
  `datafusion/physical-optimizer/src/optimizer.rs:143-242`). A custom rule
  inserted at the wrong position can silently reintroduce a requirement
  violation that `SanityCheckPlan` then has to reject.
- **A rule with `schema_check() == true` must not change the plan's output
  schema.** The framework checks this after every rule runs; changing
  nullability without overriding `schema_check` to `false` is a common way
  to trip this validation.
- **`SanityCheckPlan` is not optional cleanup — it is the correctness
  backstop.** Disabling it (or running a custom rule list that omits it) can
  let a genuinely unrunnable plan (e.g. a full sort over an unbounded
  stream) reach execution instead of failing fast with a diagnostic.
- **Rules that insert operators must recompute `PlanProperties` correctly
  for the new node**, or later rules in the same pass will reason about
  stale properties.

## Trade-offs

Running a fixed, hand-ordered rule list instead of a fixed-point loop (as
the logical optimizer uses) trades generality for predictability: physical
properties like partitioning interact in ways that don't obviously converge
under repeated application, so a curated order is easier to reason about and
faster to run, at the cost of needing careful, documented ordering
constraints whenever a new rule is added. The framework's own comments warn
that adding a new rule "is expensive as it will be applied to all queries" —
a deliberate bias toward extending existing rules over multiplying the rule
count.

## Key takeaways

- The physical optimizer runs a fixed, carefully ordered list of
  `PhysicalOptimizerRule`s once each — not a repeated fixed-point loop.
- Rules compare a node's declared requirements
  (`required_input_distribution`, `required_input_ordering`) against its
  children's actual `PlanProperties`, and insert `RepartitionExec` or
  `SortExec` only when the child doesn't already satisfy the requirement.
- `JoinSelection` and other algorithm-choice rules run early because they
  determine what later distribution/ordering rules must enforce;
  `SanityCheckPlan` runs last as a pure, non-rewriting gatekeeper.
- `schema_check()` lets a rule opt out of automatic output-schema
  validation when it legitimately changes nullability or field metadata.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md)
- [Chapter 8 — Aggregate Functions](./08-aggregate-functions.md)
- [Chapter 10 — Data Sources](./10-data-sources.md)
