# Apache DataFusion: Architecture Deep Dive & Learning Guide

A comprehensive guide for understanding DataFusion's architecture and each component in depth.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Query Execution Pipeline](#2-query-execution-pipeline)
3. [Component Deep Dives](#3-component-deep-dives)
   - [SQL Parsing & Planning](#31-sql-parsing--planning)
   - [Logical Expressions](#32-logical-expressions)
   - [Logical Optimizer](#33-logical-optimizer)
   - [Physical Planning](#34-physical-planning)
   - [Physical Expressions](#35-physical-expressions)
   - [Physical Optimizer](#36-physical-optimizer)
   - [Aggregate Functions](#37-aggregate-functions)
   - [Window Functions](#38-window-functions)
   - [Data Sources](#39-data-sources)
   - [Catalog System](#310-catalog-system)
   - [Execution Runtime](#311-execution-runtime)
   - [Session Management](#312-session-management)
4. [Crate Dependency Map](#4-crate-dependency-map)
5. [Extension Points Summary](#5-extension-points-summary)
6. [Learning Path Recommendations](#6-learning-path-recommendations)

---

## 1. Architecture Overview

DataFusion is a **modular, extensible query engine** built in Rust, leveraging Apache Arrow for columnar data processing. It provides:

- **SQL and DataFrame APIs** for query expression
- **Streaming, vectorized execution** using Apache Arrow
- **Pull-based Volcano-style** execution model
- **Multi-threaded parallelism** via Tokio runtime

### Core Design Principles

1. **Modularity**: 40+ specialized crates with clear separation of concerns
2. **Extensibility**: Almost every component is trait-based and pluggable
3. **Performance**: Vectorized columnar execution with predicate pushdown
4. **Correctness**: Comprehensive type system and semantic validation

---

## 2. Query Execution Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SQL String / DataFrame API                    │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SQL Parser (sqlparser-rs)                                          │
│  Crate: datafusion-sql                                              │
│  Converts SQL text → AST                                            │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SqlToRel Planner                                                   │
│  Crate: datafusion-sql                                              │
│  Converts AST → LogicalPlan                                         │
│  - Resolves table/column names via Catalog                          │
│  - Converts SQL expressions to Expr                                 │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Analyzer (AnalyzerRules)                                           │
│  Crate: datafusion-optimizer                                        │
│  - Type coercion                                                    │
│  - Schema validation                                                │
│  - Semantic rewrites                                                │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LogicalPlan (Validated)                                            │
│  Crate: datafusion-expr                                             │
│  DAG of logical operators (Filter, Project, Join, Aggregate, etc.)  │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Logical Optimizer (OptimizerRules)                                 │
│  Crate: datafusion-optimizer                                        │
│  - Filter pushdown                                                  │
│  - Projection pushdown                                              │
│  - Common subexpression elimination                                 │
│  - Join reordering                                                  │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Physical Planner                                                   │
│  Crate: datafusion-core                                             │
│  Converts LogicalPlan → ExecutionPlan                               │
│  - Selects algorithms (hash join vs sort-merge)                     │
│  - Determines partitioning                                          │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ExecutionPlan (Physical)                                           │
│  Crate: datafusion-physical-plan                                    │
│  DAG of physical operators (FilterExec, HashJoinExec, etc.)         │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Physical Optimizer (PhysicalOptimizerRules)                        │
│  Crate: datafusion-physical-optimizer                               │
│  - Distribution enforcement                                         │
│  - Sort optimization                                                │
│  - Join selection                                                   │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Execution                                                          │
│  Crate: datafusion-physical-plan                                    │
│  - plan.execute(partition, TaskContext)                             │
│  - Returns SendableRecordBatchStream                                │
│  - Streaming, parallel execution via Tokio                          │
└───────────────────────────────────┬─────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  RecordBatch Results (Apache Arrow)                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Data Types Between Stages

| Stage | Input | Output |
|-------|-------|--------|
| Parser | SQL String | Statement (AST) |
| SqlToRel | Statement | LogicalPlan + Expr |
| Analyzer | LogicalPlan | LogicalPlan (validated) |
| Optimizer | LogicalPlan | LogicalPlan (optimized) |
| Physical Planner | LogicalPlan | ExecutionPlan |
| Physical Optimizer | ExecutionPlan | ExecutionPlan (optimized) |
| Executor | ExecutionPlan | SendableRecordBatchStream |
| Stream | poll() | RecordBatch |

---

## 3. Component Deep Dives

### 3.1 SQL Parsing & Planning

**Crate**: `datafusion/sql`

**Purpose**: Translates SQL query text into DataFusion's internal LogicalPlan representation.

#### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `SqlToRel` | `planner.rs:454` | Main planner orchestrating conversion |
| `PlannerContext` | `planner.rs:257` | Planning state (CTEs, scopes) |
| `ParserOptions` | `planner.rs:45` | Configuration (case sensitivity, etc.) |

#### Query Processing Flow

```
SQL: "SELECT a, b FROM t WHERE a > 5"
        ↓
1. Parse with sqlparser-rs → Statement AST
        ↓
2. statement.rs routes to select_to_plan()
        ↓
3. select.rs processes:
   - FROM clause → TableScan
   - WHERE clause → Filter
   - SELECT clause → Projection
        ↓
4. expr/mod.rs converts expressions:
   - "a > 5" → BinaryExpr(Column("a"), Gt, Literal(5))
        ↓
5. Returns LogicalPlan tree
```

#### Extension Points

- **Custom Statement Handling**: Extend `DFParser` for new SQL constructs
- **Custom Expression Handling**: Implement `ExprPlanner` trait
- **Custom Type Handling**: Implement `TypePlanner` trait

#### Key Files

| File | Lines | Purpose |
|------|-------|---------|
| `planner.rs` | 38K | Core SqlToRel struct |
| `statement.rs` | 107K | DDL/DML statement handling |
| `select.rs` | 50K | SELECT processing |
| `expr/mod.rs` | 54K | Expression conversion |
| `expr/function.rs` | 46K | Function resolution |

---

### 3.2 Logical Expressions

**Crate**: `datafusion/expr`

**Purpose**: Defines the logical representation layer for expressions and query plans.

#### The Expr Enum

The `Expr` enum (at `expr.rs:326-434`) represents all possible logical expressions:

```rust
pub enum Expr {
    // Basic values
    Column(Column),                    // Reference to a table column
    Literal(ScalarValue, ...),         // Constant value

    // Operators
    BinaryExpr(BinaryExpr),            // a + b, a > b
    Not(Box<Expr>),                    // NOT expr

    // Functions
    ScalarFunction(ScalarFunction),    // scalar_func(args)
    AggregateFunction(AggregateFunction), // SUM(col), COUNT(*)
    WindowFunction(Box<WindowFunction>),  // ROW_NUMBER() OVER (...)

    // Subqueries
    ScalarSubquery(Subquery),          // (SELECT col FROM table)
    InSubquery(InSubquery),            // col IN (subquery)
    Exists(Exists),                    // EXISTS (subquery)

    // And 20+ more variants...
}
```

#### LogicalPlan Variants

The `LogicalPlan` enum (`logical_plan/plan.rs:206-293`) represents query operations:

```rust
pub enum LogicalPlan {
    // Data sources
    TableScan(TableScan),              // FROM table
    EmptyRelation(EmptyRelation),      // SELECT without FROM

    // Transformations
    Projection(Projection),            // SELECT expressions
    Filter(Filter),                    // WHERE predicate
    Aggregate(Aggregate),              // GROUP BY with aggregates
    Join(Join),                        // INNER/LEFT/RIGHT JOIN
    Sort(Sort),                        // ORDER BY
    Limit(Limit),                      // LIMIT/OFFSET

    // And 15+ more variants...
}
```

#### Type System

- **DFSchema**: Extends Arrow Schema with table qualifiers
- **ExprSchemable**: Trait for type inference (`get_type()`, `nullable()`)
- **TreeNode**: Trait for expression tree traversal and transformation

#### Key Files

| File | Lines | Purpose |
|------|-------|---------|
| `expr.rs` | 3,500+ | Expr enum and implementations |
| `expr_schema.rs` | 1,400+ | Type inference |
| `logical_plan/plan.rs` | 6,800+ | LogicalPlan enum |
| `logical_plan/builder.rs` | 3,200+ | LogicalPlanBuilder API |
| `udf.rs` | 1,260+ | ScalarUDF definition |
| `udaf.rs` | 1,535+ | AggregateUDF definition |

---

### 3.3 Logical Optimizer

**Crate**: `datafusion/optimizer`

**Purpose**: Transforms LogicalPlans into equivalent but more efficient forms.

#### AnalyzerRule vs OptimizerRule

| Aspect | AnalyzerRule | OptimizerRule |
|--------|--------------|---------------|
| Purpose | Make plan *valid* | Make plan *efficient* |
| Runs | Once, early | Multiple passes |
| Semantics | May change form | Must preserve equivalence |
| Examples | Type coercion | Filter pushdown |

#### Built-in Optimization Rules

**Filter/Join Optimization**:
- `PushDownFilter`: Move filters toward data sources
- `EliminateCrossJoin`: CROSS JOIN → INNER JOIN when predicates exist
- `EliminateOuterJoin`: OUTER → INNER when filter permits

**Projection Optimization**:
- `OptimizeProjections`: Remove unused columns
- `EliminateDuplicatedExpr`: Remove redundant expressions

**Expression Simplification**:
- `SimplifyExpressions`: Constant folding, boolean algebra
- `CommonSubexprEliminate`: Reuse computed expressions

**Subquery Optimization**:
- `DecorrelatePredicateSubquery`: IN/EXISTS → SEMI/ANTI joins
- `ScalarSubqueryToJoin`: Scalar subqueries → left joins

#### Rule Application

Rules are applied in multiple passes until a fixed point:

```rust
// Default rule order (25+ rules)
vec![
    SimplifyExpressions::new(),
    EliminateFilter::new(),
    PushDownFilter::new(),
    CommonSubexprEliminate::new(),
    OptimizeProjections::new(),
    // ... more rules
]
```

#### Extension Point

```rust
impl OptimizerRule for MyCustomRule {
    fn name(&self) -> &str { "my_rule" }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig)
        -> Result<Transformed<LogicalPlan>> {
        // Transform and return Transformed::yes() or Transformed::no()
    }
}
```

---

### 3.4 Physical Planning

**Crate**: `datafusion/physical-plan`

**Purpose**: Converts LogicalPlans to executable ExecutionPlans.

#### The ExecutionPlan Trait

```rust
pub trait ExecutionPlan: Any + Send + Sync + Display + Debug {
    // Identity & Schema
    fn name(&self) -> &str;
    fn properties(&self) -> &Arc<PlanProperties>;

    // Execution (THE CORE METHOD)
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    // Plan Structure
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
    fn with_new_children(...) -> Result<Arc<dyn ExecutionPlan>>;

    // Requirements
    fn required_input_distribution(&self) -> Vec<Distribution>;
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>>;
}
```

#### Built-in ExecutionPlans

**Leaf Operators**:
- `EmptyExec`: No-op, produces zero rows
- `MemoryExec`: Read in-memory tables
- `DataSourceExec`: Read from files (Parquet, CSV, etc.)

**Unary Operators**:
- `FilterExec`: Apply predicates
- `ProjectionExec`: Select/compute columns
- `SortExec`: ORDER BY
- `LimitExec`: LIMIT/OFFSET
- `AggregateExec`: GROUP BY with aggregations

**Binary Operators**:
- `HashJoinExec`: Distributed hash join
- `SortMergeJoinExec`: Join pre-sorted inputs
- `NestedLoopJoinExec`: Fallback O(n²) join
- `CrossJoinExec`: Cartesian product

**Distribution Operators**:
- `RepartitionExec`: Redistribute data across partitions
- `CoalescePartitionsExec`: Merge partitions into one

#### Partitioning Model

```rust
pub enum Partitioning {
    RoundRobinBatch(usize),              // Round-robin distribution
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize), // Hash on expressions
    UnknownPartitioning(usize),          // Unknown distribution
}

pub enum Distribution {
    UnspecifiedDistribution,
    SinglePartition,
    HashPartitioned(Vec<PhysicalExpr>),
}
```

---

### 3.5 Physical Expressions

**Crate**: `datafusion/physical-expr`

**Purpose**: Evaluates expressions on actual data (RecordBatch).

#### The PhysicalExpr Trait

```rust
pub trait PhysicalExpr: Any + Send + Sync + Display + Debug {
    // Type information
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;

    // Evaluation (THE CRITICAL METHOD)
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;

    // Tree operations
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;
    fn with_new_children(...) -> Result<Arc<dyn PhysicalExpr>>;
}
```

#### ColumnarValue: Array or Scalar

```rust
pub enum ColumnarValue {
    Array(ArrayRef),     // Column of N values
    Scalar(ScalarValue), // Single value (broadcasted)
}
```

Key insight: Literals stay as scalars (no materialization), Arrow handles broadcasting.

#### Expression Types

| Type | Implementation | Purpose |
|------|----------------|---------|
| Column | `column.rs` | Reference table column by index |
| Literal | `literal.rs` | Constant values |
| BinaryExpr | `binary.rs` | Arithmetic, comparison, logical |
| CastExpr | `cast.rs` | Type conversion |
| CaseExpr | `case.rs` | CASE WHEN expressions |
| ScalarFunctionExpr | `scalar_function.rs` | UDF invocation |

---

### 3.6 Physical Optimizer

**Crate**: `datafusion/physical-optimizer`

**Purpose**: Optimizes ExecutionPlans for efficient execution.

#### Key Rules

**Distribution Enforcement**:
- `EnforceDistribution`: Insert RepartitionExec for parallelism
- `EnforceSorting`: Add/remove SortExec nodes

**Join Optimization**:
- `JoinSelection`: Choose hash vs sort-merge, swap sides based on statistics

**Limit Optimization**:
- `LimitPushdown`: Push LIMIT through operators
- `TopKAggregation`: Optimize GROUP BY with LIMIT

**Sort Optimization**:
- `PushdownSort`: Push sorts to data sources
- `OptimizeAggregateOrder`: Reorder aggregate expressions

#### Rule Order Matters

```rust
// Simplified default order
vec![
    JoinSelection::new(),           // Lock in join strategies first
    EnforceDistribution::new(),     // Then enforce partitioning
    EnforceSorting::new(),          // Then enforce ordering
    ProjectionPushdown::new(),      // Then push projections
    LimitPushdown::new(),           // Finally push limits
    SanityCheckPlan::new(),         // Validate final plan
]
```

---

### 3.7 Aggregate Functions

**Crate**: `datafusion/functions-aggregate`

**Purpose**: Implements SQL aggregate functions (SUM, COUNT, AVG, etc.).

#### Core Traits

**AggregateUDFImpl**: Defines aggregate function metadata
```rust
pub trait AggregateUDFImpl {
    fn name(&self) -> &str;
    fn signature(&self) -> &Signature;
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>;
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>;
}
```

**Accumulator**: Maintains aggregation state
```rust
pub trait Accumulator: Send + Sync + Debug {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;
    fn evaluate(&mut self) -> Result<ScalarValue>;
    fn state(&mut self) -> Result<Vec<ScalarValue>>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
}
```

#### Multi-Phase Aggregation

```
Phase 1 (Partial): Each partition aggregates independently
                   Output: (group_key, partial_state)
                          ↓
Phase 2 (Final):   Merge partial results across partitions
                   Output: (group_key, final_value)
```

#### Built-in Aggregates

| Category | Functions |
|----------|-----------|
| Basic | COUNT, SUM, AVG, MIN, MAX |
| Statistical | STDDEV, VARIANCE, COVARIANCE, CORRELATION |
| Order-sensitive | FIRST_VALUE, LAST_VALUE, ARRAY_AGG, STRING_AGG |
| Approximate | APPROX_DISTINCT, APPROX_MEDIAN, APPROX_PERCENTILE_CONT |
| Bitwise | BIT_AND, BIT_OR, BIT_XOR, BOOL_AND, BOOL_OR |

---

### 3.8 Window Functions

**Crate**: `datafusion/functions-window`

**Purpose**: Implements SQL window functions (ROW_NUMBER, RANK, LAG, etc.).

#### Core Traits

**WindowUDFImpl**: Defines window function
```rust
pub trait WindowUDFImpl {
    fn name(&self) -> &str;
    fn signature(&self) -> &Signature;
    fn partition_evaluator(&self, args: PartitionEvaluatorArgs)
        -> Result<Box<dyn PartitionEvaluator>>;
}
```

**PartitionEvaluator**: Computes results per partition
```rust
pub trait PartitionEvaluator: Debug + Send {
    // Batch evaluation (most efficient)
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize)
        -> Result<ArrayRef>;

    // Streaming evaluation (for bounded execution)
    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>)
        -> Result<ScalarValue>;

    // Capabilities
    fn is_causal(&self) -> bool;                    // No future data needed?
    fn uses_window_frame(&self) -> bool;            // Respects ROWS/RANGE?
    fn supports_bounded_execution(&self) -> bool;   // Can stream?
}
```

#### WindowFrame

```rust
pub struct WindowFrame {
    pub units: WindowFrameUnits,     // ROWS, RANGE, or GROUPS
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}
```

#### Built-in Window Functions

| Category | Functions |
|----------|-----------|
| Ranking | ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST |
| Navigation | LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE |
| Distribution | NTILE |

---

### 3.9 Data Sources

**Crates**: `datafusion/datasource`, `datafusion/datasource-parquet`, etc.

**Purpose**: Bridge between raw data files and the query engine.

#### Core Traits

**TableProvider**: Main interface for tables
```rust
pub trait TableProvider: Any + Debug + Sync + Send {
    fn schema(&self) -> SchemaRef;
    fn table_type(&self) -> TableType;
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    fn supports_filters_pushdown(&self, filters: &[&Expr])
        -> Result<Vec<TableProviderFilterPushDown>>;
}
```

**FileFormat**: Format-specific logic
```rust
pub trait FileFormat: Any + Send + Sync {
    async fn infer_schema(...) -> Result<SchemaRef>;
    async fn infer_stats(...) -> Result<Statistics>;
    async fn create_physical_plan(...) -> Result<Arc<dyn ExecutionPlan>>;
}
```

#### Filter Pushdown Levels (Parquet)

```
Level 1: Row Group Pruning (using min/max statistics)
Level 2: Page Pruning (using page index)
Level 3: Bloom Filter Pruning (exact value check)
Level 4: Row-Level Filtering (during decode)
```

#### Supported Formats

| Format | Crate | Optimizations |
|--------|-------|---------------|
| Parquet | datasource-parquet | Statistics, Bloom filters, predicate pushdown |
| CSV | datasource-csv | Schema inference, projection pushdown |
| JSON | datasource-json | NDJSON, schema inference |
| Avro | datasource-avro | Embedded schema |
| Arrow | datasource-arrow | Native format |

---

### 3.10 Catalog System

**Crate**: `datafusion/catalog`

**Purpose**: Metadata management for tables, schemas, and catalogs.

#### Catalog Hierarchy

```
CatalogProviderList (manages all catalogs)
    └─ CatalogProvider (one catalog = "database")
        └─ SchemaProvider (one schema)
            └─ TableProvider (one table)
```

#### Core Traits

**CatalogProvider**:
```rust
pub trait CatalogProvider: Any + Debug + Sync + Send {
    fn schema_names(&self) -> Vec<String>;
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
    fn register_schema(...) -> Result<Option<Arc<dyn SchemaProvider>>>;
}
```

**SchemaProvider**:
```rust
pub trait SchemaProvider: Any + Debug + Sync + Send {
    fn table_names(&self) -> Vec<String>;
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
    fn register_table(...) -> Result<Option<Arc<dyn TableProvider>>>;
}
```

#### Built-in Implementations

| Implementation | Purpose |
|----------------|---------|
| MemoryCatalogProvider | In-memory catalog storage |
| MemorySchemaProvider | In-memory schema storage |
| InformationSchemaProvider | SQL standard metadata tables |
| ListingSchemaProvider | Discover tables from ObjectStore |

---

### 3.11 Execution Runtime

**Crate**: `datafusion/execution`

**Purpose**: Runtime infrastructure for resource management.

#### Core Components

**TaskContext**: Runtime context for operators
```rust
pub struct TaskContext {
    session_id: String,
    session_config: SessionConfig,
    scalar_functions: HashMap<...>,
    aggregate_functions: HashMap<...>,
    runtime: Arc<RuntimeEnv>,
}
```

**RuntimeEnv**: Resource management
```rust
pub struct RuntimeEnv {
    pub memory_pool: Arc<dyn MemoryPool>,
    pub disk_manager: Arc<DiskManager>,
    pub cache_manager: Arc<CacheManager>,
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
}
```

#### Memory Management

**MemoryPool Implementations**:
- `UnboundedMemoryPool`: No limits (debugging)
- `GreedyMemoryPool`: First-come, first-served
- `FairSpillPool`: Fair allocation among spillable operators
- `TrackConsumersPool`: Monitoring wrapper

**Reservation Pattern**:
```rust
let consumer = MemoryConsumer::new("SortExec")
    .with_can_spill(true)
    .register(memory_pool);

consumer.try_grow(10_000_000)?;  // Request 10MB
// If fails, spill to disk and retry
consumer.shrink(freed_bytes);
```

#### Disk Spilling

When memory is exhausted:
1. Operator calls `disk_manager.create_tmp_file()`
2. Serialize intermediate state to disk
3. `shrink()` memory reservation
4. Continue processing with limited memory
5. Merge spilled data during finalization

---

### 3.12 Session Management

**Crates**: `datafusion/session`, `datafusion/core`

**Purpose**: Manage query execution context across operations.

#### Core Components

**SessionContext**: User-facing API
```rust
pub struct SessionContext {
    session_id: String,
    session_start_time: DateTime<Utc>,
    state: Arc<RwLock<SessionState>>,  // Thread-safe shared state
}
```

**SessionState**: Query execution state
```rust
pub struct SessionState {
    // Planning components
    analyzer: Analyzer,
    optimizer: Optimizer,
    physical_optimizers: PhysicalOptimizer,
    query_planner: Arc<dyn QueryPlanner>,

    // Function registries
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,

    // Catalog and configuration
    catalog_list: Arc<dyn CatalogProviderList>,
    config: SessionConfig,
    runtime_env: Arc<RuntimeEnv>,
}
```

#### Key APIs

```rust
// SQL execution
let df = ctx.sql("SELECT * FROM table").await?;

// DataFrame API
let df = ctx.read_parquet("file.parquet", options).await?;
let df = df.filter(col("a").gt(lit(10)))?
           .select(vec![col("a"), col("b")])?;

// Table registration
ctx.register_csv("table", "file.csv", options).await?;
ctx.register_table("table", Arc::new(my_provider))?;

// UDF registration
ctx.register_udf(my_scalar_udf);
ctx.register_udaf(my_aggregate_udf);
```

---

## 4. Crate Dependency Map

```
LAYER 1: FOUNDATIONAL
├─ arrow, sqlparser, tokio, object_store

LAYER 2: COMMON & TYPES
├─ datafusion-common (errors, utilities, TreeNode)
├─ datafusion-expr-common (Accumulator, ColumnarValue)
├─ datafusion-physical-expr-common (Sort, Partitioning)

LAYER 3: EXPRESSIONS & PLANS
├─ datafusion-expr (LogicalPlan, Expr, UDF traits)
├─ datafusion-physical-expr (PhysicalExpr)

LAYER 4: EXECUTION
├─ datafusion-execution (TaskContext, RuntimeEnv)
├─ datafusion-physical-plan (ExecutionPlan trait)

LAYER 5: PLANNING & OPTIMIZATION
├─ datafusion-optimizer (Analyzer, Optimizer)
├─ datafusion-physical-optimizer
├─ datafusion-sql (SQL parsing, SqlToRel)

LAYER 6: DATA SOURCES
├─ datafusion-datasource (DataSource, FileSource)
├─ datafusion-datasource-parquet, -csv, -json, -avro

LAYER 7: CATALOG & SESSION
├─ datafusion-catalog (TableProvider, CatalogProvider)
├─ datafusion-session

LAYER 8: FUNCTIONS
├─ datafusion-functions (scalar)
├─ datafusion-functions-aggregate
├─ datafusion-functions-window
├─ datafusion-functions-nested

LAYER 9: HIGH-LEVEL API
├─ datafusion-core (SessionContext, DataFrame)
├─ datafusion (re-exports, prelude)
```

---

## 5. Extension Points Summary

| Extension Type | Trait to Implement | Registration |
|----------------|-------------------|--------------|
| Custom Table | `TableProvider` | `ctx.register_table()` |
| Custom File Format | `FileFormat` + `FileSource` | `SessionState` |
| Scalar Function | `ScalarUDF` | `ctx.register_udf()` |
| Aggregate Function | `AggregateUDF` | `ctx.register_udaf()` |
| Window Function | `WindowUDF` | `ctx.register_udwf()` |
| Logical Optimizer | `OptimizerRule` | `ctx.add_optimizer_rule()` |
| Physical Optimizer | `PhysicalOptimizerRule` | via `SessionStateBuilder` |
| Custom Operator | `ExecutionPlan` | via custom planner |
| Custom Catalog | `CatalogProvider` | `ctx.register_catalog()` |
| Custom Memory Pool | `MemoryPool` | via `RuntimeEnvBuilder` |

---

## 6. Learning Path Recommendations

### Beginner (Weeks 1-2)

1. **SessionContext & Basic Usage**
   - File: `datafusion/core/src/lib.rs` (lines 42-161)
   - Run: `examples/dataframe/dataframe.rs`

2. **SQL to Results Flow**
   - File: `datafusion/core/src/lib.rs` (lines 240-305)
   - Understand: LogicalPlan → ExecutionPlan → RecordBatch

3. **DataFrame API**
   - File: `datafusion/core/src/dataframe/mod.rs`
   - Practice: Filter, project, aggregate

### Intermediate (Weeks 3-4)

4. **Logical Plans & Expressions**
   - Files: `datafusion/expr/src/expr.rs`, `logical_plan/plan.rs`
   - Example: `examples/query_planning/expr_api.rs`

5. **Optimization Rules**
   - File: `datafusion/optimizer/src/lib.rs`
   - Example: `examples/query_planning/optimizer_rule.rs`

6. **Physical Planning & Execution**
   - Files: `datafusion/physical-plan/src/execution_plan.rs`
   - Understand: ExecutionPlan trait, partitioning

### Advanced (Weeks 5-8)

7. **Custom TableProvider**
   - File: `datafusion/catalog/src/table.rs`
   - Implement: Custom data source

8. **User-Defined Functions**
   - Examples: `examples/udf/simple_udf.rs`, `simple_udaf.rs`
   - Implement: Domain-specific functions

9. **Custom Optimizer Rules**
   - Files: `datafusion/optimizer/src/*.rs`
   - Implement: Custom query rewriting

10. **Custom ExecutionPlan**
    - File: `datafusion/physical-plan/src/execution_plan.rs`
    - Implement: Custom physical operator

---

## Additional Resources

- **API Documentation**: https://docs.rs/datafusion
- **Project Website**: https://datafusion.apache.org
- **Examples Directory**: `datafusion-examples/`
- **Architecture Documentation**: `docs/source/contributor-guide/architecture.md`

---

*This guide was generated by deep exploration of the DataFusion codebase. For the most current information, always refer to the source code and official documentation.*
