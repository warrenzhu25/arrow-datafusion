# Chapter 10 — Data Sources

## Why this exists

Somewhere between a logical `TableScan` node and a physical operator that
actually reads bytes, DataFusion has to answer questions no other layer can:
what does this file's schema look like, which of the query's filters can
this source apply itself, and how should the data be split into partitions
for parallel scanning? Data source code is where those questions get
answered — and it is also the layer most embedders touch first, since
plugging in a new storage system or file format means implementing the
traits this chapter covers rather than modifying the query engine itself.

## The big picture

Two traits carry almost the entire data source story:

- **`TableProvider`** (`datafusion-catalog`) is what the catalog
  ([Chapter 11](./11-catalog-system.md)) hands back for a table name. Its
  job is to turn `(projection, filters, limit)` into an `ExecutionPlan`.
- **`FileFormat`** (`datafusion-datasource`) is what a file-backed
  `TableProvider` delegates to for anything format-specific: schema
  inference, statistics inference, and building the actual scan plan. CSV,
  JSON, Avro, Arrow, and Parquet each implement this trait once.

Everything else in this chapter — file grouping, schema adaptation,
statistics, filter pushdown negotiation — exists to let `TableProvider` and
`FileFormat` make good decisions without the SQL or logical-planning layers
needing to know anything about files, object stores, or storage formats at
all.

## Core concepts

### `TableProvider`: the table abstraction

```rust
// datafusion/catalog/src/table.rs
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>>;
}
```

`scan` is deliberately narrow: three optional hints (columns needed, filters
that apply, how many rows are wanted) and one required output (a physical
plan). A newer, additive `scan_with_args` method (same trait, structured
`ScanArgs`/`ScanResult` types) exists for providers that want to consume
richer planner hints — such as a preferred output ordering — without
breaking the simpler `scan` contract; it defaults to delegating to `scan`,
so existing providers are unaffected.

### `FileFormat`: format-specific behavior

```rust
// datafusion/datasource/src/file_format.rs
pub trait FileFormat: Any + Send + Sync + fmt::Debug {
    fn get_ext(&self) -> String;

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

A file-backed `TableProvider` (the built-in listing table, for example)
turns its `scan` call into a `FileScanConfig` — object store URLs, file
groups, projected schema, statistics, limits — and asks the configured
`FileFormat` to turn that into an `ExecutionPlan`. This is why adding
support for a new file format never touches SQL planning, the optimizer, or
the physical planner: it is purely a new `FileFormat` implementation plus
registration.

### Filter pushdown is negotiated, not assumed

```rust
// datafusion/expr/src/table_source.rs
pub enum TableProviderFilterPushDown {
    Unsupported, // filter is not pushed down; DataFusion evaluates it
    Inexact,     // provider may reduce data, but DataFusion still re-checks
    Exact,       // provider guarantees only passing rows are returned
}
```

The optimizer asks a provider, per filter, which of these three applies —
it never assumes. `Exact` means DataFusion can drop the filter entirely
after the scan. `Inexact` means the source used the filter to *reduce* data
(e.g. skipping whole files or row groups) but cannot guarantee every
remaining row actually passes, so DataFusion keeps a `FilterExec` above the
scan as a safety net. `Unsupported` means the filter had no effect on the
source at all, and the safety-net filter does all the work.

## How it works: file grouping, schema adaptation, and pruning

- **`file_groups.rs`** decides how files are grouped into partitions for
  parallel scanning — this directly controls the physical parallelism of a
  scan, independent of how many files exist.
- **`schema_adapter.rs`** reconciles a physical file's schema with the
  table's declared schema when they are compatible but not identical (a
  missing column that should read as `NULL`, or a type that needs a cheap
  cast) — this is what lets a table span files written by different schema
  versions.
- **`statistics.rs`** carries per-file and per-table statistics used by the
  optimizer for pruning and join-order decisions.

For Parquet specifically (`datafusion-datasource-parquet`), pushed-down
filters can prune at three granularities before a single row is decoded:
row-group statistics (`row_group_filter.rs`) skip whole row groups whose
min/max ranges cannot satisfy a predicate, page indexes (`page_filter.rs`)
skip individual pages within a surviving row group, and bloom filters
(referenced in `row_group_filter.rs` and `opener.rs`) rule out row groups
for equality predicates even when min/max statistics alone could not. All
three live behind the same `FileFormat`/`TableProviderFilterPushDown`
abstractions described above — SQL and logical planning never branch on
"is this Parquet."

## A worked example: `SELECT * FROM parquet_table WHERE region = 'us-east'`

1. The logical optimizer's filter-pushdown rule ([Chapter 4](./04-analyzer-optimizer.md))
   sees `Filter(region = 'us-east')` directly above a `TableScan` and moves
   the predicate into the scan node's filter list.
2. Physical planning calls the listing table's `scan`, passing `[region =
   'us-east']` as `filters`. The provider calls `supports_filters_pushdown`
   first; for a Parquet source this typically reports `Inexact` (Parquet can
   skip data using the predicate, but decoding still needs to double-check
   rows near a range boundary, dictionary-encoded pages, etc.).
3. Because pushdown is `Inexact`, the physical planner keeps a `FilterExec`
   above the Parquet scan as a correctness backstop, even though the scan
   itself will already have discarded most non-matching data.
4. The Parquet `FileFormat` builds a `FileScanConfig` and creates the scan's
   `ExecutionPlan`. At execution time, `row_group_filter.rs` uses `region`'s
   min/max statistics (and bloom filter, if present) to skip whole row
   groups that cannot contain `'us-east'`, and `page_filter.rs` narrows
   further within any row group that survives.
5. Rows that make it through Parquet-level pruning still pass through the
   `FilterExec` from step 3, which re-evaluates `region = 'us-east'` exactly
   — guaranteeing correctness regardless of how aggressive the Parquet-level
   pruning was.

## Invariants and edge cases

- **An `Inexact` or `Unsupported` pushdown claim must never cause
  DataFusion to skip re-evaluating the filter.** Only `Exact` licenses
  dropping the safety-net `FilterExec`; a provider that mistakenly claims
  `Exact` for a filter it does not fully honor produces silently wrong
  results, not just a slower query.
- **`infer_schema` must be consistent across all files a table spans.**
  Divergence is handled by `schema_adapter.rs`, not by `infer_schema`
  itself — the inference step establishes the table's schema, adaptation
  reconciles each file's reality against it at scan time.
- **File grouping determines scan parallelism, not row count or file
  count directly.** A table with many small files and a table with few
  large files can produce the same number of scan partitions if
  `file_groups.rs` groups them accordingly.

## Trade-offs

Negotiating pushdown per-filter, per-provider (rather than assuming a table
provider either handles all filters or none) adds a round trip — the
optimizer must ask before it can decide whether a backstop `FilterExec` is
needed — but it lets partial pushdown be both safe and precise: a source
that can prune coarsely (skip files) but not exactly (still needs row-level
re-checking) still gets to participate in pushdown instead of being all the
way in or all the way out. The cost lands on `TableProvider` implementors:
correctly classifying a filter as `Exact` versus `Inexact` is a correctness
obligation, not just a performance tuning knob.

## Key takeaways

- `TableProvider::scan` is the single seam between logical table references
  and physical scan plans; `FileFormat` is the seam between generic
  file-table logic and one storage format's specifics.
- Filter pushdown is negotiated per predicate via `TableProviderFilterPushDown`
  (`Unsupported`/`Inexact`/`Exact`) — DataFusion keeps a correctness-backstop
  filter unless a source guarantees `Exact`.
- Parquet layers pruning at three granularities — row group statistics,
  page indexes, bloom filters — entirely behind the same abstractions used
  by every other format.
- File grouping and schema adaptation are what let a single logical table
  span many physical files with varying layouts and partition counts.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 4 — Analyzer and Logical Optimizer](./04-analyzer-optimizer.md) — where filter pushdown is decided at the logical level
- [Chapter 5 — Physical Planning and Execution Plans](./05-physical-planning.md) — how a scan `ExecutionPlan` fits the physical tree
- [Chapter 11 — Catalog System](./11-catalog-system.md) — how a `TableProvider` is resolved from a SQL name in the first place
