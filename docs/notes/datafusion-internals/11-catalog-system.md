# Chapter 11 — Catalog System

## Why this exists

Before a query can reference a table, something has to answer "what table is
`orders`, and who owns it?" SQL planning ([Chapter 2](./02-sql-planning.md))
needs that answer while it is still building `LogicalPlan` nodes, long before
any data is read. The catalog system is the layer that answers it: a small,
trait-based hierarchy that turns a possibly-qualified SQL name into a
concrete `TableProvider` ([Chapter 10](./10-data-sources.md)), without SQL
planning ever needing to know whether that table lives in memory, on an
object store, or behind a remote metastore.

## The big picture

DataFusion's catalog hierarchy mirrors SQL's three-part naming:
`catalog.schema.table`.

```
CatalogProviderList
  └── CatalogProvider ("catalog")
        └── SchemaProvider ("schema")
              └── TableProvider ("table")
```

Each level is a trait, not a concrete type. The default implementations are
simple in-memory maps, but nothing about SQL planning depends on that — an
embedder can back `CatalogProvider` with a remote metastore, back
`SchemaProvider` with a lazily-populated cache, or back `TableProvider` with
a data source DataFusion has never heard of, and the planner code does not
change.

## Core concepts

### `CatalogProviderList`, `CatalogProvider`, `SchemaProvider`

```rust
// datafusion/catalog/src/catalog.rs
pub trait CatalogProvider: Any + Debug + Sync + Send {
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

pub trait CatalogProviderList: Any + Debug + Sync + Send {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;
    fn catalog_names(&self) -> Vec<String>;
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}
```

A **`CatalogProviderList`** is the top-level registry a `SessionState`
([Chapter 13](./13-session-dataframe-api.md)) holds — it is how a session
supports more than one catalog at once (useful for federated or
multi-tenant setups). Each **`CatalogProvider`** owns a set of named
schemas; each **`SchemaProvider`** owns a set of named tables:

```rust
// datafusion/catalog/src/schema.rs
pub trait SchemaProvider: Any + Debug + Sync + Send {
    fn owner_name(&self) -> Option<&str> { None }
    fn table_names(&self) -> Vec<String>;

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
}
```

`SchemaProvider::table` is `async` — deliberately. A schema backed by a
remote metastore may need to make a network call to resolve a table on first
reference; an in-memory schema resolves it synchronously under the hood but
still exposes the same async signature so callers do not need two code
paths.

### Implementations beyond the in-memory default

| File | What it adds |
|---|---|
| `datafusion/catalog/src/catalog.rs` | `MemoryCatalogProvider` / `MemoryCatalogProviderList` — the default, a `DashMap`-backed registry used unless a session is configured otherwise. |
| `datafusion/catalog/src/listing_schema.rs` | `ListingSchemaProvider` — discovers tables by listing an object store path instead of requiring explicit `register_table` calls, so new files can appear as new tables. |
| `datafusion/catalog/src/information_schema.rs` | Implements SQL-standard `information_schema.tables`, `.columns`, `.schemata`, and `.routines` as queryable `TableProvider`s backed by the catalog list itself. |
| `datafusion/catalog/src/view.rs` | `ViewTable` — a `TableProvider` whose scan plan is another stored `LogicalPlan`, letting `CREATE VIEW` reuse the same resolution path as a real table. |
| `datafusion/catalog/src/default_table_source.rs` | Bridges a `TableProvider` into `datafusion_expr::TableSource`, the narrower interface SQL planning actually depends on (see below). |

## How it works: resolving a name during planning

1. SQL planning encounters a table reference — `FROM orders` or `FROM
   sales.orders` or `FROM db.sales.orders`.
2. `SqlToRel` ([Chapter 2](./02-sql-planning.md)) asks its `ContextProvider`
   to resolve the `TableReference`. Session configuration supplies the
   default catalog and schema names used when a reference is unqualified.
3. Resolution walks the hierarchy: `CatalogProviderList::catalog(catalog_name)`
   → `CatalogProvider::schema(schema_name)` → `SchemaProvider::table(table_name)`.
   Any missing level produces a "table not found" planning error.
4. The resolved `Arc<dyn TableProvider>` is wrapped through
   `DefaultTableSource` into a `datafusion_expr::TableSource` — the minimal,
   catalog-independent view of a table that a `LogicalPlan::TableScan` node
   actually stores. This keeps the logical layer from depending on the full
   `TableProvider` trait (which includes `scan`, an execution-planning
   concern) inside a purely semantic data structure.
5. Physical planning ([Chapter 5](./05-physical-planning.md)) later calls
   `TableProvider::scan` on that same provider — reached through the table
   scan node, not re-resolved through the catalog — to produce an
   `ExecutionPlan`.

Name resolution therefore happens exactly once, during logical planning; by
the time a plan reaches the optimizer or the physical planner, table
identity is already pinned to a concrete `TableProvider` instance.

## A worked example: `information_schema` as an ordinary table

`information_schema.tables` is not special-cased in the SQL planner at all —
it is a normal table lookup that happens to resolve to a `TableProvider`
whose `scan` builds its output by walking the very `CatalogProviderList` it
is registered under. When a session enables `information_schema` support,
`SessionState` registers an `InformationSchemaProvider` as a schema in a
reserved catalog. A query like:

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
```

goes through the identical `CatalogProvider` → `SchemaProvider` →
`TableProvider` resolution path as `SELECT * FROM public.orders` — the only
difference is what the resolved provider's `scan` does when physical
planning calls it. This is a useful proof that the catalog abstraction is
pulling real weight: metadata introspection did not need a parser
special-case, only another implementation of traits SQL planning already
knew how to use.

## Invariants and edge cases

- **Name resolution is case-sensitivity- and quoting-aware, and happens
  once.** A `LogicalPlan::TableScan` stores the resolved `TableSource`
  directly; nothing downstream re-resolves the name, so a schema that
  mutates concurrently with a running query cannot change what a plan built
  moments ago will scan.
- **`register_table` and `register_schema` return the previous value, not
  an error, on a name collision.** This is a deliberate "swap and tell me
  what was there" contract — callers that care about collisions must check
  the `Option` themselves.
- **`ListingSchemaProvider` tables are point-in-time.** Listing happens when
  the provider is asked, not continuously; a file added to the backing path
  after listing is not visible until the schema is asked again.
- **`information_schema` tables are always consistent with the live catalog
  state at scan time**, not at parse time, because their `scan()` reads the
  catalog list directly rather than caching a snapshot.

## Trade-offs

Making every level of the hierarchy a trait — rather than a concrete
in-memory struct with hooks — means every lookup pays a dynamic dispatch and
every table lookup is `async` even when the common case is a synchronous
`HashMap` read. In exchange, an embedder can back an entire catalog with a
remote system (a Hive metastore, a lakehouse catalog service) without
touching SQL planning, and can make catalog discovery lazy (`ListingSchemaProvider`)
instead of requiring every table to be registered up front. For a project
whose tables are all known ahead of time, this generality is close to free —
the default in-memory providers add negligible overhead — but it is what
makes DataFusion usable as an embedded planner in front of storage systems
its authors never anticipated.

## Key takeaways

- Catalog resolution follows SQL's own hierarchy: `CatalogProviderList` →
  `CatalogProvider` → `SchemaProvider` → `TableProvider`, each one a trait.
- Resolution happens once, during logical planning; the resolved table
  identity is baked into the `LogicalPlan::TableScan` node as a
  `TableSource`, not re-looked-up later.
- `information_schema`, views, and dynamically-listed tables are not special
  cases in the planner — they are just other `TableProvider` implementations
  reached through the same catalog path.
- The catalog layer is the seam that lets DataFusion sit in front of an
  external metastore without SQL planning code changes.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 2 — SQL Parsing and Planning](./02-sql-planning.md)
- [Chapter 10 — Data Sources](./10-data-sources.md)
- [Chapter 13 — Session Management and DataFrame API](./13-session-dataframe-api.md)
