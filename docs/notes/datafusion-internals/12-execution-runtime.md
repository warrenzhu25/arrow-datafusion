# Chapter 12 — Execution Runtime

## Why this exists

A physical operator ([Chapter 5](./05-physical-planning.md)) knows *what* to
compute, but not *with what resources*. A hash join building a probe table,
a sort accumulating rows, a grouped aggregate holding one accumulator per
group — all of them need memory, and all of them need somewhere to put data
when memory runs out. The execution runtime is the layer that owns those
resources and the policy for allocating them, so operator code can ask "may
I have N more bytes?" without knowing whether the answer comes from an
unbounded pool, a fixed budget, or a budget shared with every other query
running in the same process.

## The big picture

```rust
// datafusion/execution/src/runtime_env.rs
pub struct RuntimeEnv {
    pub memory_pool: Arc<dyn MemoryPool>,
    pub disk_manager: Arc<DiskManager>,
    pub cache_manager: Arc<CacheManager>,
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
}
```

`RuntimeEnv` bundles the resources that are naturally *process-scoped*
rather than *query-scoped*: a memory budget, a place to write spill files, a
cache, and a registry of object stores. It is typically created once and
shared by every session and every query that session runs, via
`RuntimeEnvBuilder`. Individual queries get a `TaskContext`
([Chapter 13](./13-session-dataframe-api.md)) that carries a reference to
the shared `RuntimeEnv` alongside query-specific configuration and function
registries — the split lets many concurrent queries share one memory budget
and one set of object store clients while still seeing their own session
configuration.

## Core concepts

### `MemoryPool` and `MemoryReservation`

```rust
// datafusion/execution/src/memory_pool/mod.rs
pub trait MemoryPool: Any + Send + Sync + std::fmt::Debug + Display {
    fn name(&self) -> &str;
    fn register(&self, _consumer: &MemoryConsumer) {}
    fn unregister(&self, _consumer: &MemoryConsumer) {}

    fn grow(&self, reservation: &MemoryReservation, additional: usize);
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize);
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()>;

    fn reserved(&self) -> usize;
}
```

A **`MemoryPool`** arbitrates memory across every concurrent consumer in a
process. Operators do not call it directly; they create a
**`MemoryConsumer`**, register it, and grow or shrink a **`MemoryReservation`**
against the pool as they allocate and free memory. `grow` is infallible — it
always succeeds, and is meant for accounting memory an operator has already
committed to using. `try_grow` is the fallible variant operators call
*before* allocating: if the pool refuses, the operator must find another way
forward — usually by spilling.

`datafusion/execution/src/memory_pool/pool.rs` implements the built-in
policies:

| Pool | Behavior |
|---|---|
| `UnboundedMemoryPool` | Never refuses `try_grow`. The default; useful for tests and workloads with no memory constraint. |
| `GreedyMemoryPool` | Enforces a fixed byte budget; `try_grow` fails once the budget is exhausted, first-come-first-served. |
| `FairSpillPool` | Enforces a fixed budget but divides it fairly across registered spill-capable consumers, so one large query cannot starve every other concurrent operator of the memory it needs to make progress. |
| `TrackConsumersPool` | A wrapper that adds per-consumer accounting on top of another pool, useful for diagnostics ("who is using the memory"). |

### `DiskManager`, `CacheManager`, `ObjectStoreRegistry`

`DiskManager` (`datafusion/execution/src/disk_manager.rs`) creates and
cleans up the temporary files spill-capable operators write to; it controls
*where* spill files land (a configured directory, or the OS temp directory
by default) without operators needing to manage file lifecycle themselves.
`ObjectStoreRegistry` (`datafusion/execution/src/object_store.rs`) resolves
a URL scheme (`s3://`, `file://`, ...) to the `object_store::ObjectStore`
client that should serve it, so file-based data sources
([Chapter 10](./10-data-sources.md)) can read local files, cloud storage, or
any other backend through one interface without a branch per scheme.
`CacheManager` coordinates optional caches — file metadata and statistics
caches used by repeated scans of the same files.

## How it works: from `try_grow` refusal to a spill file

A spill-capable operator (grouped hash aggregation, external sort, hash
join build side) follows the same rhythm:

1. While consuming input batches, the operator grows a `MemoryReservation`
   to account for its in-memory state (a hash table, a set of accumulators,
   a sorted run).
2. When `try_grow` fails, the operator does not treat this as an error. It
   instead emits its current in-memory state as a `RecordBatch`, sorts or
   otherwise prepares it for merging, and writes it to a spill file through
   `DiskManager`.
3. The operator shrinks its reservation to reflect the memory it just freed,
   clears its in-memory structures, and resumes consuming input.
4. Once all input is consumed, the operator streams-merges its spill files
   (and any final in-memory state) to produce correct output — spilling
   trades memory for a merge step, not for correctness.

## A worked example: `GroupedHashAggregateStream::spill`

`datafusion/physical-plan/src/aggregates/row_hash.rs:1237-1298` implements
exactly this rhythm for grouped aggregation:

```rust
impl GroupedHashAggregateStream {
    /// Emit all intermediate aggregation states, sort them, and store them on disk.
    fn spill(&mut self) -> Result<()> {
        // Emit and sort intermediate aggregation state
        let Some(emit) = self.emit(EmitTo::All, true)? else {
            return Ok(());
        };

        // Free accumulated state now that data has been emitted into `emit`.
        // This must happen before reserving sort memory so the pool has room.
        self.clear_shrink(0);
        self.update_memory_reservation()?;

        let batch_size_ratio = self.batch_size as f32 / emit.num_rows() as f32;
        let batch_memory = get_record_batch_memory_size(&emit);
        // Worst case for a sort is 2x the underlying buffer size.
        let sort_memory = (batch_memory
            + (emit.get_sliced_size()? as f32 * batch_size_ratio) as usize)
            .min(batch_memory * 2);

        // If we can't grow even that, we cannot spill without sorting first.
        self.reservation.try_grow(sort_memory).map_err(|err| {
            resources_datafusion_err!(
                "Failed to reserve memory for sort during spill: {err}"
            )
        })?;

        let sorted_iter = IncrementalSortIterator::new(
            emit,
            self.spill_state.spill_expr.clone(),
            self.batch_size,
        );
        let spillfile = self
            .spill_state
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "HashAggSpill",
            )?;

        // Shrink the memory allocated for sorting; sorting is done.
        self.reservation.shrink(sort_memory);

        match spillfile {
            Some((spillfile, max_record_batch_memory)) => {
                self.spill_state.spills.push(SortedSpillFile {
                    file: spillfile,
                    max_record_batch_memory,
                })
            }
            None => {
                return internal_err!(
                    "Calling spill with no intermediate batch to spill"
                );
            }
        }
        Ok(())
    }
}
```

Notice the ordering: state is emitted and the hash table's memory is
released (`clear_shrink(0)`) *before* the operator reserves memory for
sorting. If it reserved sort memory first, a tightly-budgeted pool could
refuse the request even though releasing the hash table would have made
plenty of room — the operator would deadlock against its own reservation.
Releasing first, then re-reserving only what sorting needs, is what keeps
the memory pool's accounting and the operator's actual peak usage in sync.
Sort output is written through `SpillManager` (grouped by group keys, so
later merging can proceed by streaming comparison rather than by re-sorting
everything together), and the sort reservation is shrunk again the moment
sorting finishes — an operator should hold a reservation for exactly as long
as it needs the memory, not for the rest of its lifetime.

## Invariants and edge cases

- **`grow`/`try_grow` and `shrink` calls on a reservation must balance.**
  A `MemoryReservation` that is grown but never shrunk (or dropped) leaks
  budget from the pool's perspective, starving every other consumer even
  after the operator that leaked it has finished.
- **`try_grow` failure is an expected control-flow branch for spill-capable
  operators, not an error to propagate.** An operator that cannot spill
  (most cannot) must instead let the failure surface as a real
  out-of-memory `Result::Err` up the execution stack.
- **`RuntimeEnv` is shared; `TaskContext` is per-query.** Two concurrently
  running queries in the same process compete for the same `MemoryPool` and
  the same `DiskManager` temp directory by design — that is what makes
  `FairSpillPool` meaningful.
- **Spill files must be cleaned up even on error or cancellation.**
  `DiskManager` ties spill file lifetime to handles it hands out, so a
  dropped operator's temp files are removed rather than accumulating on
  disk across a long-running process.

## Trade-offs

Routing every allocation through an explicit reservation API — rather than
letting operators allocate Rust memory freely and only checking a global
counter periodically — adds bookkeeping to every operator that wants to
participate in memory management. In exchange, it makes memory limits
*enforceable* rather than *advisory*: `try_grow` can refuse before an
allocation happens, instead of discovering after the fact that the process
overshot its budget. The cost falls on operator authors (every
spill-capable operator has to implement the grow/shrink/spill dance
correctly); the benefit falls on operators and users that need a hard
memory ceiling, which is most production deployments.

## Key takeaways

- `RuntimeEnv` bundles process-scoped execution resources: `MemoryPool`,
  `DiskManager`, `CacheManager`, `ObjectStoreRegistry`.
- Operators account for memory through `MemoryConsumer` and
  `MemoryReservation` against a shared `MemoryPool`; `try_grow` is the
  fallible check-before-allocating call spill-capable operators rely on.
- Spilling follows a strict order: emit and release in-memory state, then
  reserve only the memory the spill operation itself needs, then shrink
  that reservation again once the spill is written.
- `RuntimeEnv` is shared across queries and sessions; `TaskContext` carries
  a reference to it alongside per-query configuration.

## Going deeper

- [Chapter 1 — The Query Lifecycle](./01-query-lifecycle.md)
- [Chapter 8 — Aggregate Functions](./08-aggregate-functions.md)
- [Chapter 10 — Data Sources](./10-data-sources.md)
- [Chapter 13 — Session Management and DataFrame API](./13-session-dataframe-api.md)
