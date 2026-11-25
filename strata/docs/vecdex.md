# VecDex — Approximate Nearest-Neighbor Vector Index

VecDex is a persistent, disk-backed vector index using the HNSW (Hierarchical
Navigable Small World) algorithm. The HNSW implementation is Rust and runs on
VSDB's storage primitives. The storage backend currently uses native Zstd
compression, whose bundled `zstd-sys` build requires a C toolchain.

Use cases: AI rigs, RAG pipelines, semantic search, recommendation engines,
embedding-based retrieval.

## Quick start

```rust
use vsdb::vecdex::{VecDex, HnswConfig, distance::Cosine};

let cfg = HnswConfig { dim: 4, ..Default::default() };
let embedding_a = [0.1, 0.2, 0.3, 0.4];
let embedding_b = [0.4, 0.3, 0.2, 0.1];
let query_vec = [0.1, 0.2, 0.3, 0.5];
let mut idx: VecDex<String, Cosine> = VecDex::new(cfg).unwrap();

idx.insert(&"doc-a".into(), &embedding_a).unwrap();
idx.insert(&"doc-b".into(), &embedding_b).unwrap();

// k-NN search
let results = idx.search(&query_vec, 10).unwrap();
for (key, distance) in &results {
    println!("{key}: {distance}");
}

// Filtered search — only consider keys matching a predicate
let results = idx.search_with_filter(&query_vec, 10, |k| k.starts_with("doc-"))
    .unwrap();

// Persist and reload
let id = idx.save_meta().unwrap();
drop(idx);
let restored: VecDex<String, Cosine> = VecDex::from_meta(id).unwrap();
```

Recovery through serde or `from_meta` verifies the stored schema and returns a
handle onto the same graph. Serialize writers, including writers using
separately restored handles. Sequential writers reload the committed state
for every mutation or batch chunk. Concurrent readers can use `reader()`
to share the graph. A reader's `read_view()` captures a snapshot for several
queries. Cloning the mutable index deep-copies the graph into a fresh prefix.

## API Reference

Vectors and queries must have all finite components. Inserts, batches and
searches reject NaN and either infinity with `VsdbError::InvalidConfig` when
the vector passes its dimension check; rejected inserts and batches make no
changes. Previously accepted nonfinite stored vectors cause `compact()` to
return `VsdbError::Corrupt` without replacing the persisted graph. Finite
inputs can still overflow a metric's arithmetic; the existing distance
behavior is unchanged.

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `(config: HnswConfig) -> Result<Self>` | Create empty index (in the current ambient namespace); `InvalidConfig` on a bad config; `ReadOnly` in read-only mode |
| `new_in` | `(ns: &Namespace, config: HnswConfig) -> Result<Self>` | Create empty index placed in `ns` |
| `namespace` | `(&self) -> Namespace` | The namespace this index lives in |
| `instance_id` | `(&self) -> InstanceId` | Complete persistent identity (`map_id` + owning namespace) |
| `insert` | `(&mut self, key: &K, vector: &[S]) -> Result<()>` | Add or update a vector |
| `insert_batch` | `(&mut self, items: &[(K, Vec<S>)]) -> Result<()>` | Chunked bulk insert (one atomic batch per chunk) |
| `search` | `(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>>` | k-NN search |
| `search_ef` | `(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>>` | Search with custom beam width |
| `search_with_filter` | `(&self, query: &[S], k: usize, predicate: impl Fn(&K) -> bool) -> Result<Vec<(K, S)>>` | k-NN search with key predicate |
| `search_ef_with_filter` | `(&self, query: &[S], k: usize, ef: usize, predicate: impl Fn(&K) -> bool) -> Result<Vec<(K, S)>>` | Filtered search with custom beam width |
| `remove` | `(&mut self, key: &K) -> Result<bool>` | Delete by key |
| `get` | `(&self, key: &K) -> Option<Vec<S>>` | Fetch vector by key |
| `contains_key` | `(&self, key: &K) -> bool` | Whether a key exists |
| `keys` | `(&self) -> impl Iterator<Item = K> + '_` | Iterate keys |
| `iter` | `(&self) -> impl Iterator<Item = (K, Vec<S>)> + '_` | Iterate key/vector pairs |
| `len` | `(&self) -> u64` | Number of indexed vectors |
| `is_empty` | `(&self) -> bool` | Whether index is empty |
| `set_ef_search` | `(&mut self, ef: usize) -> Result<()>` | Update the default search beam width |
| `clear` | `(&mut self) -> Result<()>` | Remove all data |
| `compact` | `(&mut self) -> Result<()>` | Rebuild graph from existing vectors |
| `save_meta` | `(&self) -> Result<InstanceId>` | Persist metadata for later recovery (create-time constant; saving once after creation suffices) |
| `from_meta` | `(instance_id: impl Into<InstanceId>) -> Result<Self>` | Recover from saved metadata (a bare `u64` works for default-namespace instances) |

Rows, counters, and graph state for each mutation are committed in one atomic
engine batch. Runtime state updates only after successful commits. Validation
and preflight rejection leave the failing batch unapplied, but a persistence
error may leave complete WAL data that recovery replays. Do not automatically
retry an I/O error as though the recovered index were unchanged.

`insert_batch` commits one chunk at a time: a later error can
leave earlier chunks committed. Recovery reads the committed state directly. Atomicity does not imply an fsync
per operation; power loss can lose an unfenced batch. These guarantees require
the single-writer ownership rule above and intact storage.

## Configuration

```rust
pub struct HnswConfig {
    pub m: usize,              // max neighbors per layer (default 16)
    pub m_max0: usize,         // max neighbors at base layer (default 32)
    pub ef_construction: usize, // build beam width (default 200)
    pub ef_search: usize,      // default search beam width (default 50)
    pub dim: usize,            // vector dimensionality (required)
}
```

### Tuning guide

| Parameter | Higher value | Lower value | Typical range |
|-----------|-------------|-------------|---------------|
| `m` | Better recall, slower insert, more disk | Faster insert, lower recall | 8-48 |
| `ef_construction` | Better graph quality, slower build | Faster build | 100-500 |
| `ef_search` | Better recall, slower search | Faster search | m .. 10*m |
| `dim` | — | — | Set to match your embedding model |

Use the defaults as a starting point and measure recall against an exact
search on representative vectors. Increasing `ef_search` trades query work
for recall; `m` and `ef_construction` also affect build cost and graph size.

If traversal exhausts a graph component before filling the beam, search
lazily scans node-info rows and resumes from unvisited live nodes. An
unfiltered beam at least as large as `len()` visits every vector, including
disconnected graphs written by earlier versions. This needs no rebuild or
stored-data rewrite. Smaller beams remain approximate; a restart scan adds
work only when ordinary graph traversal cannot fill the beam.

## Distance Metrics

| Metric | Formula | When to use |
|--------|---------|-------------|
| `L2` | `sum((a-b)^2)` | General-purpose, geometric distance |
| `Cosine` | `1 - cos(a,b)` | Text embeddings, normalized vectors (most LLM APIs) |
| `InnerProduct` | `-(a . b)` | Maximum inner product search, pre-normalized data |

Custom metrics can be implemented via the `DistanceMetric<S>` trait.

### Runtime metric selection

When the metric is decided from configuration or user input instead of
at compile time, use `VecDexDyn<K, S>` with `MetricKind`:

```rust
use vsdb::vecdex::{VecDexDyn, HnswConfig, distance::MetricKind};

let cfg = HnswConfig { dim: 4, ..Default::default() };
let mut idx: VecDexDyn<String> = VecDexDyn::new_with_metric(MetricKind::Cosine, cfg).unwrap();
idx.insert(&"doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
assert_eq!(idx.metric(), MetricKind::Cosine);
```

`VecDexDyn<K, S>` is an alias for `VecDex<K, MetricKind, S>`. Search dispatches
once on the stored metric and uses one reusable vector buffer across visited
nodes. Custom parameterized metrics implement `distance(&self, a, b)` and
provide a deterministic serde encoding for their parameters.

## Filtered Search

`search_with_filter` evaluates a predicate on each candidate's key *during*
the HNSW beam search, not as a post-filter.  Non-matching nodes still
participate in graph traversal (maintaining connectivity) but are excluded
from the final result set. This can improve recall over over-fetching and
post-filtering; recall still depends on the graph, predicate, and search budget.

```rust
use vsdb::vecdex::{VecDex, HnswConfig, distance::Cosine};

let mut idx: VecDex<String, Cosine> =
    VecDex::new(HnswConfig { dim: 4, ..Default::default() }).unwrap();
idx.insert(&"session-42/doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
let query = [0.1, 0.2, 0.3, 0.5];

// Find 10 nearest vectors whose key starts with "session-42"
let results = idx.search_with_filter(&query, 10, |k: &String| {
    k.starts_with("session-42")
}).unwrap();
```

The search keeps expanding until it holds `max(ef, k)` passing candidates
and every remaining frontier node is farther than the worst of them, so a
selective predicate can require visiting more nodes. An exhausted component
resumes from unvisited live nodes, with restart candidates counting toward
the same visit cap. A predicate that almost nothing satisfies is bounded by
`max(64 × max(ef, k), 4096)` evaluated nodes;
beyond it, results can be incomplete — scan the matching keys directly for
such filters.

## Storage Architecture

All persistent state lives in **one** `MapxRaw` handle, namespaced by a
leading tag byte:

```text
VecDex<K, D, S = f32>
  [0x00 | node_id BE]          -> IEEE-754 f32/f64 components, little endian
  [0x01 | layer | node_id BE]  -> neighbor ids (packed u64 LE)
  [0x02 | key bytes]           -> node_id (u64 LE)
  [0x03 | node_id BE]          -> user key bytes
  [0x04 | node_id BE]          -> node max layer
  [0x05]                       -> 34-byte graph state
  [0x06]                       -> immutable creation config, scalar and metric
```

A VSTYPE04 handle carries a nine-byte extension containing layout version 2
and the first eight bytes of Keccak256 of the schema row. Opening the handle
checks this binding, then decodes the dimension, construction settings, scalar
type and metric parameters from the stored schema.

Each mutation or bulk-insert chunk stages related rows in one atomic batch.
Node IDs increase across clear, compaction and key replacement. A point
lookup can therefore read the key mapping and immutable vector with two
ordinary reads. Traversals capture the graph at one engine sequence number.
Construction caches at most 32 MiB of decoded vectors per transaction.
Compaction still stages the full replacement graph in memory.

### Upgrading a v17 index

The separate `vsdb_migrate` crate provides
`vecdex::migrate_v17::<K, D, S>(id)`. After upgrading the universe, run this
converter with exclusive access. Set `D = MetricKind` for v17 runtime metrics.
The converter validates the old type envelope, vector framing, node mappings and graph state. It rewrites the vector framing in
bounded chunks, then commits the new schema and fixed-width state with a WAL
fence before replacing the handle metadata. A retry continues from a cursor
committed with its vector rows. The prefix, node IDs and adjacency lists are
preserved, so searches on the converted graph retain their original neighbors.

## Type Aliases

```rust
use vsdb::vecdex::{VecDex, distance::{L2, Cosine}};

// f32 (default)
pub type VecDexL2<K> = VecDex<K, L2>;
pub type VecDexCosine<K> = VecDex<K, Cosine>;

// f64
pub type VecDexL2F64<K> = VecDex<K, L2, f64>;
pub type VecDexCosineF64<K> = VecDex<K, Cosine, f64>;
```

## Thread Safety

VecDex is `Send + Sync` when its generic parameters (`K`, `D`, `S`) are —
true for all provided metrics and scalar types.
For concurrent read/write access, wrap it in
`parking_lot::RwLock<VecDex<K, D>>`.

---

## Feature Status

### Implemented

| Feature | Notes |
|---------|-------|
| HNSW insert / search / delete | Full algorithm with layer assignment and bidirectional edges |
| L2, Cosine, InnerProduct metrics | Extensible via `DistanceMetric<S>` trait |
| f32 / f64 scalar support | `VecDex<K, D, S = f32>` generic over `Scalar` trait |
| Filtered search | `search_with_filter` — predicate evaluated during beam search, not post-filter |
| Bulk insert | `insert_batch(&[(K, Vec<S>)])` for batch loading |
| Connectivity-aware neighbor selection | HNSW paper Algorithm 4 heuristic for diverse neighbors |
| Index compaction | `compact()` rebuilds graph from existing vectors after heavy churn |
| Configurable M, m_max0, ef_construction, ef_search | Per-index config, ef overridable per-query |
| Disk persistence via MMDB | All graph data persisted; survives restarts |
| save_meta / from_meta | Instance recovery from instance ID |
| Send + Sync | Safe for multi-threaded use when generic parameters permit |
| Generic key types | Public methods require `K: KeyEnDe` |
| Duplicate key handling | Re-insert replaces old vector and rebuilds connections |
| Runtime metric selection | `VecDexDyn<K, S>` + `MetricKind` — metric chosen at construction, persisted in meta; one enum dispatch per search, distance loops stay monomorphized |
| Criterion benchmarks | Insert and search benches at 1K/5K/10K scales |

### Planned (not yet implemented)

| Feature | Priority | Description |
|---------|----------|-------------|
| SIMD-optimized distance | P2 | Architecture-specific SIMD for distance computation |
| f16 / i8 quantized vectors | P3 | Reduced storage and faster distance for large indices |
| Multi-vector per key | P3 | Store multiple embeddings per key (e.g., chunked documents) |
| VerMap integration | P3 | Versioned vector index with branching and merge |
