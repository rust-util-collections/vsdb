# VecDex — Approximate Nearest-Neighbor Vector Index

VecDex is a persistent, disk-backed vector index using the HNSW (Hierarchical
Navigable Small World) algorithm.  It is built entirely in Rust on top of VSDB's
storage primitives, with no C/C++ dependencies.

Use cases: AI rigs, RAG pipelines, semantic search, recommendation engines,
embedding-based retrieval.

## Quick start

```rust
use vsdb::vecdex::{VecDex, HnswConfig, distance::Cosine};

let cfg = HnswConfig { dim: 768, ..Default::default() };
let mut idx: VecDex<String, Cosine> = VecDex::new(cfg);

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
let restored: VecDex<String, Cosine> = VecDex::from_meta(id).unwrap();
```

## API Reference

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `(config: HnswConfig) -> Self` | Create empty index (in the current ambient namespace) |
| `new_in` | `(ns: &Namespace, config: HnswConfig) -> Self` | Create empty index placed in `ns` |
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
| `set_ef_search` | `(&mut self, ef: usize)` | Update the default search beam width |
| `clear` | `(&mut self)` | Remove all data |
| `compact` | `(&mut self) -> Result<()>` | Rebuild graph from existing vectors |
| `save_meta` | `(&self) -> Result<InstanceId>` | Persist metadata for later recovery (create-time constant; saving once after creation suffices) |
| `from_meta` | `(instance_id: impl Into<InstanceId>) -> Result<Self>` | Recover from saved metadata (a bare `u64` works for default-namespace instances) |

Every mutation (insert, remove, `set_ef_search`, the graph state) is committed
through a single atomic engine write batch, so a crash can never leave the
index internally inconsistent: `from_meta` always returns a coherent index and
never needs to reconcile or rebuild anything.

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

For most use cases, the defaults (`m=16, ef_construction=200, ef_search=50`)
work well up to ~100K vectors.  Increase `ef_search` at query time for higher
recall; increase `m` and `ef_construction` for larger datasets.

## Distance Metrics

| Metric | Formula | When to use |
|--------|---------|-------------|
| `L2` | `sum((a-b)^2)` | General-purpose, geometric distance |
| `Cosine` | `1 - cos(a,b)` | Text embeddings, normalized vectors (most LLM APIs) |
| `InnerProduct` | `-(a . b)` | Maximum inner product search, pre-normalized data |

Custom metrics can be implemented via the `DistanceMetric<S>` trait.

## Filtered Search

`search_with_filter` evaluates a predicate on each candidate's key *during*
the HNSW beam search, not as a post-filter.  Non-matching nodes still
participate in graph traversal (maintaining connectivity) but are excluded
from the final result set.  This gives much better recall than over-fetching
and post-filtering, especially with selective predicates.

```rust
// Find 10 nearest vectors whose key starts with "session-42"
let results = idx.search_with_filter(&query, 10, |k: &String| {
    k.starts_with("session-42")
}).unwrap();
```

To compensate for selective filters, the search internally expands `ef` to
`max(ef * 4, k * 2)` to maintain graph exploration breadth.

## Storage Architecture

All persistent state lives in **one** `MapxRaw` handle, namespaced by a
leading tag byte:

```text
VecDex<K, D, S = f32>
  [0x00 | node_id BE]          -> vector data (postcard Vec<S>)
  [0x01 | layer | node_id BE]  -> neighbor ids (packed u64 LE)
  [0x02 | key bytes]           -> node_id (u64 LE)
  [0x03 | node_id BE]          -> user key bytes
  [0x04 | node_id BE]          -> node max layer
  [0x05]                       -> graph state (entry point, counters)
```

Because every mutation stages its rows and commits them in a single
atomic engine write batch, on-disk state is always internally
consistent — there is no dirty flag and no rebuild-on-recovery path.
The serialized handle metadata (single prefix + creation `HnswConfig`)
is create-time constant.

All data is persisted to MMDB (LSM-Tree); a single-handle index lives in
one shard, which is what makes the whole-mutation write batch atomic.

## Type Aliases

```rust
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
| Generic key types | Public methods require `K: KeyEnDe + ValueEnDe + Clone + Eq + Serialize + DeserializeOwned` |
| Duplicate key handling | Re-insert replaces old vector and rebuilds connections |
| Criterion benchmarks | Insert and search benches at 1K/5K/10K scales |

### Planned (not yet implemented)

| Feature | Priority | Description |
|---------|----------|-------------|
| SIMD-optimized distance | P2 | Architecture-specific SIMD for distance computation |
| f16 / i8 quantized vectors | P3 | Reduced storage and faster distance for large indices |
| Multi-vector per key | P3 | Store multiple embeddings per key (e.g., chunked documents) |
| Runtime metric selection | P3 | Select distance metric at runtime instead of compile-time generic |
| VerMap integration | P3 | Versioned vector index with branching and merge |
