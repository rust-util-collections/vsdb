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
| `new` | `(config: HnswConfig) -> Self` | Create empty index |
| `insert` | `(&mut self, key: &K, vector: &[f32]) -> Result<()>` | Add or update a vector |
| `search` | `(&self, query: &[f32], k: usize) -> Result<Vec<(K, f32)>>` | k-NN search |
| `search_ef` | `(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<(K, f32)>>` | Search with custom beam width |
| `search_with_filter` | `(&self, query: &[f32], k: usize, predicate: impl Fn(&K) -> bool) -> Result<Vec<(K, f32)>>` | k-NN search with key predicate |
| `search_ef_with_filter` | `(&self, query, k, ef, predicate) -> Result<Vec<(K, f32)>>` | Filtered search with custom beam width |
| `remove` | `(&mut self, key: &K) -> Result<bool>` | Delete by key |
| `len` | `(&self) -> u64` | Number of indexed vectors |
| `is_empty` | `(&self) -> bool` | Whether index is empty |
| `clear` | `(&mut self)` | Remove all data |
| `save_meta` | `(&self) -> Result<u64>` | Persist for later recovery |
| `from_meta` | `(instance_id: u64) -> Result<Self>` | Recover from saved metadata |

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

Custom metrics can be implemented via the `DistanceMetric` trait.

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

VecDex maps the HNSW graph onto VSDB's core storage primitives:

```text
VecDex<K, D, S = f32>
  vectors:      MapxOrd<u64, Vec<S>>       node_id -> vector data
  adjacency:    MapxRaw                     (layer || node_id) -> neighbor_ids
  key_to_node:  Mapx<K, u64>               user key -> node_id
  node_to_key:  MapxOrd<u64, K>            node_id -> user key
  node_info:    MapxOrd<u64, NodeInfo>     node_id -> layer info
  meta:         Orphan<HnswMeta>           entry point, counters, config
```

Adjacency compound key: `[layer: u8][node_id: u64 BE]` = 9 bytes.
Neighbor lists: packed little-endian `u64` arrays.

All data is persisted to MMDB (LSM-Tree) via the standard 16-shard routing.

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

VecDex is `Send + Sync`.  For concurrent read/write access, wrap in
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
| Send + Sync | Safe for multi-threaded use |
| Generic key types | Any `K: KeyEnDe + ValueEnDe + Clone + Eq` |
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
