# VecDex vector search

VecDex stores vectors, their keys and a hierarchical navigable small world
(HNSW) search graph in one shard. Search returns up to `k` key-distance
pairs, ordered by increasing distance. Choose `VecDex<K, D, S>` for a
compile-time metric or `VecDexDyn<K, S>` for a runtime-selected metric.
The scalar type `S` defaults to `f32`.

## Quick start

```rust
use vsdb::{Cosine, HnswConfig, VecDex};

let config = HnswConfig { dim: 2, ..Default::default() };
let mut index = VecDex::<String, Cosine>::new(config).unwrap();
index.insert(&"east".into(), &[1.0, 0.0]).unwrap();
index.insert(&"north".into(), &[0.0, 1.0]).unwrap();

let results = index.search(&[1.0, 0.0], 1).unwrap();
assert_eq!(results[0].0, "east");
let id = index.save_meta().unwrap();
drop(index);
let restored = VecDex::<String, Cosine>::from_meta(id).unwrap();
assert_eq!(restored.len(), 2);
```

## API reference

| Task | Methods |
| --- | --- |
| Create an index | `new(config)` uses the ambient namespace. `new_in(ns, config)` selects a namespace. Use `new_with_metric` or `new_with_metric_in` to supply a metric value. |
| Modify vectors | `insert` adds or replaces a key, `insert_batch` groups writes, and `remove` returns whether it removed a key. `clear` empties the index. |
| Look up or count | `get` returns an optional vector. Use `contains_key`, `keys`, `iter`, `len` and `is_empty` for membership, scans and counts. |
| Search | `search(query, k)` uses the stored beam width. `search_ef(query, k, ef)` overrides it for one call. Both have typed and raw-key filter variants. |
| Capture a read sequence | `reader()` shares the graph. Call `read_view()` on the index or a reader to reuse one snapshot across queries. |
| Retain or copy the graph | `save_meta` saves the handle and returns its `InstanceId`. `from_meta` reopens it in the owning namespace. `clone_in` deep-copies it into a selected namespace. |
| Tune or rebuild | `set_ef_search` persists the default search width. `compact` rebuilds the graph from live vectors. |

### Batch writes and recovery

Vectors and queries must have the configured dimension and contain only
finite components. `insert_batch` validates all input vectors first, keeps
the last occurrence of each key and publishes up to 64 surviving keys per
shard batch. If a later chunk fails, earlier successful chunks remain
committed. For power-loss durability after successful writes, call
`index.namespace().try_sync_wal()`. [WAL fences and error outcomes](../../docs/storage.md#batch-visibility-and-durability)

If a stored adjacency row contains a truncated ID or an ID beyond the
previously allocated range, the mutation returns an error before publishing
that chunk. New edges in the chunk may refer to its newly allocated nodes.
Before replacing a graph, `compact()` validates the vectors, live count and
both key mappings. A missing reverse mapping returns `VsdbError::Corrupt`
and preserves the previously readable vectors.

## Configuration

| Field | Default | Meaning and limits |
| --- | ---: | --- |
| `dim` | 0 | Set to the vector length before creating the index. Valid dimensions are 1 through `u32::MAX`. |
| `m` | 16 | Maximum neighbors per node on upper layers. Must be at least 2. |
| `m_max0` | 32 | Maximum neighbors at the base layer. Must be at least `m`. |
| `ef_construction` | 200 | Beam width when adding a node. Must be positive. |
| `ef_search` | 50 | Default query beam width. A search uses at least `k` slots in the beam. |

### Tuning guide

Measure recall against an exact search on representative vectors. A wider
search beam can improve recall at the cost of more distance evaluations.
Larger neighbor limits can increase graph size, and a wider construction
beam increases build work.

If traversal exhausts a component before filling the beam, search resumes
from unvisited live nodes. An unfiltered beam at least as large as `len()`
visits every vector, including in disconnected graphs written by earlier
versions. Smaller beams can return approximate results.

## Distance metrics

| Metric | Formula | When to use |
|--------|---------|-------------|
| `L2` | `sum((a-b)^2)` | Squared Euclidean distance |
| `Cosine` | `1 - cos(a,b)` | Angular similarity (a zero vector has distance 1) |
| `InnerProduct` | `-(a . b)` | Maximum inner product search |

### Type aliases

The crate exports `VecDexL2<K>` and `VecDexCosine<K>` for `f32` vectors.
`VecDexL2F64<K>` and `VecDexCosineF64<K>` select `f64`. Implement
`DistanceMetric<S>` to supply a custom metric.

### Runtime metric selection

Use `VecDexDyn<K, S>` with `MetricKind` when the application selects a
metric at runtime.

```rust
use vsdb::{HnswConfig, MetricKind, VecDexDyn};

let cfg = HnswConfig { dim: 4, ..Default::default() };
let mut idx: VecDexDyn<String> = VecDexDyn::new_with_metric(MetricKind::Cosine, cfg).unwrap();
idx.insert(&"doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
assert_eq!(idx.metric(), MetricKind::Cosine);
```

`VecDexDyn<K, S>` is an alias for `VecDex<K, MetricKind, S>`. Search dispatches
once on the stored metric and uses one reusable vector buffer across visited
nodes. Custom parameterized metrics implement `distance(&self, a, b)` and
provide a deterministic serde encoding for their parameters.

## Filtered search

`search_with_filter` evaluates the predicate during traversal. Nodes whose
keys fail the predicate can lead to other nodes. Only matching keys enter
the result set. Each search uses one snapshot. If a stored row or engine
read fails, search returns an error. A predicate with side effects may
already have run on some candidates.

```rust
use vsdb::{Cosine, HnswConfig, VecDex};

let mut idx: VecDex<String, Cosine> =
    VecDex::new(HnswConfig { dim: 4, ..Default::default() }).unwrap();
idx.insert(&"session-42/doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
let query = [0.1, 0.2, 0.3, 0.5];

let results = idx.search_with_filter(&query, 10, |key| {
    key.starts_with("session-42/")
}).unwrap();
assert_eq!(results.len(), 1);
```

The search keeps expanding until it holds `max(ef, k)` passing candidates
and every remaining frontier node is farther than the worst of them, so a
selective predicate can require visiting more nodes. An exhausted component
resumes from unvisited live nodes, with restart candidates counting toward
the same visit cap. The filtered base-layer traversal visits at most
`max(64 × max(ef, k), 4096)` distinct node IDs. The unfiltered descent
through upper layers happens first. The search can return fewer than
`k` matches even if more matching keys exist. Use an exact scan when every
matching vector must be considered.

For predicates over the stored representation, `search_with_raw_filter` and
`search_ef_with_raw_filter` receive the bytes produced by `K::encode_key`.
Only the winning keys need decoding. With postcard, a string's length comes
before its contents and integers use varints. For a string prefix such as
`"session-42"`, the typed predicate above compares the decoded text directly.

## Storage architecture

The graph and its configuration occupy one `MapxRaw` prefix. The first byte
of each local key identifies the row type.

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

A `VSTYPE04` handle carries a nine-byte extension containing layout version 2
and the first eight bytes of Keccak256 of the schema row. Opening the handle
checks this binding, then decodes the dimension, construction settings, scalar
type and metric parameters from the stored schema.

Each mutation or bulk-insert chunk stages related rows in one atomic batch.
Node IDs increase across clear, compaction and key replacement. A point
lookup can therefore read the key mapping and immutable vector with two
ordinary reads. Traversals capture the graph at one engine sequence number.
Construction caches at most 32 MiB of decoded vectors per transaction.
Compaction still stages the full replacement graph in memory.

## Thread safety

A `VecDex` is `Send + Sync` when its generic parameters permit it. To keep
writing while a snapshot is in use, capture the view through an independent
`reader()`. Serialize writes through all handles to the same index, including
separately restored handles. Each mutation or batch chunk reloads the committed
graph state. Cloning a mutable index deep-copies the graph to a fresh prefix.
