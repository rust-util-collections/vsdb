# Storage Engine Comparison: MMDB vs RocksDB

vsdb supports two storage backends selected at compile time via feature flags:

- `backend_mmdb` (default) — MMDB, a pure-Rust LSM-Tree engine
- `backend_rocksdb` — RocksDB via C++ FFI

## Performance Profile

The two engines have different strengths. The table below summarizes benchmarks
on a representative workload (10K-100K entries, mixed point and range operations).

### Point Operations

| Operation | MMDB | RocksDB | Winner |
|-----------|------|---------|--------|
| MapxOrd read | 336 ns | 102 us | MMDB (302x) |
| MapxOrd write | 1.8 us | 1.6 us | ~tie |
| MapxOrd remove | 1.6 us | 2.0 us | MMDB (1.2x) |
| Mapx contains_key | 1.6 us | 67 us | MMDB (43x) |
| Mapx random read | 4.5 us | 3.6 us | RocksDB (1.2x) |
| Mapx random write | 3.5 us | 3.9 us | MMDB (1.1x) |

### Range & Iteration

| Operation | MMDB | RocksDB | Winner |
|-----------|------|---------|--------|
| range (1K keys) | 80 us | 694 us | MMDB (8.7x) |
| iter full (10K keys) | 764 us | 1.7 ms | MMDB (2.3x) |
| get_ge | 2.2 us | 568 us | MMDB (264x) |
| get_le | 258 us | 569 us | MMDB (2.2x) |

### Batch Writes

| Operation | MMDB | RocksDB | Winner |
|-----------|------|---------|--------|
| batch write (100 items) | 118 us | 77 us | RocksDB (1.5x) |
| normal write (100 items) | 203 us | 160 us | RocksDB (1.3x) |

### Concurrent Access

| Operation | MMDB | RocksDB | Winner |
|-----------|------|---------|--------|
| independent writes (2t) | 5.1 us | 10.8 us | MMDB (2.1x) |
| hotspot writes (16t) | 46 us | 71 us | MMDB (1.5x) |
| reads (2t) | 1.2 us | 0.9 us | RocksDB (1.4x) |
| mixed r/w (2r+2w) | 6.8 us | 9.7 us | MMDB (1.4x) |

## Key Trade-offs

### Iterator Creation vs Iterator Traversal

This is the most important architectural difference:

- **MMDB iterator creation** is relatively expensive (~100-250 us per iterator).
  Each call to `iter()` / `range()` builds a full iterator stack:
  SuperVersion load, IterSource construction, MergingIterator with heap, DBIterator
  with dedup/visibility/tombstone state.

- **MMDB iterator traversal** is very fast once created. Skiplist `next()` is
  O(1) pointer following; memtable data stays in cache. Range scanning 1K keys
  costs only ~80 us (vs RocksDB's 694 us).

- **RocksDB iterator creation** is cheap (~5-10 us) thanks to its C++ iterator
  pool, arena allocator, and highly optimized construction path.

- **RocksDB iterator traversal** is slower per-entry due to block decoding,
  decompression, and FFI overhead.

**Implications**: Code that creates many short-lived iterators (e.g., a loop
with repeated `range()` calls reading only a few entries each) will be slower
on MMDB. Code that creates one iterator and scans many entries will be faster
on MMDB.

The `SlotDex` module demonstrates this principle: its original design created
7+ iterators per page query for tier traversal, where MMDB's creation cost
was especially visible. Switching tier data to an in-memory `BTreeMap` cache
eliminated tier-level iterator creation entirely, reducing page query latency
from ~1 ms to ~8 us (both backends benefit equally from this optimization).

### When to Choose MMDB

- Pure Rust, no C/C++ dependency — simpler builds, cross-compilation, WASM
- Point reads and ordered iteration are the primary workload
- Low-concurrency write scenarios (single-writer is fine)
- Applications that benefit from prefix-scoped iteration

### When to Choose RocksDB

- Batch-heavy write workloads (WAL group commit, parallel compaction)
- High-concurrency pure-read workloads (optimized block cache)
- Applications that create many short-lived iterators
- Production systems requiring battle-tested durability guarantees

## Switching Backends

```toml
# MMDB (default)
vsdb = "10.0.0"

# RocksDB
vsdb = { version = "10.0.0", default-features = false, features = ["backend_rocksdb"] }
```

The API is identical regardless of backend. No special action is needed
after switching — `SlotDex` tier caches are hydrated lazily on first access.
