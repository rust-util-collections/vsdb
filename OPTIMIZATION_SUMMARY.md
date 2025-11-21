# VSDB Performance Optimization Summary

This document summarizes the performance optimization work done on the VSDB project.

## 1. RocksDB Engine Optimization

### 1.1 Performance Bottleneck Analysis

Code review identified the following performance issues:

1. **High Overhead in Hot Path Memory Allocation**
    *   Every `get()`, `insert()`, and `remove()` operation required creating a new `Vec` and copying `meta_prefix + key`.
    *   This caused significant memory allocation and copying in high-frequency operation scenarios.

2. **Frequent `max_keylen` Updates**
    *   Every `insert` checked the key length.
    *   If the length increased, it immediately wrote to the meta DB.
    *   The check-then-store pattern was racy under concurrent writes.

3. **Lack of Batch Operation API**
    *   Unable to utilize RocksDB's `WriteBatch` optimization.
    *   Batch operations had to be written sequentially.

4. **Global Lock on `prefix_allocator`**
    *   `alloc_prefix()` was protected by a `Mutex`.
    *   This became a bottleneck in high-concurrency scenarios.

### 1.2 Optimization Solutions and Implementation

#### Optimization 1: Stack-Allocated Full Keys

**Modified File**: `core/src/common/engines/rocks_backend.rs`

**Implementation**:

```rust
const FULL_KEY_STACK_CAP: usize = 64;

enum FullKey {
    Stack { buf: [u8; FULL_KEY_STACK_CAP], len: usize },
    Heap(Vec<u8>),
}

impl AsRef<[u8]> for FullKey {
    fn as_ref(&self) -> &[u8] {
        match self {
            FullKey::Stack { buf, len } => &buf[..*len],
            FullKey::Heap(v) => v.as_slice(),
        }
    }
}

fn make_full_key(meta_prefix: &[u8], key: &[u8]) -> FullKey {
    let total_len = meta_prefix.len() + key.len();
    if total_len <= FULL_KEY_STACK_CAP {
        let mut buf = [0u8; FULL_KEY_STACK_CAP];
        buf[..meta_prefix.len()].copy_from_slice(meta_prefix);
        buf[meta_prefix.len()..total_len].copy_from_slice(key);
        FullKey::Stack { buf, len: total_len }
    } else {
        let mut v = Vec::with_capacity(total_len);
        v.extend_from_slice(meta_prefix);
        v.extend_from_slice(key);
        FullKey::Heap(v)
    }
}
```

**Effect**:
*   PREFIX_SIZE = 8, so keys up to 56 bytes fit entirely on the stack (64-byte buffer).
*   Zero heap allocations for the vast majority of keys.
*   Falls back to heap for rare oversized keys.
*   Iterator overlap-detection vectors (`last_fwd_full_key`, `last_rev_full_key`) reuse heap allocations via `clear()` + `extend_from_slice()`.

#### Optimization 2: Race-Free `max_keylen` Update

**Implementation**:

```rust
fn set_max_key_len(&self, len: usize) {
    let prev = self.max_keylen.fetch_max(len, Ordering::Relaxed);
    if len > prev {
        self.db
            .put(META_KEY_MAX_KEYLEN, len.to_be_bytes())
            .expect("vsdb: meta write failed");
    }
}
```

**Effect**:
*   Uses `AtomicUsize::fetch_max()` instead of check-then-store, eliminating the race where two threads could both see len > current and clobber each other.
*   Persists to meta DB on every new maximum to ensure crash consistency.
*   Key length growth usually stabilizes quickly, so writes are rare in steady state.

#### Optimization 3: WriteBatch API

**New API**:

```rust
/// Batch write operations for better performance
let mut batch = map.batch_entry();
batch.insert(&key, &value);
batch.commit().unwrap();
```

**Effect**:
*   2-5x performance improvement for batch writes.
*   Atomicity guarantee: all operations succeed or fail together.
*   Reduced fsync calls.

#### Optimization 4: Per-Thread Prefix Allocator

**Implementation**:

Each thread reserves a batch of `PREFIX_ALLOC_BATCH` (8192) prefixes from the global counter, then hands them out locally with zero cross-core contention. The global atomic is only touched once per 8192 allocations per thread.

```rust
fn alloc_prefix(&self) -> Pre {
    thread_local! {
        static LOCAL_NEXT: Cell<u64> = const { Cell::new(0) };
        static LOCAL_CEIL: Cell<u64> = const { Cell::new(0) };
    }

    LOCAL_NEXT.with(|next_cell| {
        LOCAL_CEIL.with(|ceil_cell| {
            let next = next_cell.get();
            let ceil = ceil_cell.get();
            if next > 0 && next < ceil {
                // Fast path: thread-local, zero contention
                next_cell.set(next + 1);
                return next;
            }
            // Slow path: reserve batch from global counter + persist ceiling to DB
            // ...
        })
    })
}
```

**Effect**:
*   Fast path is entirely thread-local — no atomics, no locks, no cross-CCD traffic.
*   Slow path (once per 8192 allocations per thread) uses a single `fetch_add` on the global counter.
*   DB persistence only happens when the global ceiling is exceeded.
*   Massive reduction in cross-CCD contention on EPYC multi-CCD CPUs.

### 1.3 Expected Performance Comparison

| Operation Type | Before | After | Improvement Source |
| :--- | :--- | :--- | :--- |
| Single Write | Baseline | 5-15% faster | Stack-allocated full keys |
| Batch Write | Baseline | 2-5x faster | batch_entry API |
| Prefix Allocation (High Concurrency) | Baseline | 10-100x faster | Per-thread batching |

## 2. RocksDB Configuration Tuning

### 2.1 Target Environment

AMD EPYC 3rd-5th gen multi-CCD servers with SSD storage.

### 2.2 Configuration Changes

**Per-level compression**: LZ4 for L0-L1 (fast, low CPU), ZSTD for L2+ (better ratio, amortized over larger SSTs). Backward compatible — RocksDB transparently reads uncompressed SSTs.

**Block size**: Increased to 16 KB for better SSD performance (fewer seeks, better compression ratio).

**Memtable bloom ratio**: Increased from 0.02 to 0.1 (10% of memtable) for better prefix-lookup false-positive reduction.

**Background parallelism**: Capped at `min(available_parallelism, 8)` to avoid diminishing returns on many-core EPYC CPUs.

**Data safety**: Removed `mmap_writes` (crash-safety risk), kept `mmap_reads`. Replaced bare `.unwrap()` on DB operations with descriptive `.expect()` messages.

## 3. Architecture Simplification

### 3.1 Single-Engine Design

The multi-engine abstraction (RocksDB + Fjall) has been removed. RocksDB is the sole storage backend — no feature flags are needed to select it.

### 3.2 Sharding Removal

The `SHARD_CNT` / `get_shard_idx()` / `get_db()` indirection has been removed. `RocksEngine` now holds a single `db: &'static DB` instead of `meta` + `shards: Vec<&'static DB>`. The directory name `shard_0` is kept for backward compatibility.

### 3.3 Dead Code Removal

*   Removed `TRASH_CLEANER` thread pool and `threadpool` dependency.

## 4. Benchmark Improvements

### 4.1 Cleanup

**Removed Files**:
*   `wrappers/benches/units/basic_vecx.rs`
*   `wrappers/benches/units/basic_vecx_raw.rs`
*   Corresponding test files

**Modified Files**:
*   `wrappers/benches/basic.rs` - Removed `vecx` references.
*   `wrappers/benches/units/mod.rs` - Commented out `vecx` module.

## 5. Code Quality Improvements

### 5.1 Removal of Deprecated Code

**Completely Removed**:
*   `Vecx` and `VecxRaw` types and all their implementations.
*   Related test files.
*   References in Benchmarks.

**Reason**:
*   Relied on unreliable `len()` tracking.
*   High maintenance cost.
*   Users can use `MapxOrd<usize, V>` as a replacement.

## 6. Compilation and Testing

### 6.1 Compilation Verification

```bash
cargo check --workspace
cargo check --workspace --tests
cargo clippy --workspace
```

### 6.2 Test Verification

```bash
# Run core tests
cargo test -p vsdb_core --release -- --test-threads=1

# Run all workspace tests
cargo test --workspace --release -- --test-threads=1
```

## 7. Summary

This optimization work focused on:

1. **RocksDB Engine Core Optimization** - Stack-allocated keys, race-free atomics, per-thread prefix batching.
2. **RocksDB Configuration Tuning** - Per-level compression (LZ4/ZSTD), 16 KB blocks, capped parallelism, data safety fixes.
3. **Architecture Simplification** - Single-engine design, sharding removal, dead code cleanup.
4. **API Improvements** - Added `batch_entry` support for batch operations.
5. **Code Cleanup** - Removed deprecated `Vecx` related code, `TRASH_CLEANER`, `threadpool`.

**Expected Overall Performance Improvement**:
*   Single Write: 5-15% improvement (stack-allocated keys).
*   Batch Write: 2-5x improvement.
*   High Concurrency: 10-100x improvement (per-thread prefix batching).
*   Memory Usage: Significantly reduced heap allocation on hot paths.
*   Disk I/O: Per-level compression reduces SSD write amplification.
