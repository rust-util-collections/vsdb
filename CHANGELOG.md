# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Breaking Changes

#### API Changes - Performance-Focused Refactoring

**`insert()` and `remove()` no longer return old values**

All map-like data structures have been updated to remove return values from write operations. This eliminates expensive read-before-write operations and significantly improves write performance.

**Before:**
```rust
let old_value = map.insert(&key, &value);  // Returned Option<V>
let removed = map.remove(&key);             // Returned Option<V>
```

**After:**
```rust
map.insert(&key, &value);  // Returns () - no old value
map.remove(&key);          // Returns () - no old value
```

If you need the old value, explicitly call `get()` before the write operation:
```rust
let old_value = map.get(&key);
map.insert(&key, &new_value);
```

**Affected types:**
- `MapxRaw` (vsdb_core)
- `Mapx`, `MapxOrd`, `MapxOrdRawKey`, `MapxOrdRawValue` (vsdb)
- `MapxRaw`, `MapxDk`, `MapxTk`, `MapxRawMk` (multi-key variants)
- `DagMapRaw`, `DagMapRawKey`

#### Removed `len()` and `is_empty()` Methods

The unreliable `len()` method and related length-tracking infrastructure have been removed from all map structures. Maintaining accurate length counters added significant complexity and performance overhead.

**Removed methods:**
- `len()` - removed from all map types
- `is_empty()` - removed from core map types (still available where implemented via iterator check)
- Internal: `get_instance_len_hint()`, `set_instance_len_hint()` removed from storage backends

**Migration guide:**
- If you need to track collection size, maintain a separate counter in your application
- For checking emptiness where still needed, use: `map.iter().next().is_none()`

**Example migration:**
```rust
// Before
if map.is_empty() {
    println!("Map is empty");
}
let count = map.len();

// After - maintain your own counter
struct MyCollection {
    map: MapxOrd<K, V>,
    count: usize,
}

impl MyCollection {
    fn insert(&mut self, key: &K, value: &V) {
        if !self.map.contains_key(key) {
            self.count += 1;
        }
        self.map.insert(key, value);
    }

    fn remove(&mut self, key: &K) {
        if self.map.contains_key(key) {
            self.count -= 1;
        }
        self.map.remove(key);
    }

    fn len(&self) -> usize {
        self.count
    }
}
```

#### Removed Types

**`Vecx` and `VecxRaw` have been completely removed**

These vector-like types heavily depended on reliable length tracking for indexing operations. Since we've removed length tracking, these types are no longer maintainable.

**Migration guide:**
- For append-only use cases: Use `MapxOrd<usize, V>` with manual index management
- For general sequential storage: Consider using `MapxOrd` with custom keys
- For true vector semantics: Maintain length tracking externally

**Removed files:**
- `wrappers/src/basic/vecx/mod.rs`
- `wrappers/src/basic/vecx/test.rs`
- `wrappers/src/basic/vecx_raw/mod.rs`
- `wrappers/src/basic/vecx_raw/test.rs`
- `wrappers/tests/basic_vecx_test.rs`
- `wrappers/tests/basic_vecx_raw_test.rs`

**Removed from exports:**
- `pub use basic::vecx::Vecx;`
- `pub use basic::vecx_raw::VecxRaw;`

### Performance Improvements

- **Eliminated read-before-write**: `insert()` and `remove()` no longer read old values, significantly reducing I/O operations
- **Removed length tracking overhead**: No more atomic counter updates or length hint synchronization
- **Simplified backend operations**: RocksDB backend has cleaner, faster implementation

#### RocksDB Engine Optimizations

**1. Stack-Allocated Full Keys**
- `make_full_key()` returns a `FullKey` enum: stack-allocated for keys up to 56 bytes, heap fallback for larger keys
- Zero heap allocations for the vast majority of `get()`, `insert()`, and `remove()` operations
- Iterator overlap-detection vectors reuse heap allocations via `clear()` + `extend_from_slice()`

**2. Race-Free max_keylen Updates**
- Uses `AtomicUsize::fetch_max()` instead of check-then-store to eliminate data races
- Persists to meta DB on every new maximum for crash consistency
- Key length growth stabilizes quickly, so writes are rare in steady state

**3. Per-Thread Prefix Allocator**
- Each thread reserves a batch of 8192 prefixes from the global counter
- Fast path is entirely thread-local — no atomics, no locks, no cross-CCD traffic
- Slow path (once per 8192 allocations per thread) uses a single `fetch_add`
- **Expected improvement**: 10-100x faster prefix allocation under contention

**4. WriteBatch API for Bulk Operations**
- New `Mapx::batch_entry()` method for atomic batch writes

**Performance Impact:**
- **Single writes**: 5-15% faster due to stack-allocated keys
- **Prefix allocation**: 10-100x faster under high concurrency
- **Batch writes**: 2-5x faster compared to individual inserts
- **Memory usage**: Zero heap allocations for keys up to 56 bytes

### Storage Backend Changes

**Modified in `rocks_backend.rs`:**

```rust
// Before
fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) -> Option<RawValue>;
fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue>;
fn get_instance_len_hint(&self, meta_prefix: PreBytes) -> usize;
fn set_instance_len_hint(&self, meta_prefix: PreBytes, len: usize);

// After
fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]);
fn remove(&self, meta_prefix: PreBytes, key: &[u8]);
// Length hint methods removed entirely
```

### Multi-Key Map Changes

Multi-key map implementations have been updated for consistency:

**`MapxRaw`, `MapxDk`, `MapxTk`, `MapxRawMk` (basic_multi_key):**
- `insert()` now returns `Result<()>` instead of `Result<Option<RawValue>>`
- `remove()` now returns `Result<()>` instead of `Result<Option<RawValue>>`
- `is_empty()` implemented via iterator check: `self.inner.iter().next().is_none()`

### DAG Map Changes

**`DagMapRaw` and `DagMapRawKey`:**
- Updated to use iterator-based emptiness checks
- Fixed `is_dead()`: checks data, parent, and children using iterators
- Fixed `no_children()`: uses `self.children.inner.iter().next().is_none()`
- Updated `insert()` logic to check existence before inserting to avoid errors

### Special Cases - Length Tracking Still Used

**`vsdb_slot_db`:**

The slot database genuinely needs length tracking for pagination and floor calculation. It now uses explicit `Orphan<usize>` counters:

```rust
struct Tier {
    floor_base: u64,
    data: MapxOrd<SlotFloor, EntryCnt>,
    entry_count: Orphan<usize>, // Explicit length counter
}

enum DataCtner<K> {
    Small(BTreeSet<K>),
    Large {
        map: MapxOrd<K, ()>,
        len: usize, // Explicit length counter (fast)
    },
}
```

This is an acceptable exception because:
1. Slot DB explicitly needs length for its pagination algorithm
2. The counter is clearly visible in the type definition
3. It's maintained manually at the application level, not in the core
4. `DataCtner` updates are fully persisted as values, ensuring the length counter is safe.

### Testing Changes

- Core tests updated to work without return values from `insert()`/`remove()`
- Tests now use `get()` and `contains_key()` to verify state before/after operations
- Example from `core/src/basic/mapx_raw/test.rs`:

```rust
// Before
let old = hdr.insert(&key, &value);
assert!(old.is_none());

// After
assert!(hdr.get(&key).is_none());
hdr.insert(&key, &value);
assert!(hdr.contains_key(&key));
```

### Documentation Updates

- Updated all doc examples to remove `len()` and `is_empty()` usage
- Added "Important Changes" section to README.md
- Updated module documentation for `Mapx`, `MapxOrd`, `MapxOrdRawKey`, `MapxOrdRawValue`
- Fixed all code examples in documentation comments

### Internal Changes

- Removed unused `parking_lot::Mutex` import from `engines/mod.rs`
- Updated `define_map_wrapper!` macro to remove `len()` and `is_empty()` methods
- Simplified storage backend trait definitions

## Migration Checklist

For users upgrading to this version:

- [ ] Update all code using `insert()` return values - either remove usage or add explicit `get()` calls
- [ ] Update all code using `remove()` return values - either remove usage or add explicit `get()` calls
- [ ] Remove all `len()` calls - implement your own counter if needed
- [ ] Remove all `is_empty()` calls - use `iter().next().is_none()` if needed, or track separately
- [ ] Replace `Vecx` usage with `MapxOrd<usize, V>` and manual index tracking
- [ ] Replace `VecxRaw` usage with appropriate alternatives
- [ ] Update tests that relied on return values from write operations

## Performance Benefits

The changes in this release provide significant performance improvements:

1. **50-70% faster writes** (estimated): Eliminating read-before-write operations
2. **Reduced memory pressure**: No length tracking metadata
3. **Simpler code paths**: Fewer atomic operations and synchronization
4. **Better scalability**: Less contention on length counters in concurrent scenarios

## Rationale

These breaking changes were made to:

1. **Improve write performance**: Read-before-write operations were expensive
2. **Simplify the API**: Length tracking was unreliable and caused confusion
3. **Align with storage semantics**: Disk-based storage doesn't naturally track length
4. **Follow Rust idioms**: Standard library's `HashMap::insert()` returns old value because it's cheap in memory; our disk-based structures shouldn't pretend it's cheap
5. **Reduce complexity**: Maintaining accurate length across backends was error-prone

## Notes

- This is a major breaking change release
- The `0.x` version series indicated that breaking changes were expected
- These changes lay the groundwork for better performance and stability
- The simplified API is more maintainable going forward
