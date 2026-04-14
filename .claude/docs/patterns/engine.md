# Engine & Storage Layer Review Patterns

## Files
- `core/src/common/engine/mod.rs` — engine::Mapx, batch ops, prefix allocation, shard routing
- `core/src/common/engine/mmdb.rs` — MMDB integration, 16-shard DB instances
- `core/src/common/mod.rs` — VSDB singleton, paths, config
- `core/src/basic/mapx_raw/mod.rs` — MapxRaw (untyped KV, prefix-scoped)

## Architecture
- 16 independent MMDB instances (shards)
- Each data structure gets a unique u64 prefix via PreAllocator
- Shard selection: `prefix_bytes % 16` (or similar routing)
- All keys stored as: `[prefix_8_bytes][user_key_bytes]`
- Global singleton: `Box::leak` for 'static lifetime
- WriteBatch per-shard for atomic multi-key operations

## Critical Invariants

### INV-E1: Prefix Uniqueness
No two live data structures may share the same u64 prefix.
**Check**: Verify PreAllocator is monotonically increasing and atomic. Verify freed prefixes are not reallocated while any reference exists.

### INV-E2: Shard Routing Consistency
Read and write paths must compute the same shard for the same prefix.
**Check**: Verify shard selection function is used identically in get(), put(), delete(), iter(). No path should use a different routing formula.

### INV-E3: Prefix Scoping
All keys for a data structure must be prefixed with its u64 prefix. A structure must never read or write keys with a different prefix.
**Check**: Verify key construction always prepends the correct prefix. Verify iteration bounds are prefix-scoped.

### INV-E4: Singleton Safety
The MMDB singleton must be initialized exactly once. Concurrent init attempts must block or fail, never produce two instances.
**Check**: Verify init uses `std::sync::Once`, `OnceLock`, or equivalent.

### INV-E5: Shard Independence
Operations on shard S1 must not affect shard S2. A WriteBatch on one shard must not leak entries to another.
**Check**: Verify WriteBatch is per-shard. Verify no cross-shard WriteBatch exists.

### INV-E6: Iterator Prefix Bound
`iter()` on a MapxRaw must return only keys with the matching prefix, even if MMDB's underlying iterator sees keys from adjacent prefixes.
**Check**: Verify iterator uses prefix-bounded seek and stops at prefix boundary.

## Common Bug Patterns

### Prefix Collision (technical-patterns.md 4.1)
Two data structures allocated the same prefix.
**Trigger**: PreAllocator counter reset after crash, or concurrent allocation without synchronization.

### Cross-Shard Read (technical-patterns.md 4.2)
Write goes to shard A, read checks shard B.
**Trigger**: Prefix bytes interpreted differently on read vs write path (e.g., endianness mismatch).

### Iterator Prefix Leak (INV-E6 violation)
Iterator scans past the prefix boundary and returns keys belonging to a different structure.
**Trigger**: No upper-bound set on iterator, or upper-bound computed incorrectly (e.g., prefix+1 overflows for prefix=u64::MAX).

### Singleton Double Init
Two threads race to initialize the DB, both succeed, creating two independent MMDB instances. Writes go to one, reads to the other.
**Check**: Verify atomic initialization guarantee.

## Review Checklist
- [ ] PreAllocator is monotonic and atomic (never reuses live prefix)
- [ ] Shard routing identical on read/write/delete/iter paths
- [ ] All keys prefixed with structure's u64 prefix
- [ ] Singleton initialized exactly once (Once or equivalent)
- [ ] WriteBatch is per-shard, no cross-shard batches
- [ ] Iterator bounded by prefix (seek + upper bound)
- [ ] prefix=u64::MAX edge case handled for upper bound computation
- [ ] Global state cleanup on close/shutdown
