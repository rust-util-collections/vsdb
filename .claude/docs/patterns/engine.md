# Engine & Storage Layer Review Patterns

## Files
- `core/src/common/engine/mod.rs` — engine::Mapx, batch ops, prefix allocation, shard routing
- `core/src/common/engine/mmdb.rs` — MMDB integration, per-namespace shard sets, global allocator backing store, format marker
- `core/src/common/namespace.rs` — Namespace handle, registry, InstanceId, ambient scope, lifecycle
- `core/src/common/mod.rs` — VSDB singleton (default namespace), paths, config
- `core/src/basic/mapx_raw/mod.rs` — MapxRaw (untyped KV, prefix-scoped)

## Architecture
- One engine instance per NAMESPACE; each = N independent MMDB shards
  (default namespace pinned to 16; non-default counts are creation-time
  persisted in the registry and validated at open)
- ONE process-global prefix allocator serves every namespace: prefixes
  are unique across the whole registry by construction; backing store is
  `{default_base}/__SYSTEM__/__prefix_ceiling__` (durable file, take-max
  fold from the legacy shard-0 key at default-engine open)
- Each data structure gets a unique u64 prefix; shard selection:
  `prefix % shard_count` within its owning namespace's engine
- All keys stored as: `[prefix_8_bytes][user_key_bytes]`
- Handles carry their `Namespace` (Arc); ambient scope affects CREATION
  only, never routing; metas embed `ns_id` as an optional 8-byte suffix
  (absent ⇔ default namespace, byte-identical to pre-v16)
- Engines are owned by their `Arc<NsInner>` (no `Box::leak`):
  `vsdb_ns_close` proves exclusivity (strong_count == 1 under
  REGISTRY_LOCK + table lock), removes the entry, per-shard `DB::close`
  (flush + WAL sync, errors surface), then the drop cascade joins
  compaction threads and releases LOCK files; registry mutations +
  namespace opens serialize on `REGISTRY_LOCK` (not-open checks for
  destroy/relocate must run UNDER it — TOCTOU)
- `__SYSTEM__/format_version` marker: written after shard creation
  completes; older binaries refuse newer formats; shard-layout
  validation is completion-aware (marker absent + fewer shards than
  requested = resumable half-created root)
- One `BlockCachePool` per engine: all of that engine's MMDB shards share
  capacity, while mmdb member IDs isolate same-numbered SST files
- WriteBatch per-shard for atomic multi-key operations

## Critical Invariants

### INV-E1: Prefix Uniqueness
No two live data structures may share the same u64 prefix.
**Check**: Verify `alloc_prefix()`/`alloc_prefix_candidate()` coordinate the
global floor/ceiling, thread-local issuance cursors, durable ceiling file, and
recovered-prefix reservations monotonically. Prefixes are never reused.

### INV-E2: Shard Routing Consistency
Read and write paths must compute the same shard for the same prefix.
**Check**: Verify shard selection function is used identically in get(), put(), delete(), iter(). No path should use a different routing formula.

### INV-E3: Prefix Scoping
All keys for a data structure must be prefixed with its u64 prefix. A structure must never read or write keys with a different prefix.
**Check**: Verify key construction always prepends the correct prefix. Verify iteration bounds are prefix-scoped.

### INV-E4: Per-Namespace Engine Uniqueness
The default engine initializes once. A non-default namespace id has at most one
open engine in-process.
**Check**: Verify default one-time initialization and the
`REGISTRY_LOCK` + under-lock `OPEN_NAMESPACES` re-check for non-default opens.

### INV-E5: Shard Independence
Operations on shard S1 must not affect shard S2. A WriteBatch on one shard must not leak entries to another.
**Check**: Verify WriteBatch is per-shard. Verify no cross-shard WriteBatch exists.

### INV-E6: Iterator Prefix Bound
`iter()` on a MapxRaw must return only keys with the matching prefix, even if MMDB's underlying iterator sees keys from adjacent prefixes.
**Check**: Verify iterator uses prefix-bounded seek and stops at prefix boundary.

### INV-E7: Namespace Lifecycle Exclusion
Open/create/destroy/relocate/close for one namespace id/root must not overlap in
a way that produces two engines or mutates an open root.
**Check**: `REGISTRY_LOCK` covers registry read-modify-write and same-id engine
open/teardown. Destroy/relocate perform the not-open check under it.
`OPEN_NAMESPACES`' table lock is released before slow close teardown.

### INV-E8: Format Marker and Shard Completeness
A marked root is complete and must contain exactly its recorded shard set.
Marker-absent roots are resumable only when their partial layout is a valid
prefix of creation; malformed/extra/missing layouts reject loudly.
**Check**: Validate shard count bounds, exact shard identities, each shard's
MMDB `CURRENT` anchor, and marker version before adopting an existing root.

### INV-E9: Namespace Identity and Placement
Handles route through their owned `Namespace`; ambient scope affects creation
only. Serialized metadata's optional namespace suffix is absent exactly for the
canonical default namespace.
**Check**: `from_meta`/`from_bytes_in` and `InstanceId` normalization cannot
silently redirect an existing handle to ambient/default storage.

### INV-E10: Per-Engine Cache-Pool Wiring
Every MMDB shard of one engine attaches to the same per-engine
`BlockCachePool`; different engines do not accidentally share pool identity.
**Check**: Pool capacity is allocated once per engine, each shard gets a member
view, and telemetry reports the intended shard/pool properties.

### INV-E11: Cross-Namespace Clone Cleanup
`clone_in` copies into a fresh, unobservable prefix in bounded independent
batches. On a failed chunk, already-committed target rows must be reclaimed
best-effort before returning the original error.
**Check**: Each chunk uses a fresh batch, the source remains unchanged, and the
error path applies one O(1) wiped batch to the partial target without replacing
the primary failure.

## Common Bug Patterns

### Prefix Collision (technical-patterns.md 4.1)
Two data structures allocated the same prefix.
**Trigger**: Durable ceiling/floor regresses, a recovered prefix is not
reserved, or a local batch cursor issues outside its claimed window.

### Cross-Shard Read (technical-patterns.md 4.2)
Write goes to shard A, read checks shard B.
**Trigger**: Prefix bytes interpreted differently on read vs write path (e.g., endianness mismatch).

### Iterator Prefix Leak (INV-E6 violation)
Iterator scans past the prefix boundary and returns keys belonging to a different structure.
**Trigger**: No upper-bound set on iterator, or upper-bound computed incorrectly (e.g., prefix+1 overflows for prefix=u64::MAX).

### Same-Namespace Double Open
Two threads race to open one namespace root and both create engines.
**Check**: Verify default one-time initialization and serialized, re-checked
non-default open.

### Partial Root Adopted as Complete
Open/create accepts a marker/layout combination that cannot represent a
completed or safely resumable namespace and silently initializes missing data.
**Check**: Treat format marker and exact shard/MMDB anchors as the completion
proof; malformed legacy roots fail instead of being "repaired" by creation.

## Review Checklist
- [ ] Prefix allocator floor/ceiling/local cursors are monotonic; durable ceiling precedes issuance
- [ ] Shard routing identical on read/write/delete/iter paths
- [ ] All keys prefixed with structure's u64 prefix
- [ ] Default initializes once; each non-default namespace id has one cached engine
- [ ] WriteBatch is per-shard, no cross-shard batches
- [ ] Iterator bounded by prefix (seek + upper bound)
- [ ] prefix=u64::MAX edge case handled for upper bound computation
- [ ] Non-default close reclaims its engine; default process-global state remains intentional
- [ ] Namespace open/create/destroy/relocate/close serialize under lifecycle protocol
- [ ] Format marker, shard count/set, and per-shard MMDB anchors agree
- [ ] Handle namespace ownership is independent of ambient creation scope
- [ ] One BlockCachePool is shared by all shards of one engine only
- [ ] `clone_in` uses bounded fresh batches and wipes partial target on error
