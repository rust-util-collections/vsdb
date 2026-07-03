# Changelog

All notable changes to this project will be documented in this file.

## [v14.0.4]

Consolidates the unpublished v14.0.2/v14.0.3 work (v14.0.1 is the last published release) plus a full sweep of the deferred audit backlog.

### Breaking

- **Typed-handle instance metadata is envelope-tagged** (on-disk format `VSTYPE02`). Safe restore paths (`serde` / `from_meta`) of `Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`, `VerMap`, `SlotDex`, `VecDex`, and `DagMapRawKey` now embed and validate an 8-byte hash of the concrete wrapper type (including **all** generic parameters — notably `VerMap<K, V>`'s key/value types, `VecDex`'s distance metric, and `DagMapRawKey<V>`'s value type, none of which occur in any field type), so loading persisted metadata under a different type fails loudly instead of silently misreading data. The tag derives from `std::any::type_name`, so persisted metas are additionally tied to the writing build's type paths/compiler rendering — a false rejection is always safer than type confusion. **Migration**: none; re-create metas with `save_meta` (older typed metas are rejected by the magic check).
- **Safe prefix restore is validated against the allocator.** `MapxRaw::from_meta` / serde deserialization reject prefixes outside the allocator-issued range and reserve still-pending prefixes so future allocations skip them; `unsafe from_bytes` remains the trusted escape hatch. The fast path is lock-free: allocator state is mirrored in process atomics, previous-run prefixes (the common restore case) are accepted without reservation, and the reservation set stays bounded (pending-window registry).
- **SMT internal-node hashing gained a `0x00` domain byte** (leaf/internal domain separation — standard second-preimage hardening). All SMT root hashes and proofs change; the SMT disk-cache format is now v2 and v1 caches are rejected cleanly (the trie rebuilds from authoritative data). MPT is unaffected. Measured cost: ~2% on SMT insert/get/remove (one extra byte per internal-node Keccak input); all other benchmark suites show no change beyond the noise floor.

### Fixed

- **Accumulated on-disk data no longer degrades small writes into a permanent multi-ms stall.** Root cause was in the mmdb engine (fixed in **mmdb 4.0.4**, now the minimum dependency): a DB opened with a pre-existing L0 backlog — exactly what accumulates across short-lived processes, since collection handles never delete data on drop — never scheduled compaction (the only routine signal was post-flush), so every write slept in the L0-slowdown band against a stale cached file count, and small workloads never flushed to break the loop (measured ~5000 µs/put steady-state vs ~2 µs/put healthy). mmdb now kicks compaction at `DB::open` and from the slowdown path. On the vsdb side, `l0_compaction_trigger` returned to 4 (8 coincided exactly with mmdb's write-slowdown trigger, leaving background compaction no buffer zone to work in).
- **Merge-base search returns all lowest common ancestors with fork-region locality.** Criss-cross histories with multiple merge bases previously could violate the source-wins policy (one base chosen); the interim multi-base fix walked the full ancestry of both commits per merge/fork-point query. The final implementation is a git-style "paint down to common" walk (max-heap + STALE propagation): all merge bases, cost bounded by the fork region.
- **`PersistentBTree::bulk_load` now meets minimum occupancy (INV-BT3).** The trailing leaf chunk / internal group is rebalanced with its left sibling, so no non-root node is built below `MIN_KEYS` keys (`MIN_KEYS + 1` children).
- **Instance-meta writes are atomic** (`save_meta` / `save_instance_meta` in both crates): tmp file + fsync + rename replaces truncate-in-place `fs::write`, so a crash mid-save can no longer leave a truncated meta file.
- **`BitPath::from_packed` normalizes trailing bits** beyond `bit_len` to zero instead of relying on a caller contract.
- **`VecDex::compact()` pre-validates all vector dimensions** before the irreversible `clear()`, closing the (previously unreachable) mid-rebuild error path.
- The three-way merge decision matrix is now a single shared function for the single-base and multi-base paths (previously duplicated).

### Changed

- SMT point lookups compare the 32-byte key hash directly at leaves (provably equivalent to the former 256-bit path comparison) instead of materializing a `BitPath` per visit.
- The MPT and SMT disk caches share one codec-primitive module (`trie/codec_util.rs`) instead of duplicating varint/bytes/checksum helpers; encodings are byte-for-byte unchanged.
- MPT proof tests now cover divergence inside an Extension node and Extension→Branch→Extension proof chains.
- Internal rename: `RESERVED_ID_CNT` → `PREFIX_ALLOC_START` (private allocator constant; `BIGGEST_RESERVED_ID` public value unchanged).

## [v14.0.0]

### Breaking

- **Unified error type across the whole ecosystem.** `VsdbError` / `Result` now live in `vsdb_core::common::error` (re-exported as `vsdb::common::error`, `vsdb::VsdbError`, `vsdb::Result`). Every public API of **both** crates returns this type — including the `KeyEnDe` / `ValueEnDe` / `KeyEnDeOrdered` encoding traits, collection batch `commit()`, `save_meta` / `from_meta`, and `vsdb_set_base_dir` — all of which previously returned `ruc::Result` (`Box<dyn RucError>`). Implementing the encoding traits for custom types no longer requires a third-party error dependency. `ruc` remains internal-only; boundary conversions preserve the **complete** chain (every frame, with file/line context) via `stringify_chain`, and the new type additionally offers matchable variants (including new `Decode` and `BaseDirFrozen`), `std::error::Error` interop, and `Send + Sync`. The root alias `vsdb::VsdbResult` was renamed to `vsdb::Result`.
- **Legacy (pre-magic) instance-meta decoding removed.** The `with_legacy_mapx_meta_decode` escape hatch and the length-only prefix decode path are gone; deserialization now unconditionally requires the magic-tagged meta format introduced in v13.4. **Migration**: load and re-save (`save_meta`) any instance metas written by pre-v13.4 releases using a v13 build first.
- **Deprecated `MapxRaw::from_prefix_slice` / `MapxRaw::as_prefix_slice` removed** (deprecated since 13.0.0). Use `from_bytes` / `as_bytes`.
- **`DagMapRaw::new` / `DagMapRawKey::new` redesigned**: the signature is now `new(parent: Option<&mut DagMapRaw>) -> Self` (previously `new(&mut Orphan<Option<DagMapRaw>>) -> Result<Self>`). Each node now **owns** its parent slot instead of aliasing a caller-managed `Orphan` shared by all siblings. Consequences: `destroy()` persistently unlinks the node from its parent chain, so stale clones, shadows, and `from_meta`-restored handles can no longer resolve inherited reads through a destroyed node (previously a documented per-handle-tombstone limitation); constructing a node can no longer fail. The on-disk serde format (3-tuple) is unchanged; DAGs whose siblings were created from one shared `Orphan` slot under v13 keep that sharing until the affected nodes are recreated.
- **`vsdb::SlotDex` now names the generic struct** `slotdex::SlotDex<S, K>` instead of silently aliasing `SlotDex64<K>` (the same name previously referred to different types depending on the import path). **Migration**: replace `vsdb::SlotDex<K>` with `vsdb::SlotDex64<K>`.
- **Internal macros un-exported**: `define_map_wrapper!`, `entry_or_insert_via_mock!`, `cow_bytes_bounds!` (vsdb) and `parse_int!` / `parse_prefix!` (vsdb_core) were implementation details accidentally exported via `#[macro_export]`; they are now crate-private.
- **`NULL` constant removed** from both crates' root re-exports (it was an empty byte slice with no in-tree users).
- **mmdb engine updated to 4.0.0** and **`ruc` updated to 11.0.0.** The mmdb 4.0 `DbOptions` dropped the `max_write_buffer_number`, `memtable_prefix_bloom_ratio`, `level_compaction_dynamic_level_bytes`, and `allow_concurrent_memtable_write` tuning knobs (now internal); VSDB no longer sets them and relies on the engine defaults. No on-disk format or public API change.
- **`SlotDex::new` now asserts `tier_capacity >= 2`** (previously `> 0`). A capacity of 1 could never terminate tier growth, causing unbounded disk/memory usage.
- **MPT keys are now bounded** — `MptCalc` insert paths reject keys longer than `MAX_MPT_KEY_LEN` (1024 bytes) to prevent stack-overflow crashes from adversarially deep tries. SMT is unaffected (depth hard-capped at 256 bits).
- **`DagMapRaw` mutable-value tombstone guard** — writing an empty value back through `get_mut()`'s `ValueMut` now panics, matching the existing `insert()` guard (the empty byte string is the internal deletion tombstone).
- **`vsdb_set_base_dir` env contract documented** — the function must be called before spawning threads (it performs `env::set_var`). Internal database initialization no longer mutates the process environment, and the `VSDB_CUSTOM_DIR` environment variable is no longer set.

### Fixed

- **`PersistentBTree` crash recovery could reuse NodeIds** — `rebuild_ref_counts` now advances the node allocator past the maximum stored NodeId, preventing post-recovery allocations from overwriting live nodes (cross-snapshot corruption).
- **`VerMap` working-state crash window** — `insert`/`remove`/`discard` released the old dirty root before persisting the branch pointer; a crash in that window (with compaction triggered by the release) could leave the durable branch state pointing at physically deleted B+ tree nodes. The branch pointer is now persisted first.
- **`VecDex` crash recovery hardening** — dirty recovery now reconciles all per-node rows (dropping torn insert/remove leftovers), prefers entry-point candidates that still have base-layer edges (an edge-less node can no longer hide the whole graph), and relinks surviving nodes whose edge writes were lost.
- **`VecDex`/`SlotDex` crash-recovery completeness** — mutations after `save_meta()` re-set the persisted dirty bit, and recovery now rebuilds all derived metadata (VecDex: `next_node_id`, `entry_point`, `max_layer`; SlotDex: per-tier floor counts and `Large`-container lengths), not just the entry count.
- **`VerMap::fork_point` / `commit_distance` validated inputs** — two identical nonexistent commit IDs were previously reported as their own fork point / distance 0.
- **Base-directory freeze covers derived directories** — reading `vsdb_get_custom_dir` / `vsdb_get_system_dir` / `vsdb_get_meta_dir` before `vsdb_set_base_dir` now freezes the base dir, so a later `vsdb_set_base_dir` fails loudly (`VsdbError::BaseDirFrozen`) instead of silently splitting the directory tree across two bases.
- **Cosine distance small-norm misclassification** — the divide-by-zero guard used machine epsilon as an absolute threshold, classifying all small-magnitude f32 vector pairs (norms ≲ 3.5e-4) as maximally dissimilar. The guard now triggers only on a true zero denominator.
- **MPT read-path subtree cloning** — `get()`, `prove()`, and extension compaction no longer deep-clone entire subtrees per descent step (O(N) → O(depth) per lookup).
- **`DagMapRaw::new` storage leak** — one orphaned engine slot was leaked per node creation; prune also no longer accumulates deletion tombstones in the genesis node.
- **`VerMap::rollback_to` validation gap** — a branch with no commits could be rolled back to any commit in the DAG; it now fails with the documented "not an ancestor" error.
- **`MapxRaw` clone memory usage** — cloning now commits in bounded chunks instead of buffering the entire map in one in-memory write batch.

## [v12.0.0]

### Breaking

- **Replaced CBOR codec with postcard** — `serde_cbor_2` has been removed and replaced with `postcard` as the sole serialization codec. Existing data serialized with CBOR is incompatible; a migration step is required.

## [v11.0.0]

### Breaking

- **Removed RocksDB backend** — MMDB is now the sole storage engine. The `backend_rocksdb` and `backend_mmdb` feature flags have been removed. No C/C++ toolchain required.
- **Commit reference counting** — `Commit` gains a `ref_count: u32` field. `delete_branch` and `rollback_to` immediately hard-delete orphaned commits via cascading ref-count decrement. No manual `gc()` call needed for commit cleanup.
- **`VerMapWithProof`: automatic cache lifecycle** — `save_cache()` and `load_cache_and_sync()` have been removed from the public API. The trie cache is now eagerly saved after each `sync_to_commit` and auto-loaded on construction. No manual calls required.

### Added

- **Commit ref counting** — each commit tracks the number of branch HEADs and child parent-links pointing to it. `commit()`, `create_branch()`, `delete_branch()`, `merge()`, and `rollback_to()` all maintain ref counts automatically.
- **B+ tree in-memory node ref counting** — `PersistentBTree` maintains a `HashMap<NodeId, NodeRef>` for zero-overhead lifecycle tracking. Dead nodes are cascade-released in memory; disk reclamation happens on `gc()` / startup.
- **`VerMapWithProof` auto-cache** — auto-load in `new()`/`from_map()`, eager save after each `sync_to_commit`. A `cache_dirty` flag avoids redundant serialization in read-only scenarios.

### Removed

- `backend_rocksdb` feature flag and all RocksDB-related code, Makefile targets, and documentation.
- `strata/docs/engine-comparison.md` (no longer applicable).
- `pending_gc`, `next_gc_seq`, `process_pending_gc()`, `recover_pending_gc()` — replaced by commit ref counting.

## [v10.0.0] - 2026-03-19

### Breaking

- **Removed msgpack codec** — CBOR (`serde_cbor_2`) is now the only serde encoding. Existing data serialized with msgpack is incompatible; a migration step is required.
- **Default backend for `vsdb` crate** — `backend_mmdb` is now enabled by default so that `vsdb = "10.0.0"` works out of the box without a C/C++ toolchain (previously required explicit feature selection).

### Added

- **MMDB backend** (`backend_mmdb`) — a pure-Rust LSM-Tree alternative to RocksDB. No C/C++ dependency; suitable for cross-compilation and WASM targets.
- **Engine comparison guide** — `strata/docs/engine-comparison.md` with detailed benchmarks of MMDB vs RocksDB.
- **`make all-rocksdb`** target and RocksDB-specific lint/test/bench targets in Makefile (default targets use MMDB).

### Changed

- **SlotDex performance** — tier data backed by an in-memory `BTreeMap` cache (auto-hydrated via `RefCell`), reducing page query latency from ~1 ms to ~8 us.
- Aligned MMDB DB options with RocksDB configuration for consistent behavior.
- Fixed mmdb 2.2 API: replaced removed `prefix_iterator` with `iter_with_prefix`.
- Replaced `unwrap`/`panic` with `c(d!())` error chains; hardened decode bounds.
- Expanded benchmark coverage and fixed methodological issues.
- Bumped dependencies.

### Removed

- Removed `lint-codecs` CI target (no longer needed with single codec).
- Removed RocksDB pre-built static lib cache from Makefile.

## [v9.1.0] - 2026-03-09

### Changed

- **Merged `vsdb_trie_db` and `vsdb_slot_db` into `vsdb`** — they are now modules (`trie` and `slotdex`) instead of separate crates. The workspace is reduced to two crates: `vsdb_core` and `vsdb`.
- **Renamed `trie_db` -> `trie`**, inner `trie/trie` -> `trie/mpt`, `slot_db` -> `slotdex`.
- **Moved `VerMapWithProof`** from `versioned::proof` to `trie::proof`, alongside `MptCalc` and `SmtCalc`.
- **Removed `merkle` feature gate** — the `trie` module (including `sha3` and `thiserror`) is always compiled.
- All public types re-exported from crate root: `MptCalc`, `SmtCalc`, `SmtProof`, `VerMapWithProof`, `SlotDex`.

### Added

- **`SmtCalc`** — Sparse Merkle Tree with 256-level proofs (`prove` / `verify_proof`).
- **SMT cache** — `save_cache` / `load_cache` for `SmtCalc` (disposable on-disk persistence).
- Comprehensive SMT test suite (24 tests) and benchmarks.
- Architecture diagram in `trie` module docs.

## [v9.0.0] - 2026-03-07

### Added

- **`VerMap` convenience APIs**: `branch_id`, `branch_name`, `has_uncommitted`, `range`, `iter_at_commit`, `get_commit` — small, high-value methods for common caller patterns.
- **Versioned benchmarks**: Criterion benchmarks covering single-branch CRUD, commit/rollback, branching, iteration/range, historical reads, three-way merge, and GC.
- **Comprehensive test coverage**: 65 new tests for the new APIs; 136 total versioned tests.

### Fixed

- Fixed all Clippy warnings for Rust 1.93+ (collapsible `if`-let chains, `ptr_arg`, `needless_borrows_for_generic_args`, `type_complexity`).

## [v8.3.0] - 2024-07-27

### Changed

- **License changed from GPL-3.0 to MIT.** The entire project is now licensed under the MIT license, allowing for more permissive use and integration.
