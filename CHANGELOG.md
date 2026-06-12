# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **mmdb engine updated to 3.3.0.**
- **`SlotDex::new` now asserts `tier_capacity >= 2`** (previously `> 0`). A capacity of 1 could never terminate tier growth, causing unbounded disk/memory usage.
- **MPT keys are now bounded** — `MptCalc` insert paths reject keys longer than `MAX_MPT_KEY_LEN` (1024 bytes) to prevent stack-overflow crashes from adversarially deep tries. SMT is unaffected (depth hard-capped at 256 bits).
- **`DagMapRaw` mutable-value tombstone guard** — writing an empty value back through `get_mut()`'s `ValueMut` now panics, matching the existing `insert()` guard (the empty byte string is the internal deletion tombstone).
- **`vsdb_set_base_dir` env contract documented** — the function must be called before spawning threads (it performs `env::set_var`). Internal database initialization no longer mutates the process environment, and the `VSDB_CUSTOM_DIR` environment variable is no longer set.

### Fixed

- **`PersistentBTree` crash recovery could reuse NodeIds** — `rebuild_ref_counts` now advances the node allocator past the maximum stored NodeId, preventing post-recovery allocations from overwriting live nodes (cross-snapshot corruption).
- **`VecDex`/`SlotDex` crash-recovery completeness** — mutations after `save_meta()` re-set the persisted dirty bit, and recovery now rebuilds all derived metadata (VecDex: `next_node_id`, `entry_point`, `max_layer`; SlotDex: per-tier floor counts and `Large`-container lengths), not just the entry count.
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
