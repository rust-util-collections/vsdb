# Changelog

All notable changes to this project will be documented in this file.

## [v14.0.6]

Full-codebase audit sweep (9 parallel subsystem reviews) with every finding
fixed except one documented, deliberate exception.

### Breaking

- **`DagMapRaw`/`DagMapRawKey<V>` no longer implement `Default`.** The derived impl silently performed real disk I/O (an eager write through `Orphan::new()`'s parent slot) on every call, so generic code (`mem::take`, `Option::unwrap_or_default()`, `HashMap::entry().or_default()`) could create orphaned, unreclaimable on-disk state without any visible indication. Use `DagMapRaw::new(None)` / `DagMapRawKey::new(None)` explicitly. **Migration**: replace any `Default::default()`/`mem::take`/`.or_default()` usage on these types with an explicit `new(None)` call.
- **`TrieCalc`/`MptCalc`/`SmtCalc` now return `vsdb::Result<T>` (`VsdbError`), not the internal `TrieError`.** This closes a gap in the "single error type" invariant (`vsdb_core::common::error::VsdbError` is documented as the only error type across both crates' public APIs). `TrieError` is still exported (`vsdb::trie::TrieError`) for downstream matching via `VsdbError::Trie { detail }`, but is no longer the error type of the trie trait/struct methods themselves. **Migration**: replace `Result<T, vsdb::trie::TrieError>` bounds/matches with `vsdb::Result<T>` / `VsdbError::Trie`.
- **`vsdb_core::common::atomic_write_file` (and its `vsdb::common` re-export) now returns `Result<()>` (`VsdbError`) instead of `std::io::Result<()>`.** The only other raw-error-type leak found in either crate's public surface. **Migration**: handle `VsdbError::Io` instead of `std::io::Error`.

### Fixed

- **`MptCalc`/`SmtCalc` (`insert`/`remove`/`root_hash`/`batch_update`) no longer silently empty the trie/tree on a rejected mutation.** All four methods `mem::take` the root into a local working value before the fallible operation; previously, on `Err` (concretely reachable via `MptCalc::insert`/`batch_update` when a key exceeds `MAX_MPT_KEY_LEN`, a public 1024-byte constant), the function returned early without restoring `self.root`, permanently replacing the entire trie with an empty one. All eight methods now unconditionally restore `self.root`/`self.trie` from the working value before propagating any error — a rejected `batch_update` still applies operations before the failing one (documented as non-atomic) but never discards unrelated prior state. This also fixes `VerMapWithProof::merkle_root`, which could otherwise silently return the empty-trie root hash for legitimate, unmodified committed data after such a rejection desynced its incremental-sync bookkeeping from the trie's actual content.
- **`MmDB::new()` no longer leaks already-opened shard handles (and their background compaction threads) if a later shard or the meta-init step fails to open.** Shards are now opened into an owned, non-`'static` `Vec<DB>` first — so a mid-loop failure drops (and cleanly closes) every already-opened shard via normal `Drop` glue — and only `Box::leak`'d after every fallible step has succeeded.
- **SlotDex crash recovery now eagerly rebuilds the tier-acceleration stack** instead of leaving it empty until the next `insert()`. Previously, `ensure_count()` correctly discarded the (potentially skewed) tier stack on unclean-shutdown detection but deferred rebuilding it, silently degrading every pagination query to an O(N) raw scan (measured ~950–2600× slower at 200k entries) for as long as the process stayed idle or read-only after the crash.
- **`SmtMut::remove` no longer discards cached ancestor hashes on a no-op removal** (removing a key that shares a path prefix with real data but isn't actually present). Mirrors MPT's existing `rewrap`-based no-change path: `remove_rec` now threads a `changed` flag and restores the prior `Cached` hash when neither child subtree actually changed, instead of unconditionally reconstructing (and later re-hashing) the node via `compact`.
- **`DagMapRaw::is_dead()` now recognizes tombstoned entries.** `remove()` writes an empty-value tombstone rather than deleting outright (existing, documented convention also used by `get`/`get_mut`); `is_dead()` previously checked only for a literally-empty backing store, so a node whose sole key was removed incorrectly reported `is_dead() == false`.
- **`Mapx::keys()`/`MapxOrd::keys()` no longer decode values.** Both previously routed through `iter().map(|(k, _)| k)`, which unconditionally decoded `V` per entry (an `engine::Mapx::deserialize` call, including lock acquisition, for nested-VSDB-collection value types) before discarding it. A new `MapxOrdRawKey::keys()` decodes only the raw key bytes; `Mapx`/`MapxOrd::keys()` now build on it, decoding only `K`.
- **`core/src/common/engine/mmdb.rs`'s `PENDING_WINDOWS` registry no longer grows unboundedly for the life of the process.** A thread-per-task workload (e.g. a thread-per-request server) previously accumulated one entry per historical thread (since `ThreadId`s are never reused and no cleanup path existed). A `thread_local!` guard now removes a thread's entry via `Drop` when that thread exits — always safe, since a dead thread's un-issued batch tail can never be issued by any other thread.

### Added

- `from_prefix_slice` (core engine, `unsafe fn`) now has a `# Safety` doc comment at its definition, matching every other `unsafe fn` in the crate.
- `Orphan`/`Mapx`/`MapxOrd`/`MapxOrdRawKey`'s `from_meta()` now documents the aliasing hazard it shares with `shadow()` (restoring while the original handle is still live creates a second handle to the same storage) — previously this was undocumented on the one restore path that isn't `unsafe`.
- Typed collections' `batch_entry()` doc comments now state the raw layer's existing "failed commit is not retryable" caveat.
- Regression tests: rejected-mutation trie root preservation (MPT insert/batch_update), `VsdbError`-typed trie errors, SMT no-op-remove cache preservation, SlotDex crash-recovery tier rebuild (both the dirty-flag and invalid-empty-tier detection paths), DagMap tombstone-aware `is_dead()`, `keys()` never decoding values (typed collections), and a `PENDING_WINDOWS` thread-exit cleanup test.

### Won't Fix

- **`VecDex::compact()` remains non-atomic across a hard crash** (documented in `docs/audit.md`): a true fix requires a prefix-swap/version-indirection redesign, and a naive version would silently desync any earlier `save_meta`/parent-collection reference to the index — a worse failure mode (silent staleness) than the current rare-crash-window data loss it would trade away.

## [v14.0.5]

### Breaking

- **SMT hash domain switched to the Diem/JMT leaf-shortcut construction.** A subtree holding exactly one leaf now commits to `Keccak256(0x01 || key_hash || value)` directly — independent of depth — instead of folding the leaf hash through its ~246 residual path levels; internal nodes are unchanged (`Keccak256(0x00 || left || right)`, compressed internal prefixes still wrap through empty siblings). All SMT root hashes change. The SMT disk-cache format is now v3; v2 caches are rejected cleanly and the trie rebuilds from authoritative data. MPT is unaffected. This removes the dominant O(N × 256) hashing term: whole-tree hashing is now O(N) hash operations.
- **`SmtProof` is now compact (variable-length).** `siblings` holds hashes only from the root down to the terminal lone-leaf/empty subtree on the key's path (O(log N) entries instead of a fixed 256), and the `value: Option<Vec<u8>>` field is replaced by `leaf: Option<([u8; 32], Vec<u8>)>` — the lone leaf occupying the terminal subtree (`leaf.0 == key_hash` ⇒ membership; a different `leaf.0` ⇒ conflicting-leaf non-membership, checked for path-prefix consistency during verification; `None` ⇒ empty-slot non-membership). Use the new `SmtProof::value()` accessor for the proven value. Proof size drops from a fixed 8 KiB to typically well under 1 KiB, and verification folds O(log N) hashes instead of 256.

### Fixed


- **SlotDex reverse paging restored to tier-accelerated complexity** (commit `9758b70`, folded into this release): the v13.4.7 correctness fix had degraded `get_entries_by_page(.., reverse=true)` to a linear reverse scan (~17 ms vs ~10 µs forward at 100k entries). Reverse paging now mirrors the forward path via `locate_page_rstart` — a rightmost-distance offset plus a descending tier-cache locate — returning reverse pages to the 10–35 µs range while preserving the corrected slot-descending / within-slot-ascending semantics.
- **SMT tree walks no longer materialize a path slice per level.** `insert`/`remove`/`get`/`prove` compared the remaining key path by allocating `full_path.slice(depth, 256)` (a bit-by-bit copy) at every internal node; they now use allocation-free offset-based comparison (`BitPath::common_prefix_from` / `starts_with_from`, byte-wise with unaligned assembly). `BitPath` itself is now a zero-allocation inline `[u8; 32]` (paths never exceed 256 bits — a type invariant), with `slice`/`concat` rewritten from per-bit loops to byte-wise shifts, and the cache deserializer now rejects bit lengths over 256 instead of allocating attacker-controlled buffers. Combined with the hash-domain change: 1000-key insert 4.6 ms → 0.77 ms, remove 9.0 ms → 1.5 ms, get 3.9 ms → 0.41 ms, cold root hash 76.6 ms → 0.93 ms, verify 79 µs → 3.2 µs per proof (reference box).
- **`mapx / sequential / iter (5k entries)` bench measured an unbounded dataset.** The iterated map had accumulated entries from all preceding timed write benches (hundreds of thousands and growing), so the reported number was meaningless and unreproducible; the bench now iterates a dedicated 5000-entry map.

### Added

- SMT adversarial proof tests: sibling-list depth extension, sibling truncation, conflicting-leaf prefix substitution, proof compactness, plus the existing tamper/wrong-root suite adapted to the new format.
- Bench symmetry: `smt_batch_update_{100,1000}` and `mpt_prove_100` / `mpt_verify_100` cases in `trie_bench`, with `black_box` hygiene on discarded results.

## [v14.0.4]

Consolidates the unpublished v14.0.2/v14.0.3 work (v14.0.1 is the last published release) plus a full sweep of the deferred audit backlog.

### Breaking

- **Typed-handle instance metadata is envelope-tagged** (on-disk format `VSTYPE02`). Safe restore paths (`serde` / `from_meta`) of `Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`, `VerMap`, `SlotDex`, `VecDex`, and `DagMapRawKey` now embed and validate an 8-byte hash of the concrete wrapper type (including **all** generic parameters — notably `VerMap<K, V>`'s key/value types, `VecDex`'s distance metric, and `DagMapRawKey<V>`'s value type, none of which occur in any field type), so loading persisted metadata under a different type fails loudly instead of silently misreading data. The tag derives from `std::any::type_name`, so persisted metas are additionally tied to the writing build's type paths/compiler rendering — a false rejection is always safer than type confusion. **Migration**: none; re-create metas with `save_meta` (older typed metas are rejected by the magic check).
- **Safe prefix restore is validated against the allocator.** `MapxRaw::from_meta` / serde deserialization reject prefixes outside the allocator-issued range and reserve still-pending prefixes so future allocations skip them; `unsafe from_bytes` remains the trusted escape hatch. The fast path is lock-free: allocator state is mirrored in process atomics, previous-run prefixes (the common restore case) are accepted without reservation, and the reservation set stays bounded (pending-window registry).
- **SMT internal-node hashing gained a `0x00` domain byte** (leaf/internal domain separation — standard second-preimage hardening). All SMT root hashes and proofs change; the SMT disk-cache format is now v2 and v1 caches are rejected cleanly (the trie rebuilds from authoritative data). MPT is unaffected. Measured cost: ~2% on SMT insert/get/remove (one extra byte per internal-node Keccak input); all other benchmark suites show no change beyond the noise floor.

### Fixed

- **`DagMapRaw::prune` is now crash-safe** (previously documented as not crash-atomic, directing callers to snapshot externally). The prune is re-phased as **destroy side branches → merge → flush → re-parent → flush → clear**: the genesis is enriched *in place* (keeping its instance ID, so pre-prune genesis metas keep resolving) while overlay top-down reads make the enrichment invisible through the head, and nothing is cleared before the merged genesis is durable and every surviving child has been re-pointed at it (the two `vsdb_flush` barriers pin the ordering across the engine's independently-recovered shards). A crash — `kill -9` or power loss — at any point leaves the canonical access paths (genesis, returned head, surviving children) observing either the complete pre-prune or the complete post-prune state, never a torn mix; interrupted-prune leftovers are plain storage leaks that the next prune reclaims, and re-running an interrupted prune either converges or is structurally refused (head clear order: parent → children → data). Destruction walks (`destroy`, `prune_children`, prune's side-branch sweep) now treat the children registry as an index only and verify each child's own parent slot before destroying, so stale double-registrations left by an interrupted prune can never kill a surviving node. Covered by `prune_crash_*` phase-boundary tests asserting value-exact views.
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
