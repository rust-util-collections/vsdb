# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [v10.0.0] - 2026-03-19

### Breaking

- **Removed msgpack codec** — CBOR (`serde_cbor_2`) is now the only serde encoding. Existing data serialized with msgpack is incompatible; a migration step is required.
- **Default backend for `vsdb` crate** — `backend_rocksdb` is now enabled by default so that `vsdb = "10.0.0"` works out of the box (previously required explicit feature selection).

### Added

- **MMDB backend** (`backend_mmdb`) — a pure-Rust LSM-Tree alternative to RocksDB. No C/C++ dependency; suitable for cross-compilation and WASM targets.
- **Engine comparison guide** — `strata/docs/engine-comparison.md` with detailed benchmarks of MMDB vs RocksDB.
- **`make all-mmdb`** target and MMDB-specific lint/test/bench targets in Makefile.

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
