# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last full audit: 2026-07-02
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

(none)

---

## Won't Fix

### [HIGH] slotdex: offset-based pagination is not stable across inter-page mutation
- **Where**: strata/src/slotdex/mod.rs:442-497 (get_entries_by_page / get_entries_by_page_slot)
- **What**: Pages are computed from `(page_size, page_index)` offsets, so an insert/remove between page requests can make a later page skip or repeat entries.
- **Reason**: This is the documented, intentional semantics of an offset-based pagination API (identical to SQL `LIMIT`/`OFFSET`). For a static dataset the result is exact. Satisfying stable cursor pagination would require a different, cursor-based API — a feature addition, not a bug fix.

### [MEDIUM] common: dirty_count saturates instead of erroring on count overflow
- **Where**: strata/src/common/dirty_count.rs:37-41 (inc)
- **What**: `inc()` clamps the live-entry count at `COUNT_MASK` (2^63-1) rather than signalling overflow.
- **Reason**: Bit 63 is the dirty flag; saturating below it is the intended, unit-tested behavior. The clamp is only reachable at 2^63 live entries — physically impossible (would require exabytes of storage). Changing well-tested behavior for an unreachable condition adds risk with no practical benefit.

### [LOW] btree: bulk_load can leave a trailing leaf/internal node below minimum occupancy
- **Where**: strata/src/basic/persistent_btree/mod.rs:978-1016 (bulk_load)
- **What**: `entries.chunks(MAX_KEYS)` and the internal-level grouping can produce a trailing node with fewer than `MIN_KEYS` keys / `MIN_KEYS+1` children, relaxing INV-BT3.
- **Reason**: Pure balance/space relaxation with no correctness impact — search, iteration, fork, and remove all return correct results. The historically panic-causing case (lone trailing child → mixed-height tree) is already guarded. A complete fix must rebalance both leaf and internal trailing nodes on the COW path that feeds `three_way_merge`; the added complexity/corruption risk is disproportionate.

### [LOW] encoding: blanket KeyEnDe cannot type-level-reject non-deterministic key types
- **Where**: strata/src/common/ende.rs:153-189 (blanket KeyEn/KeyEnDe impls)
- **What**: The ergonomic blanket impl makes every `Serialize` type a key, including `HashMap`/`HashSet`/floats whose encoding is non-deterministic or non-canonical.
- **Reason**: Enforcing this at the type level (sealed/marker trait) is a breaking API change affecting all downstream users and internal call sites, disproportionate to a footgun that VSDB's own code never triggers. The documentation now warns about these types.

### [LOW] trie-smt: internal-node hashing lacks leaf/internal domain separation
- **Where**: strata/src/trie/smt/codec.rs:27-47 (hash_leaf / hash_internal)
- **What**: `hash_leaf` tags its input with `0x01` but `hash_internal` adds no domain byte, so leaf and internal Keccak preimage domains can overlap (classic second-preimage hardening gap).
- **Reason**: Not exploitable in current code — `verify_proof` walks a fixed structure (exactly 256 `hash_internal` steps above a single `hash_leaf`), so a verifier never reinterprets an internal hash as a leaf and intermediate Keccak outputs are not attacker-controlled. Adding a `0x00` internal tag changes the on-disk/root-hash format, requiring a versioned data + proof migration.

### [LOW] trie-smt: BitPath::from_packed does not re-mask trailing bits of the last byte
- **Where**: strata/src/trie/smt/bitpath.rs:35-40 (from_packed), codec.rs read_bitpath
- **What**: `from_packed` only `debug_assert`s the byte length; it never zeroes bits beyond `bit_len`, relying on a documented caller contract.
- **Reason**: No reachable trigger — the writer (`as_packed`) always emits zeroed trailing bits and cached payloads are Keccak-checksummed before parsing, so malformed bytes are rejected before reaching `from_packed`. `common_prefix` is robust to trailing garbage: it caps matches at `min_bits` and uses MSB-first `leading_zeros`.

### [LOW] common: instance-meta persistence is non-atomic (truncate-in-place write)
- **Where**: strata/src/common/mod.rs:24-32 (save_instance_meta)
- **What**: `fs::write` truncates then writes; a crash mid-write leaves a truncated meta file.
- **Reason**: Pre-existing codebase-wide convention (core/src/basic/mapx_raw/mod.rs save_meta uses the same `fs::write`); cold explicit-save path; the count is independently recoverable via the dirty-flag mechanism. Fixing one site without the matching core change is inconsistent and the churn/risk is disproportionate to a cold-path durability nicety.

### [LOW] typed-collections: inconsistent decode error handling (unwrap vs pnk!)
- **Where**: mapx_ord_rawkey/mod.rs, mapx_ord/mod.rs, mapx/mod.rs (various iterator methods)
- **What**: Value-decode calls use bare `.unwrap()` in some iterator methods and `pnk!()` in others.
- **Reason**: Cosmetic inconsistency only; both panic on corrupt data with identical outcome. Changing would touch ~15 sites across 3 files with no correctness benefit. Not worth the churn risk.

### [LOW] common/macros: entry_or_insert_via_mock! and cow_bytes_bounds! are unnecessarily #[macro_export]
- **Where**: strata/src/common/macros.rs:151-184
- **What**: Helper macros are #[macro_export] making them public, but they are internal implementation details.
- **Reason**: Rust's #[macro_export] cannot be scoped to pub(crate). Removing export would break cross-module usage within the crate. Renaming to `__` prefix is a semver break for any external user who discovered them.

### [LOW] error: VsdbError::Trie variant overlap with Other
- **Where**: strata/src/common/error.rs:50-54
- **What**: Merge failures route through VsdbError::Other rather than a dedicated MergeError variant.
- **Reason**: No correctness impact; callers cannot distinguish merge errors from other errors without string parsing, but no caller currently needs to. Adding a variant is a future enhancement.

### [LOW] engine: RESERVED_ID_CNT naming is misleading
- **Where**: core/src/common/mod.rs:53
- **What**: `RESERVED_ID_CNT` (value `4096_0000`) functions as an allocation start offset, not just a count. The name implies it's purely a count, but it's also used as the base for the initial PreAllocator value.
- **Reason**: Cosmetic naming issue with no functional impact. The behavior is correct — the allocator starts at this offset and never allocates below it. Renaming risks missing a reference and introducing a bug for no correctness gain.

### [MEDIUM] encoding: ruc::Result in public trait signatures forces undocumented dependency on implementors
- **Where**: strata/src/common/ende.rs:68-165 (KeyEn, KeyDe, ValueEn, ValueDe, KeyEnDe, ValueEnDe, KeyEnDeOrdered traits)
- **What**: Trait methods return `Result<...>` which resolves to `ruc::Result<T>` = `std::result::Result<T, Box<dyn ruc::err::RucError>>`. The `ruc` crate is not re-exported, so downstream users implementing `KeyEnDeOrdered` for a custom type cannot name the return type.
- **Reason**: The blanket impls cover most use cases. Changing signatures to use a concrete error type or re-exporting `ruc` is a semver-breaking API change. Downstream users who need custom implementations can add `ruc` as their own dependency. Documenting this in the trait docs would help but doesn't require a code change.

### [MEDIUM] trie: VerMapWithProof integration layer has zero test coverage
- **Where**: strata/src/trie/proof.rs (entire file: sync_to_branch, sync_to_commit, try_load_cache, apply_diff, Drop auto-save)
- **What**: `VerMapWithProof<K, V, T>` is the primary user-facing entry point for versioned Merkle commitments. It contains nontrivial logic for cache sync, dirty overlay management, incremental diff application, and Drop-based auto-save — none of which are tested. Existing tests only exercise `MptCalc` and `SmtCalc` directly.
- **Reason**: Feature addition (test coverage), not a bug. The integration logic is straightforward delegation to tested primitives. Adding end-to-end integration tests is desirable future work but doesn't block this audit.

### [MEDIUM] trie: MPT proof tests lack coverage for complex node type combinations
- **Where**: strata/src/trie/test.rs:929-1127
- **What**: MPT proof tests cover basic cases but not: (1) empty trie proof verification against `[0u8; 32]` root, (2) key path divergence at an Extension node, (3) proof path traversing Extension→Branch→Extension chain.
- **Reason**: Feature addition (test coverage), not a bug. The code logic for these cases appears correct from source review. Adding targeted tests would improve regression protection but doesn't address a known defect.

### [LOW] trie: varint/bytes/checksum helpers duplicated across MPT and SMT cache modules
- **Where**: strata/src/trie/cache.rs:274-356 and strata/src/trie/smt/cache.rs:234-315
- **What**: `write_varint`, `read_varint`, `write_bytes`, `read_bytes`, `read_u8`, `compute_checksum`, and `io_err` are near-identical between MPT and SMT cache modules (~80 lines).
- **Reason**: Refactoring opportunity, not a bug. No correctness impact — the code is identical in both places. Extracting common primitives into a shared module would be cleaner but adds no functional value and risks introducing subtle serialization mismatches during the refactor.

### [LOW] trie: SMT query.rs reconstructs full 256-bit BitPath from key_hash on every Leaf visit
- **Where**: strata/src/trie/smt/query.rs:36-37
- **What**: `BitPath::from_hash(key_hash)` allocates a Vec and copies 32 bytes on every Leaf encounter during `get()`. A simple `[u8; 32]` comparison would avoid the allocation.
- **Reason**: Micro-optimization on the hot path. The allocation is small (32 bytes → Vec) and the performance impact is negligible for typical workloads. Not worth the risk of introducing a subtle comparison bug.

### [LOW] vecdex: compact() performs irreversible clear before re-insert
- **Where**: strata/src/vecdex/mod.rs:826
- **What**: `compact()` calls `self.clear()` then re-inserts vectors one at a time. If `insert()` were to fail mid-way, data is unrecoverable (all maps already empty).
- **Reason**: Currently safe because `insert()` can only fail on dimension mismatch, and all vectors were validated at their original insertion time. Map insertions are infallible. The code comment at lines 822-825 already acknowledges this fragility and notes the two-phase commit requirement if insert semantics change. Adding recovery infrastructure for a hypothetical future change is over-engineering.
