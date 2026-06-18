# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

(none)

---

## Won't Fix

### [HIGH] slotdex: offset-based pagination is not stable across inter-page mutation
- **Where**: strata/src/slotdex/mod.rs:442-497 (get_entries_by_page / get_entries_by_page_slot)
- **What**: Pages are computed from `(page_size, page_index)` offsets, so an insert/remove between page requests can make a later page skip or repeat entries.
- **Reason**: This is the documented, intentional semantics of an offset-based pagination API (identical to SQL `LIMIT`/`OFFSET`). For a static dataset the result is exact (tier boundaries, cross-tier counting, and swap_order symmetry were all verified correct). Satisfying stable cursor pagination would require a different, cursor-based API — a feature addition, not a bug fix. The offset-pagination caveat is now documented on both methods.

### [MEDIUM] common: dirty_count saturates instead of erroring on count overflow
- **Where**: strata/src/common/dirty_count.rs:37-41 (inc)
- **What**: `inc()` clamps the live-entry count at `COUNT_MASK` (2^63-1) rather than signalling overflow.
- **Reason**: Bit 63 is the dirty flag; saturating below it is the intended, unit-tested behavior (`inc_saturates_below_dirty_bit`). The clamp is only reachable at 2^63 live entries — physically impossible (would require exabytes of storage). Changing well-tested behavior for an unreachable condition adds risk with no practical benefit.

### [LOW] btree: bulk_load can leave a trailing leaf/internal node below minimum occupancy
- **Where**: strata/src/basic/persistent_btree/mod.rs:978-1016 (bulk_load)
- **What**: `entries.chunks(MAX_KEYS)` and the internal-level grouping can produce a trailing node with fewer than `MIN_KEYS` keys / `MIN_KEYS+1` children, relaxing INV-BT3.
- **Reason**: Pure balance/space relaxation with no correctness impact — search, iteration, fork, and remove all return correct results (covered by `bulk_load_lone_trailing_child_then_remove`). The historically panic-causing case (a lone trailing child → mixed-height tree) is already guarded. A complete fix must rebalance both the leaf and internal trailing nodes on the COW path that feeds `three_way_merge`; the added complexity/corruption risk is disproportionate to a cosmetic occupancy nicety.

### [LOW] encoding: blanket KeyEnDe cannot type-level-reject non-deterministic key types
- **Where**: strata/src/common/ende.rs:153-189 (blanket KeyEn/KeyEnDe impls)
- **What**: The ergonomic blanket impl makes every `Serialize` type a key, including `HashMap`/`HashSet`/floats whose encoding is non-deterministic or non-canonical.
- **Reason**: Enforcing this at the type level (sealed/marker trait) is a breaking API change affecting all downstream users and internal call sites, disproportionate to a footgun that VSDB's own code never triggers. The actionable part — the documentation gap — is fixed: `KeyEn` now also warns about floats and `KeyEnDe` cross-references the unsupported-types list.

### [LOW] trie-smt: internal-node hashing lacks leaf/internal domain separation
- **Where**: strata/src/trie/smt/codec.rs:27-47 (hash_leaf / hash_internal)
- **What**: `hash_leaf` tags its input with `0x01` but `hash_internal` adds no domain byte, so leaf and internal Keccak preimage domains can overlap (classic second-preimage hardening gap).
- **Reason**: Not exploitable in the current code — `verify_proof` walks a fixed structure (exactly 256 `hash_internal` steps above a single `hash_leaf`), so a verifier never reinterprets an internal hash as a leaf and intermediate Keccak outputs are not attacker-controlled. Adding a `0x00` internal tag changes the on-disk/root-hash format, requiring a versioned data + proof migration — disproportionate to a non-exploitable theoretical hardening.

### [LOW] trie-smt: BitPath::from_packed does not re-mask trailing bits of the last byte
- **Where**: strata/src/trie/smt/bitpath.rs:35-40 (from_packed), codec.rs read_bitpath
- **What**: `from_packed` only `debug_assert`s the byte length; it never zeroes bits beyond `bit_len`, relying on a documented caller contract.
- **Reason**: No reachable trigger — the writer (`as_packed`) always emits zeroed trailing bits and cached payloads are Keccak-checksummed before parsing, so malformed bytes are rejected before reaching `from_packed`. Additionally `common_prefix` is already robust to trailing garbage: it caps matches at `min_bits` and uses MSB-first `leading_zeros`, so any valid-bit difference is found before a trailing bit. No correctness impact.

### [LOW] common: instance-meta persistence is non-atomic (truncate-in-place write)
- **Where**: strata/src/common/mod.rs:24-32 (save_instance_meta)
- **What**: `fs::write` truncates then writes; a crash mid-write leaves a truncated meta file.
- **Reason**: Pre-existing codebase-wide convention (core/src/basic/mapx_raw/mod.rs save_meta uses the same `fs::write`); cold explicit-save path; the count is independently recoverable via the dirty-flag mechanism. Fixing one site without the matching core change is inconsistent and the churn/risk is disproportionate to a cold-path durability nicety.

### [LOW] typed-collections: inconsistent decode error handling (unwrap vs pnk!)
- **Where**: mapx_ord_rawkey/mod.rs, mapx_ord/mod.rs, mapx/mod.rs (various iterator methods)
- **What**: Value-decode calls use bare `.unwrap()` in some iterator methods and `pnk!()` in others
- **Reason**: Cosmetic inconsistency only; both panic on corrupt data with identical outcome. Changing would touch ~15 sites across 3 files with no correctness benefit. Not worth the churn risk.

### [LOW] common/macros: entry_or_insert_via_mock! and cow_bytes_bounds! are unnecessarily #[macro_export]
- **Where**: strata/src/common/macros.rs:151-184
- **What**: Helper macros are #[macro_export] making them public, but they are internal implementation details
- **Reason**: Rust's #[macro_export] cannot be scoped to pub(crate). Removing export would break cross-module usage within the crate. Renaming to `__` prefix is a semver break for any external user who discovered them.

### [LOW] error: VsdbError::Trie variant overlap with Other
- **Where**: strata/src/common/error.rs:50-54
- **What**: Merge failures route through VsdbError::Other rather than a dedicated MergeError variant
- **Reason**: No correctness impact; callers cannot distinguish merge errors from other errors without string parsing, but no caller currently needs to. Adding a variant is a future enhancement.
