# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last full audit: 2026-07-03 (all 9 subsystems, parallel deep review; every
> new finding fixed in the same pass — see "Fixed in the latest full audit").
> Followed by the v14.0.0 design-level overhaul (see the section at the end),
> which additionally resolved four former Won't Fix entries.
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

### [HIGH] dagmap: prune_mainline is not crash-atomic
- **Where**: strata/src/dagmap/raw/mod.rs (prune_mainline)
- **What**: Pruning merges mainline data into the genesis node and clears the intermediate nodes/side branches step by step across multiple storage prefixes. A crash mid-prune can leave a partially merged genesis and a broken parent chain.
- **Reason**: The engine has no cross-prefix write transactions, and unlike VerMap's ref-counts the overlay data cleared mid-merge is not reconstructible from surviving state, so a VerMap-style dirty-flag rebuild cannot repair it. A journaled two-phase prune is new infrastructure disproportionate to this cold, explicit maintenance API. The limitation is documented on `prune` (raw + rawkey), directing callers to snapshot externally first.

### [HIGH] slotdex: offset-based pagination is not stable across inter-page mutation
- **Where**: strata/src/slotdex/mod.rs:387-440 (get_entries_by_page / get_entries_by_page_slot)
- **What**: Pages are computed from `(page_size, page_index)` offsets, so an insert/remove between page requests can make a later page skip or repeat entries.
- **Reason**: This is the documented, intentional semantics of an offset-based pagination API (identical to SQL `LIMIT`/`OFFSET`). For a static dataset the result is exact. Satisfying stable cursor pagination would require a different, cursor-based API — a feature addition, not a bug fix.

### [MEDIUM] common: dirty_count saturates instead of erroring on count overflow
- **Where**: strata/src/common/dirty_count.rs:37-41 (inc)
- **What**: `inc()` clamps the live-entry count at `COUNT_MASK` (2^63-1) rather than signalling overflow.
- **Reason**: Bit 63 is the dirty flag; saturating below it is the intended, unit-tested behavior. The clamp is only reachable at 2^63 live entries — physically impossible (would require exabytes of storage). Changing well-tested behavior for an unreachable condition adds risk with no practical benefit.

### [MEDIUM] engine: failed WriteBatch commit is not retryable
- **Where**: core/src/common/engine/mmdb.rs (MmdbBatch::commit), core/src/common/engine/mod.rs (BatchTrait)
- **What**: `commit` moves the buffered `WriteBatch` into the engine's `write` call; on error the buffered operations are consumed, so a retry on the same batch object commits nothing (and reports success).
- **Reason**: mmdb's `write(batch)` takes the batch by value, so the operations cannot be restored without cloning every batch on the warm path. Elsewhere in the engine, mmdb write failures are treated as fatal (`.expect`). The public typed wrappers already consume the batch (`commit(self)`), making retry impossible there; the remaining `MapxRaw::batch_entry` trait object now documents the non-retryable contract on both `BatchTrait::commit` and `batch_entry`.

### [MEDIUM] trie: MPT proof tests lack coverage for complex node type combinations
- **Where**: strata/src/trie/test.rs (mpt_proof_tests)
- **What**: MPT proof tests cover basic cases but not: (1) key path divergence at an Extension node, (2) proof path traversing Extension→Branch→Extension chain.
- **Reason**: Feature addition (test coverage), not a bug. The code logic for these cases appears correct from source review. Adding targeted tests would improve regression protection but doesn't address a known defect.

### [LOW] btree: bulk_load can leave a trailing leaf/internal node below minimum occupancy
- **Where**: strata/src/basic/persistent_btree/mod.rs (bulk_load)
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
- **Where**: strata/src/common/mod.rs (save_instance_meta)
- **What**: `fs::write` truncates then writes; a crash mid-write leaves a truncated meta file.
- **Reason**: Pre-existing codebase-wide convention (core/src/basic/mapx_raw/mod.rs save_meta uses the same `fs::write`); cold explicit-save path; the count is independently recoverable via the dirty-flag mechanism. Fixing one site without the matching core change is inconsistent and the churn/risk is disproportionate to a cold-path durability nicety.

### [LOW] error: VsdbError::Trie variant overlap with Other
- **Where**: core/src/common/error.rs (VsdbError)
- **What**: Merge failures route through VsdbError::Other rather than a dedicated MergeError variant.
- **Reason**: No correctness impact; callers cannot distinguish merge errors from other errors without string parsing, but no caller currently needs to. The enum is `#[non_exhaustive]`, so a variant can be added compatibly whenever a caller needs it (v14 already added `Decode` and `BaseDirFrozen` this way).

### [LOW] engine: RESERVED_ID_CNT naming is misleading
- **Where**: core/src/common/mod.rs:53
- **What**: `RESERVED_ID_CNT` (value `4096_0000`) functions as an allocation start offset, not just a count. The name implies it's purely a count, but it's also used as the base for the initial PreAllocator value.
- **Reason**: Cosmetic naming issue with no functional impact. The behavior is correct — the allocator starts at this offset and never allocates below it. Renaming risks missing a reference and introducing a bug for no correctness gain.

### [LOW] trie: varint/bytes/checksum helpers duplicated across MPT and SMT cache modules
- **Where**: strata/src/trie/cache.rs:274-356 and strata/src/trie/smt/cache.rs:234-315
- **What**: `write_varint`, `read_varint`, `write_bytes`, `read_bytes`, `read_u8`, `compute_checksum`, and `io_err` are near-identical between MPT and SMT cache modules (~80 lines).
- **Reason**: Refactoring opportunity, not a bug. No correctness impact — the code is identical in both places. Extracting common primitives into a shared module would be cleaner but adds no functional value and risks introducing subtle serialization mismatches during the refactor.

### [LOW] trie: SMT query.rs reconstructs full 256-bit BitPath from key_hash on every Leaf visit
- **Where**: strata/src/trie/smt/query.rs:36-37
- **What**: `BitPath::from_hash(key_hash)` allocates a Vec and copies 32 bytes on every Leaf encounter during `get()`. A simple `[u8; 32]` comparison would avoid the allocation.
- **Reason**: Micro-optimization on the hot path. The allocation is small (32 bytes → Vec) and the performance impact is negligible for typical workloads. Not worth the risk of introducing a subtle comparison bug.

### [LOW] vecdex: compact() performs irreversible clear before re-insert
- **Where**: strata/src/vecdex/mod.rs (compact)
- **What**: `compact()` calls `self.clear()` then re-inserts vectors one at a time. If `insert()` were to fail mid-way, data is unrecoverable (all maps already empty).
- **Reason**: Currently safe because `insert()` can only fail on dimension mismatch, and all vectors were validated at their original insertion time. Map insertions are infallible. The code comment already acknowledges this fragility and notes the two-phase commit requirement if insert semantics change. Adding recovery infrastructure for a hypothetical future change is over-engineering.

---

## Fixed in the latest full audit (2026-07-03)

- **[HIGH] engine**: safe `MapxRaw` serde/from_meta could restore allocator-future prefixes and collide with the next allocation. Safe restore now rejects prefixes outside the allocator-reserved range and reserves accepted recovered prefixes so future allocations skip them; unsafe `from_bytes` remains the explicit trusted escape hatch.
- **[HIGH] typed-collections**: safe restore could type-confuse collection handles by loading one typed wrapper as another. Typed handle metadata now includes and validates the concrete wrapper/type tag.
- **[HIGH] versioning**: dirty recovery rebuilt commit ref-counts but not the branch-name index. Deserialization now rebuilds `branch_names` from `branches`, and branch creation checks branch names against the branch table as the source of truth.
- **[HIGH] versioning**: criss-cross histories with multiple merge bases could violate source-wins by choosing one base. Merge now computes all lowest merge bases and treats disagreeing base values as source-wins conflicts.
- **[HIGH] slotdex**: `save_meta` cleared the dirty bit before writing current metadata, so a crash could make stale tier metadata look clean. Metadata is now written before clearing dirty, and clean restores defensively drop empty tier stacks.
- **[HIGH] vecdex**: dirty recovery could keep stale adjacency to dropped nodes and reuse ids referenced only by stale edges. Recovery now sanitizes adjacency, tracks adjacency ids for `next_node_id`, and relinks nodes without live base edges.
- **[MEDIUM] typed-collections**: `Orphan<T>` implemented `Eq` for `T: PartialEq`, allowing `Orphan<f64>` to violate `Eq` reflexivity. The impl now requires `T: Eq`.
- **[MEDIUM] vecdex**: filtered HNSW search ignored `ef` as a traversal budget and could scan an entire component. Filtered traversal is now bounded by the inflated `ef` visit budget while preserving result-only filtering.
- **[LOW] dagmap**: `DagMapRawKey::destroy` docs still described obsolete per-handle tombstone semantics. Docs now match persistent parent-slot unlink behavior.
- **[LOW] engine**: ungrouped `std` imports in `MapxRaw` tests were fixed.
- **[LOW] typed-collections**: no-op `get_mut()` / `iter_mut()` on `MapxOrdRawKey` rewrote values on drop. Typed mutable wrappers now compare encoded bytes on drop and write back only when the value changed, including safe interior-mutability changes.
- **[MEDIUM] trie**: the former VerMapWithProof integration test-coverage Won't Fix entry is obsolete; integration coverage now exists in `versioned/test.rs`, so the entry was removed.

---

## Fixed in full audit (2026-07-02)

- **[CRITICAL] versioning**: `insert`/`remove`/`discard` released the old dirty root *before* persisting the updated branch pointer; a crash in that window (with compaction triggered by the release) could leave the durable branch state pointing at physically deleted B+ tree nodes. Fixed by persisting the branch state first (matching the ordering already used by `rollback_to`/`merge`/`branch_delete`).
- **[HIGH] vecdex**: dirty recovery could elect a torn, edge-less node as the HNSW entry point (hiding the whole graph) and left crash-orphaned nodes permanently unreachable. Recovery now reconciles all per-node rows (dropping torn inserts/removes), prefers linked entry candidates (also in `remove()`'s re-election), and relinks live nodes whose edge writes were lost.
- **[MEDIUM] vecdex**: recovery did not reconcile `key_to_node`/`node_to_key`/`node_info`, so `contains_key`/`len`/`keys` could disagree with search after a crash. Covered by the same reconciliation pass + regression tests.
- **[MEDIUM] vecdex**: `ef * 4` / `k * 2` filtered-search budget could overflow for extreme public inputs — now `saturating_mul`.
- **[MEDIUM] versioning**: `fork_point(x, x)` / `commit_distance(x, x)` reported nonexistent commit IDs as valid (`Some(x)` / `Some(0)`) — both now validate existence.
- **[MEDIUM] engine**: reading `vsdb_get_custom_dir`/`vsdb_get_system_dir`/`vsdb_get_meta_dir` before `vsdb_set_base_dir` froze derived paths to the old base while `vsdb_set_base_dir` still succeeded, silently splitting the directory tree — derived-dir initializers now freeze the base dir.
- **[LOW] engine**: `with_legacy_mapx_meta_decode` (length-only prefix decode) was a safe `pub` fn — now `unsafe` with a documented trust contract; `BatchTrait::commit` non-retryability documented.
- **[LOW] slotdex**: the test reference model (`testdb::TestDB`) reversed within-slot order on reverse paging, contradicting SlotDex's documented slots-only reversal; rebuilt as `BTreeMap<slot, BTreeSet<key>>` and the workflow test now covers duplicate-slot entries.
- **[LOW] dagmap**: undocumented public contracts — `new()`'s live parent-slot alias, `destroy()`'s per-handle tombstone, and `prune()`'s non-atomicity are now documented (raw + rawkey).
- **[LOW] style**: grouped-import/inline-path violations fixed in slotdex/mod.rs, dagmap/raw/mod.rs, trie/test.rs, vecdex/test.rs; CLAUDE.md unsafe-block inventory updated.

---

## Resolved by the v14.0.0 design overhaul

Former Won't Fix entries whose blocking constraint ("semver-breaking") was
lifted by the major-version bump:

- **[MEDIUM] encoding: `ruc::Result` in public trait signatures** — all
  encoding traits (and every other public API of both crates) now return the
  unified `vsdb_core::common::error::Result`; `ruc` is internal-only.
- **[HIGH] dagmap: destroy() tombstone was per-handle** — each node now owns
  its parent slot (`new(Option<&mut Self>)`), so `destroy()` persists the
  parent unlink; the runtime `destroyed` flag was removed and stale
  clones/shadows/`from_meta` restores observe the destroyed state.
- **[LOW] common/macros: internal macros were `#[macro_export]`** —
  `define_map_wrapper!`, `entry_or_insert_via_mock!`, `cow_bytes_bounds!`
  (vsdb) and `parse_int!`/`parse_prefix!` (vsdb_core) are now crate-private
  via the `macro_rules! + pub(crate) use` pattern.
- **[LOW] typed-collections: unwrap-vs-pnk! decode inconsistency** — all
  internal trusted-decode sites now use assert-style `.unwrap()` (pnk! left
  the decode paths together with `ruc`).

Also swept in v14.0.0: the legacy (pre-magic, length-only) instance-meta
decode path, the deprecated `MapxRaw::from_prefix_slice`/`as_prefix_slice`
aliases, the unused `NULL` root constant, and the `vsdb::SlotDex` /
`vsdb::slotdex::SlotDex` same-name-different-type aliasing confusion.
