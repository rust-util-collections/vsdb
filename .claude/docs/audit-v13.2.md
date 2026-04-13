# Audit Findings — v13.2.0

Full codebase audit performed 2026-04-13.

## Confirmed & Fixed

### FIX-1 [HIGH] vecdex: remove corrupts max_layer on entry point deletion

**Where**: `strata/src/vecdex/mod.rs:484-492`
**What**: When entry_point is removed, new entry point is picked by `node_info.iter().next()` (sorted by node_id, not layer). Sets `max_layer` from that arbitrary node, potentially downgrading it and making higher layers unreachable.
**Fix**: Scan all remaining nodes to find the actual global max_layer.

### FIX-2 [LOW] doc-code alignment: lib.rs, review infra missing VecDex

**Where**: `strata/src/lib.rs:1-12`, `.claude/commands/vs-review.md`, `.claude/docs/review-core.md`
**Fix**: Add VecDex to all three locations.

### FIX-3 [LOW] doc-code alignment: .claude/docs/ missing vecdex pattern guide

**Where**: `.claude/docs/patterns/`
**Fix**: Create `vecdex.md` pattern guide for future audits.

## Confirmed — Pre-existing (not introduced by v13.2)

These are real issues found in pre-existing code, documented for future fix.

### PRE-1 [HIGH] dagmap/rawkey: get_mut panics on tombstone after remove

**Where**: `strata/src/dagmap/raw/mod.rs:250-251`, `strata/src/dagmap/rawkey/mod.rs:166`
**What**: `remove()` stores empty bytes `[]` as tombstone. `get_mut()` unwrap-decodes empty bytes and panics.
**Status**: Pre-existing. Tracked for separate fix.

### PRE-2 [HIGH] dagmap/raw: destroy() mutates shared parent Orphan

**Where**: `strata/src/dagmap/raw/mod.rs:378-381`
**What**: `destroy()` sets `self.parent = None` through shared Orphan, corrupting external references.
**Status**: Pre-existing. Tracked for separate fix.

### PRE-3 [MEDIUM] persistent_btree: bulk_load can violate non-root minimum occupancy

**Where**: `strata/src/basic/persistent_btree/mod.rs:880-916`
**What**: Naive chunking can emit non-root internal nodes with < MIN_KEYS entries (e.g., 1285+ entries -> 7-key intermediate node).
**Status**: Pre-existing. Only affects bulk_load path (used by merge). Tracked for separate fix.

### PRE-4 [HIGH] versioning merge: no-op/ancestor merges not short-circuited

**Where**: `strata/src/versioned/map.rs:778-859`
**What**: When src.head == tgt.head, creates redundant merge commit. When one is ancestor of other, creates merge commit instead of fast-forwarding.
**Status**: Pre-existing. Tracked for separate fix.

### PRE-5 [HIGH] encoding: blanket key codec allows nondeterministic Serialize types

**Where**: `strata/src/common/ende.rs:136-145`
**What**: Any `Serialize` type can be used as a key via blanket impl. HashMap/HashSet keys produce nondeterministic bytes, causing lookup misses.
**Status**: Pre-existing design. Users must use deterministic key types. Document as known constraint.

### PRE-6 [HIGH] trie cache: loaded hash not recomputed from structure

**Where**: `strata/src/trie/cache.rs`, `strata/src/trie/proof.rs`
**What**: Cache load trusts stored hash without forcing structural rehash.
**Status**: Pre-existing. Cache is disposable (can be deleted); rehash on load would be expensive. Tracked for optional integrity mode.

### PRE-7 [CRITICAL] versioning: gc_dirty only toggled inside decrement_ref

**Where**: `strata/src/versioned/map.rs:1140-1162`
**What**: Multi-step topology mutations (merge, rollback) persist changes before all ref updates, but gc_dirty is only set inside decrement_ref.
**Status**: Pre-existing architectural issue. Crash between persist and ref-update can leave skewed state.

### PRE-8 [CRITICAL] slotdex: crash can leave index/data inconsistent

**Where**: `strata/src/slotdex/mod.rs:191-207`
**What**: data, tier stores, and total are updated in separate writes with no atomicity.
**Status**: Pre-existing. Acknowledged design limitation of LSM-backed multi-structure updates.

### PRE-9 [HIGH] engine/common: env::set_var unsafe preconditions not enforced

**Where**: `core/src/common/mod.rs:68-71`
**What**: `unsafe { env::set_var(...) }` assumes no concurrent env readers but no hard enforcement.
**Status**: Pre-existing. Runs during LazyLock init (single-threaded by design). SAFETY comments present.

### PRE-10 [MEDIUM] mapx_raw: from_meta can panic on malformed metadata

**Where**: `core/src/basic/mapx_raw/mod.rs:461-463`
**What**: from_meta forwards bytes to unsafe prefix reconstruction without validating length.
**Status**: Pre-existing. Only triggered by corrupted meta files.

### PRE-11 [MEDIUM] slotdex: tier_capacity=1 and u128 truncation edge cases

**Where**: `strata/src/slotdex/mod.rs:141-142`
**What**: tier_capacity=1 allowed, as_u64 truncates u128 values.
**Status**: Pre-existing. Pathological but not crash-inducing.
