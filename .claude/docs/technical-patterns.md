# VSDB Technical Bug Patterns

This document catalogs known bug categories for VSDB and its underlying mmdb engine.
Load this document FIRST before performing any review or debug analysis.

---

## Category 1: B+ Tree (COW) Correctness Bugs

### 1.1 COW Violation — In-Place Mutation
**Pattern**: Modifying an existing B+ tree node instead of allocating a new NodeId. Breaks structural sharing — other branches referencing the old node see the mutation.
**Where**: `strata/src/basic/persistent_btree/` — insert, delete, split, merge paths.
**Impact**: Cross-branch data corruption. A commit on branch A silently modifies branch B's state.
**Check**: Every mutation path must allocate a new NodeId and return it up the call stack. The old NodeId must remain untouched.

### 1.2 Node Split Boundary Error
**Pattern**: When a B+ tree node exceeds capacity (B=16, max 32 keys), the split produces an incorrect median key, or the median is placed in both children.
**Where**: `persistent_btree/` — split logic.
**Impact**: Key ordering violation, duplicate keys across children, or missing keys.
**Check**: After split, left child's max key < median < right child's min key. No key appears in both children.

### 1.3 Structural Sharing Leak
**Pattern**: After a node is modified (COW), the old node is not reachable from any live commit but is never garbage-collected.
**Where**: `persistent_btree/` — COW allocation + GC interaction.
**Impact**: Unbounded disk growth — dead nodes accumulate.
**Check**: Every replaced NodeId must be tracked for GC. Verify GC reachability analysis covers all live commits.

### 1.4 Empty Node After Delete
**Pattern**: A delete operation produces an empty internal node that violates B+ tree minimum occupancy.
**Where**: `persistent_btree/` — delete + merge/rebalance logic.
**Impact**: Degraded tree structure, potential panic on empty node access.
**Check**: After delete, verify merge/rebalance triggers when node occupancy drops below minimum.

---

## Category 2: Versioning (VerMap) Correctness Bugs

### 2.1 Reference Count Imbalance
**Pattern**: Creating a branch increments a commit's ref-count, but deleting the branch doesn't decrement it (or vice versa). Also: merging creates a new commit but doesn't correctly adjust parent ref-counts.
**Where**: `strata/src/versioned/` — branch create/delete, commit, merge.
**Impact**: Memory/disk leak (ref never reaches 0, dead commits never GC'd) or premature free (ref reaches 0 while still referenced).
**Check**: Every operation that creates a reference to a commit must increment. Every operation that removes a reference must decrement. Verify ref-count is checked AFTER decrement, not before.

### 2.2 Three-Way Merge Conflict Resolution Error
**Pattern**: The merge algorithm incorrectly resolves a key that was modified on both branches. Source-wins policy should keep the source branch value, but the merge keeps base or target instead.
**Where**: `strata/src/versioned/merge.rs`.
**Impact**: Silent data loss — target branch value overwrites source.
**Check**: For each key: if modified on both branches, source wins. If modified on one, that one wins. If deleted on one and modified on other, the modification wins (or the deletion, depending on policy — verify which).

### 2.3 Commit DAG Cycle
**Pattern**: A merge operation creates a commit whose parent list includes itself (directly or transitively).
**Where**: `strata/src/versioned/` — merge creates a commit with two parents.
**Impact**: Infinite loop during DAG traversal (GC, diff).
**Check**: Verify new commit's parents are always existing commits with lower timestamps/sequence.

### 2.4 Branch Pointer Stale After Rollback
**Pattern**: Rollback moves the branch pointer to an older commit, but the intermediate commits between the old and new position are not properly handled (ref-counts not decremented, or data not preserved for other branches still referencing them).
**Where**: `strata/src/versioned/` — rollback logic.
**Impact**: Orphaned commits (leak) or premature GC of commits still needed by other branches.
**Check**: Rollback must only decrement ref-counts for commits that are no longer reachable from ANY branch, not just the rolled-back branch.

### 2.5 Dirty Flag Not Set on Crash
**Pattern**: A versioning operation (commit, merge, GC) crashes mid-way but the dirty flag was never set, so recovery doesn't detect the inconsistency.
**Where**: `strata/src/versioned/` — dirty flag management.
**Impact**: Silently corrupt version DAG after crash.
**Check**: Dirty flag must be set BEFORE the operation begins, and cleared AFTER it completes atomically.

---

## Category 3: Merkle Trie Bugs

### 3.1 Proof Generation/Verification Mismatch
**Pattern**: The proof generated for a key doesn't verify correctly because the prover and verifier use different node serialization or hash computation.
**Where**: `strata/src/trie/` — proof.rs, mpt/, smt/.
**Impact**: Valid proofs rejected, or invalid proofs accepted.
**Check**: Verify both prover and verifier use identical: (1) node encoding, (2) hash function, (3) path computation (nibble vs bit).

### 3.2 MPT Nibble Path Error
**Pattern**: MPT uses nibble-based paths (4-bit per level, 16-ary trie). A key's nibble path is computed incorrectly, placing it at the wrong trie location.
**Where**: `strata/src/trie/mpt/` — nibble extraction.
**Impact**: Key stored at wrong path — get() returns None for an existing key.
**Check**: Verify nibble extraction: high nibble = `byte >> 4`, low nibble = `byte & 0x0F`. Verify path length = 2 * key_byte_length.

### 3.3 SMT Default Hash Assumption
**Pattern**: Sparse Merkle Tree assumes empty subtrees hash to a well-known default. If the default hash is computed differently during build vs verify, proofs break.
**Where**: `strata/src/trie/smt/` — default hash constants.
**Impact**: Root hash mismatch after rebuild.
**Check**: Verify default hash is a compile-time constant or computed identically everywhere.

### 3.4 Trie Cache Staleness
**Pattern**: Trie disk cache stores a snapshot of the trie at commit C. After new commits modify the trie, the cache is not invalidated.
**Where**: `strata/src/trie/cache.rs`.
**Impact**: Stale Merkle root returned, proof verification fails against current state.
**Check**: Verify cache is keyed by (branch, commit_id) and invalidated or versioned on new commits.

---

## Category 4: Prefix Isolation & Engine Bugs

### 4.1 Prefix Collision
**Pattern**: Two different data structures are assigned the same u64 prefix, causing their keys to collide in the same MMDB shard.
**Where**: `core/src/common/` — PreAllocator, prefix assignment.
**Impact**: Data corruption — one structure reads/overwrites another's data.
**Check**: Verify prefix allocator is monotonic and never recycles a prefix that is still in use.

### 4.2 Shard Routing Mismatch
**Pattern**: Write goes to shard `prefix % 16` but read computes a different shard (e.g., using the full key hash instead of the prefix).
**Where**: `core/src/common/engine/mmdb.rs` — shard selection.
**Impact**: Read returns None for an existing key.
**Check**: Verify both read and write paths compute shard index identically: `prefix_bytes % NUM_SHARDS`.

### 4.3 MMDB Singleton Initialization Race
**Pattern**: Two threads call vsdb init simultaneously, both try to open MMDB, one creates the singleton while the other gets a partially-initialized reference.
**Where**: `core/src/common/` — global DB initialization.
**Impact**: Crash or undefined behavior from partially-initialized DB.
**Check**: Verify initialization uses `Once` or equivalent single-init guarantee.

### 4.4 WriteBatch Cross-Shard Atomicity
**Pattern**: A logical operation spans multiple prefixes (different shards). If only some shard writes succeed, the operation is partially applied.
**Where**: `core/src/common/engine/mmdb.rs` — cross-shard writes.
**Impact**: Inconsistent state across data structures (e.g., B+ tree node written but parent pointer not updated).
**Check**: Verify whether cross-shard atomicity is needed for the operation. If yes, verify all-or-nothing semantics.

---

## Category 5: Unsafe Code Bugs

### 5.1 Shadow Handle Data Race
**Pattern**: `shadow()` creates a second handle to the same underlying storage. If the caller doesn't enforce single-writer exclusion, two writers mutate concurrently.
**Where**: All collection types with `pub unsafe fn shadow(&self)`.
**Impact**: Data corruption, torn writes, inconsistent B+ tree structure.
**Check**: Every `shadow()` call site must have documented SWMR enforcement. Verify the caller holds an exclusive lock or uses a single-writer architecture.

### 5.2 from_bytes Deserialization of Untrusted Data
**Pattern**: `unsafe fn from_bytes()` reconstructs a handle from raw bytes without validation. If the bytes are corrupted or from a different version, the handle points to garbage.
**Where**: Collection `from_bytes()` / `from_prefix_slice()` methods.
**Impact**: Silently operate on wrong data or panic on access.
**Check**: Verify callers only pass bytes that were produced by the same code version's `to_bytes()`. Verify no external/untrusted input reaches `from_bytes()`.

### 5.3 Raw Pointer Cast in Entry API
**Pattern**: `self.hdr as *mut MapxRaw` creates a mutable pointer from a shared reference. If two threads use the entry API simultaneously, this is UB (aliasing violation).
**Where**: Macro-generated entry API code.
**Impact**: Undefined behavior.
**Check**: Verify entry API is only used under single-writer guarantee (same as shadow() contract).

---

## Category 6: Encoding / Serialization Bugs

### 6.1 Postcard Encoding Non-Determinism
**Pattern**: postcard serialization of the same logical value produces different byte sequences (e.g., HashMap iteration order). Since MMDB uses byte-ordered keys, this breaks lookups.
**Where**: `strata/src/common/ende.rs` — KeyEnDe/ValueEnDe traits.
**Impact**: Key lookup misses — put(k, v) followed by get(k) returns None.
**Check**: Verify key types produce deterministic serialization. HashMap/HashSet keys are NOT safe for KeyEnDe.

### 6.2 Encoding Version Incompatibility
**Pattern**: A new version of postcard (or a changed type definition) produces different bytes for the same logical data, making existing on-disk data unreadable.
**Where**: Any Serialize/Deserialize type used as a key or value.
**Impact**: Data loss on upgrade — existing entries become invisible.
**Check**: Verify postcard version is pinned in Cargo.toml. Verify struct changes use `#[serde(default)]` for backward compatibility.

### 6.3 Node Encoding Mismatch
**Pattern**: B+ tree or trie node encoding (hand-written, not postcard) has a write/read asymmetry. The encoder writes fields in one order, the decoder reads in another.
**Where**: `persistent_btree/` node codec, `trie/node/` codec.
**Impact**: Corrupted nodes on read — keys and children misaligned.
**Check**: Verify encode and decode process fields in identical order. Verify round-trip tests exist.
