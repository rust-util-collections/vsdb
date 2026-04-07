# Persistent B+ Tree Subsystem Review Patterns

## Files
- `strata/src/basic/persistent_btree/` — COW B+ tree with structural sharing

## Architecture
- B=16 (max 32 keys per node), ~4 levels for 1M entries
- Copy-on-write: every mutation allocates a new NodeId
- Structural sharing: unchanged subtrees shared across versions
- Backed by MapxRaw (untyped KV → MMDB)
- Node types: Internal (keys + child NodeIds) and Leaf (keys + values)

## Critical Invariants

### INV-BT1: Copy-On-Write Integrity
Every mutation (insert, delete, update) must allocate a NEW NodeId for every modified node on the path from leaf to root. The old NodeId must remain immutable.
**Check**: Verify no mutation path writes to an existing NodeId. Every modified node returns a new NodeId up the call chain.

### INV-BT2: Key Ordering
Within each node: `keys[i] < keys[i+1]` for all i.
Across parent-child: all keys in `children[i]` < `keys[i]` <= all keys in `children[i+1]`.
**Check**: Verify insert/split/merge maintain ordering. Pay special attention to the median key during splits.

### INV-BT3: Node Occupancy
Internal nodes: min ceil(B/2) children (except root). Leaf nodes: min ceil(B/2) keys (except root).
Max 2*B keys per node.
**Check**: Verify split triggers at 2*B+1 and merge triggers below ceil(B/2). Verify root is exempt from minimum.

### INV-BT4: Structural Sharing Correctness
Two versions that share a subtree must see identical data for that subtree. If version V1 modifies node N, V1 gets a new copy N'; V2 still sees the original N.
**Check**: Verify no path reaches a shared node and modifies it. Verify Arc/reference counting prevents premature deallocation.

### INV-BT5: GC Reachability
A node is garbage if no live commit's root tree can reach it. GC must not collect reachable nodes.
**Check**: Verify GC traverses from ALL live commit roots, not just the latest.

## Common Bug Patterns

### Split Median Misplacement (technical-patterns.md 1.2)
The median key is included in both left and right children after split.
**Trigger**: Insert into a full node → split → median duplicated.
**Check**: After split, median must be in exactly ONE location (promoted to parent or kept in one child, not both).

### In-Place Node Mutation (technical-patterns.md 1.1)
A hot path writes directly to node storage without allocating a new NodeId.
**Trigger**: Any mutation path that skips COW allocation.
**Check**: Grep for direct storage writes in mutation paths.

### Unbalanced Tree After Merge
Delete cascades underflow but merge doesn't properly redistribute or combine nodes.
**Trigger**: Delete many keys from one side of the tree → underflow chain.

## Review Checklist
- [ ] Every mutation path allocates new NodeIds (no in-place writes)
- [ ] Split produces correct median — not duplicated, not lost
- [ ] Key ordering maintained after insert, delete, split, merge
- [ ] Node occupancy bounds respected (except root)
- [ ] Structural sharing: old NodeIds never modified
- [ ] GC considers all live commit roots
- [ ] Node encode/decode round-trips correctly (hand-written codec)
- [ ] Empty tree / single-entry edge cases handled
