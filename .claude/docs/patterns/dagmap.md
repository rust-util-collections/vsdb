# DagMap Subsystem Review Patterns

## Files
- `strata/src/dagmap/mod.rs` — DagMap types
- `strata/src/dagmap/raw/` — DagMapRaw implementation
- `strata/src/dagmap/rawkey/` — DagMapRawKey<V> typed wrapper

## Architecture
- DAG-based collection: entries can have multiple parents
- Backed by MapxOrd for storage
- Each node **owns** its parent slot (`Orphan<Option<DagMapRaw>>`) holding an
  aliasing shadow of the parent node; `destroy()` nulls that slot
  persistently, so every handle (clones, shadows, `from_meta` restores)
  observes the destroyed state
- Unique ID allocation via `parking_lot::Mutex<DagIdAllocator>` with crash-safe ceiling file
- Used for graph-like data structures with persistent storage

## Critical Invariants

### INV-DG1: Unique ID Monotonicity
IDs must be strictly monotonically increasing and never reused.
**Check**: Verify ID allocation holds mutex for the entire read-increment-write cycle. Verify no path resets the counter.

### INV-DG2: DAG Acyclicity
Parent references must not create cycles. An entry's parents must have been created before it.
**Check**: Verify insert validates that all parent IDs exist and are less than the new ID (or uses timestamps for ordering).

### INV-DG3: Orphan Prevention
Deleting a node that is referenced as a parent by other nodes would create dangling references.
**Check**: Verify delete either: (1) refuses if node has children, or (2) cascades to children, or (3) explicitly documents dangling is acceptable.

### INV-DG4: Per-Node Parent Slot Ownership
A node's parent slot must be owned exclusively by that node (allocated in
`new()`), never shared with siblings. `destroy()` relies on this to unlink
persistently without orphaning siblings.
**Check**: Verify `new()` allocates a fresh `Orphan` per node and stores a
shadow of the parent handle (NOT a `Clone` — `Clone` deep-copies storage).
Verify `destroy()` nulls only its own slot.

### INV-DG5: Registry Is Index, Parent Slot Is Ownership Truth
A `children` registry entry is only an index; the child's own parent slot
decides ownership. Interrupted multi-step operations (prune) can leave stale
double-registrations, so every registry-driven destruction walk must verify
`child.parent` points back at the walking node (or is `None` — reclaimable
residue) before destroying.
**Check**: Verify `destroy()`, `prune_children`, and prune's side-branch
destruction all gate on the ownership test (`owned_or_residue`); a foreign
entry must only be dropped from the registry, never destroyed through it.

### INV-DG6: Prune Phase Ordering (Crash Safety)
`prune_mainline` must be ordered **destroy branches → merge → flush →
re-parent → flush → clear**, with nothing cleared before the genesis holds
the complete merged state and all surviving children are re-pointed at it.
The genesis is enriched **in place** (keeps its instance ID), which is
invisible through the head because overlay reads resolve top-down.
The head's per-node clear order is **parent → children → data**: once
clearing starts, the head is parentless, so re-running prune is refused
(early return) instead of re-folding against a half-cleared head.
**Check**: Verify no `clear()`/parent-null precedes the merge+re-parent
completion; verify the two `self.namespace().flush()` barriers (scoped to the
DAG's own engine — a composite never spans namespaces — with cross-shard WALs
recovering independently); verify a crash at any phase boundary leaves genesis +
surviving children value-exact (see the `prune_crash_*` tests).

## Common Bug Patterns

### ID Counter Rollback on Crash
Counter backed by a crash-safe ceiling file (batch-allocated, fsync'd via tmp→rename). On crash, the counter resumes from the persisted ceiling — IDs between the last returned value and the ceiling are skipped (safe gap of at most `DAG_ID_BATCH`). No ID is ever reused.
**Check**: Verify ceiling is persisted before any ID in the new batch is returned. Verify atomic write (tmp → fsync → rename).

### Mutex Contention on ID Allocation
`parking_lot::Mutex` on the counter becomes a bottleneck under concurrent writes.
**Check**: This is acceptable if DagMap writes are inherently serialized (SWMR). Flag only if the design assumes concurrent writers.

## Review Checklist
- [ ] ID allocation is atomic (mutex held for full read-increment-write)
- [ ] IDs are never reused (monotonic, crash-safe)
- [ ] Parent references are valid (exist and precede child)
- [ ] Delete handles child references (no silent dangling)
- [ ] Encode/decode round-trip for DAG node structure
