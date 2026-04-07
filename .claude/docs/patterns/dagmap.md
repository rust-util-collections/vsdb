# DagMap Subsystem Review Patterns

## Files
- `strata/src/dagmap/mod.rs` — DagMap types
- `strata/src/dagmap/raw/` — DagMapRaw implementation
- `strata/src/dagmap/rawkey/` — DagMapRawKey<V> typed wrapper

## Architecture
- DAG-based collection: entries can have multiple parents
- Backed by MapxOrd for storage
- Unique ID allocation via `parking_lot::Mutex<Orphan<u128>>`
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

## Common Bug Patterns

### ID Counter Rollback on Crash
Counter stored in Orphan<u128>. If crash happens after ID assignment but before the entry is persisted, the next restart may reuse the ID.
**Check**: Verify counter persistence is atomic with entry creation.

### Mutex Contention on ID Allocation
`parking_lot::Mutex` on the counter becomes a bottleneck under concurrent writes.
**Check**: This is acceptable if DagMap writes are inherently serialized (SWMR). Flag only if the design assumes concurrent writers.

## Review Checklist
- [ ] ID allocation is atomic (mutex held for full read-increment-write)
- [ ] IDs are never reused (monotonic, crash-safe)
- [ ] Parent references are valid (exist and precede child)
- [ ] Delete handles child references (no silent dangling)
- [ ] Encode/decode round-trip for DAG node structure
