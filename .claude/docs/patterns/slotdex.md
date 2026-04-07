# SlotDex Subsystem Review Patterns

## Files
- `strata/src/slotdex/mod.rs` — SlotDex<S, K>, SlotType trait
- `strata/src/slotdex/test.rs` — tests

## Architecture
- Tier-based indexing for timestamp/slot-based queries
- SlotType trait: maps application slots to tiers
- Each tier is a MapxOrd range, enabling efficient paged queries
- Used for time-series and DEX order-book patterns

## Critical Invariants

### INV-SD1: Tier Assignment Determinism
The same slot value must always map to the same tier. Non-deterministic mapping breaks range queries.
**Check**: Verify `SlotType::tier()` is a pure function with no hidden state.

### INV-SD2: Cross-Tier Query Completeness
A range query spanning multiple tiers must return results from ALL intersected tiers, not just the first or last.
**Check**: Verify range iteration crosses tier boundaries correctly.

### INV-SD3: Tier Boundary Correctness
Keys at exact tier boundaries must be assigned to exactly one tier (no duplication, no gap).
**Check**: Verify boundary is inclusive on one side and exclusive on the other, consistently.

### INV-SD4: Pagination Consistency
Paginated queries must not skip or duplicate entries across pages, even if new entries are inserted between page requests.
**Check**: Verify cursor-based pagination uses a stable key, not an offset index.

## Common Bug Patterns

### Tier Boundary Off-By-One
Key at exact tier boundary is missed by both the lower and upper tier query.
**Trigger**: Query tier T1 with `key < boundary` and tier T2 with `key >= boundary`, but the boundary key itself falls through.

### Empty Tier Panic
Querying a tier that has no entries causes an unwrap on None from MapxOrd::iter().
**Check**: Verify empty tier returns empty iterator, not error.

## Review Checklist
- [ ] SlotType::tier() is deterministic and pure
- [ ] Range queries span all intersected tiers
- [ ] Tier boundaries: no gaps, no overlaps
- [ ] Pagination uses stable cursor, not offset
- [ ] Empty tier handled gracefully
- [ ] Insert and query use identical tier computation
