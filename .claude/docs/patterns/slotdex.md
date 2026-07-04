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
Pagination is **offset-based** by design (`page_size * page_index`, like SQL `LIMIT`/`OFFSET`), not cursor-based — recomputed fresh via `locate_page_start`/`locate_page_rstart` on every call. Within a single call, a page must be internally consistent (no off-by-one, no skip/duplicate). Across separate calls, pages are NOT guaranteed stable if entries are inserted/removed between requests — this is the documented, accepted contract (see the doc comments on `get_entries_by_page`/`get_entries_by_page_slot`), not a bug.
**Check**: Verify a single page request returns the correct, contiguous slice of the *current* data (no internal off-by-one). Do not flag the offset-based (vs. cursor-based) design itself as a violation.

### INV-SD5: swap_order Transparency
`swap_order` is a pure storage-layout optimization (internal slot complement + result reversal). Logical query results MUST be identical for `swap_order=true` vs `false` on the same data.
**Check**: Any change to query/insert paths must preserve identical logical output under both modes. Verify tests compare both modes against the same reference (see `test.rs` reference-model tests).

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
- [ ] Pagination is internally consistent per call (offset-based by design; not required to be stable across concurrent mutation)
- [ ] Empty tier handled gracefully
- [ ] Insert and query use identical tier computation
- [ ] swap_order=true and =false produce identical logical results
