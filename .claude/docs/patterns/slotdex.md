# SlotDex Subsystem Review Patterns

## Files
- `strata/src/slotdex/mod.rs` — SlotDex<S, K>, storage layout, tier cache
- `strata/src/slotdex/slot_type.rs` — SlotType trait (u32/u64/u128 impls)
- `strata/src/slotdex/test.rs` — tests

## Architecture
- Tier-based indexing for timestamp/slot-based queries
- Single-handle storage: entry rows (`0x00|slot|key`), level count rows (`0x01|level|floor`; level 0 = per-slot counts), and the grand total (`0x02`) all live in one `MapxRaw`
- Every mutation is staged and committed in one atomic engine write batch — no dirty flag, no rebuild-on-recovery; the serialized handle metadata is create-time constant
- SlotType trait: maps application slots to tiers
- Tier levels >= 1 are cached in memory (hydrated on open); level 0 is walked on disk
- `slot_rows` is a derived O(1) mirror used only while no tier exists; bulk
  staging adds `pending_slot_rows` so one large batch promotes at serial cadence
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

### INV-SD6: Tier-Less Growth Accounting
While `levels` is empty, `slot_rows` exactly mirrors committed level-0 row
count. It may be stale while tiers exist because no reader uses it then.
`insert_batch` adds not-yet-committed `pending_slot_rows`, and promotion builds
from the merged committed + staged level-0 stream.
**Check**: Hydration and tier-truncating removal re-seed the mirror only on
re-entry to the tier-less state (both scans bounded by `tier_capacity + 1`).
Serial insert, one-shot bulk insert, chunked bulk insert, and reopen must produce
equivalent tier growth.

## Common Bug Patterns

### Tier Boundary Off-By-One
Key at exact tier boundary is missed by both the lower and upper tier query.
**Trigger**: Query tier T1 with `key < boundary` and tier T2 with `key >= boundary`, but the boundary key itself falls through.

### Empty Range Handling
Queries over a slot range with no entries (or an entirely empty index) must
return an empty result, not panic — check the count-row walk and the entry
scan against absent rows.
**Check**: Verify empty index / empty range returns empty results on every
query path (forward, reverse, per-slot).

### Bulk Load Never Promotes
A tier-less one-shot `insert_batch` gates only on committed rows, so pending
slots never cross capacity and the index remains a permanent O(N) level-0 walk.
**Check**: Growth uses `slot_rows + pending_slot_rows` and builds the new level
through `StagedRows::scan_prefix`.

## Review Checklist
- [ ] SlotType::tier() is deterministic and pure
- [ ] Range queries span all intersected tiers
- [ ] Tier boundaries: no gaps, no overlaps
- [ ] Pagination is internally consistent per call (offset-based by design; not required to be stable across concurrent mutation)
- [ ] Empty tier handled gracefully
- [ ] Insert and query use identical tier computation
- [ ] swap_order=true and =false produce identical logical results
- [ ] `slot_rows` is exact whenever tier-less and never read while tiers exist
- [ ] one-shot bulk load promotes tiers with the same cadence as serial inserts
