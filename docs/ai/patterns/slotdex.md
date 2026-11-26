# SlotDex Review Patterns

[Slot index and queries](../../../strata/src/slotdex)

Each `SlotType` defines an order-preserving fixed-width byte array, including
custom coordinate types. Tier capacity is a `u32` of at least two. One
`MapxRaw` stores entries (`0x00 | slot | key`), counts (`0x01 | level | floor`)
and a 16-byte total and generation row (`0x02`). Level zero lives on disk.
Higher tiers are cached in memory. `insert_batch` commits once when it adds
entries. `clear` advances the generation even when it empties the index.

## Invariants

**SD1 Tier map pure** — bucket = `slot.floor_align(floor_base)`, `floor_base = cap^level` in unsigned rank space anchored at `MIN`. An overflowing stride means one bucket at `MIN`.

**SD2 Cross-tier query** — spans all intersected tiers.

**SD3 Boundaries** — each slot in exactly one bucket per level (insert bumps every level); consistent half-open.

**SD4 Pagination** — offset-based by design (`page*size`); internal consistency per call; cross-call stability under concurrent mut is **not** required (documented). Reverse paging reverses slot groups only, preserving ascending keys within each slot. A page seeks to its slot and skips the local offset. Forward-chunked descending traversals group at most 256 entries and 16 KiB of raw keys at a time; oversized slots stream individually.

**SD5 swap_order** — layout only; logical results byte-equal true↔false.

**SD6 Tier-less growth** — empty `levels` ⇒ `slot_rows` exact L0 count; may be stale once tiers exist (unused). Batch inserts coalesce level-zero and upper-tier counts by bucket. Flush pending level-zero counts into the overlay before the first tier grows. Apply runtime cache changes only after the storage commit succeeds. Hydrate/trunc re-seed mirror only re-entering tier-less. Serial/bulk/reopen same growth cadence.

**SD7 Range API** — `page(slots, size, idx, Order)` / `count(slots)` map `RangeBounds` to
inclusive `Option` bounds: `Excluded` via `checked_succ` / `checked_pred` (overflow ⇒
empty), `Unbounded` ⇒ `None` (never `MIN`/`MAX`: `swap_order` transforms bounds),
inverted ⇒ empty. Counts and pages must agree with the logical slot and key order.

## Bugs

**Boundary off-by-one** · empty range / empty index panics · bulk never promotes (gate only committed).

## Checklist

- [ ] Pure `SlotType::tier_stride` / `floor_align`, applied through `aligned`
- [ ] Ranges cover all tiers
- [ ] No gap/overlap at bounds
- [ ] Page internal consistency (offset design OK); bounded boundary scans and ascending within-slot keys
- [ ] Empty handled
- [ ] Insert = query floor formula
- [ ] swap_order parity
- [ ] slot_rows rules + bulk promote cadence

## Restored-handle ownership

Serde / `from_meta` rebuild independent runtime caches over the same rows.
Serialize writers across restored handles. Every write verifies the generation in the total row. Stale writers return an error. A stale reader loads fresh tier caches from its own captured read view. Clone copies storage and resets the new generation to zero.
