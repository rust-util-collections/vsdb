# SlotDex Review Patterns

**Files:** `slotdex/{mod,slot_type,test}.rs`.

**Arch:** Tier index for slot/time queries. One MapxRaw: entries `0x00|slot|key`,
counts `0x01|level|floor`, total `0x02`. One staged atomic batch per mutation
(no dirty flag). Tiers ≥1 memory-cached; L0 on disk. `slot_rows` O(1) mirror when
tier-less; bulk uses `pending_slot_rows`.

## Invariants

**SD1 Tier map pure** — bucket = `slot.floor_align(floor_base)`, `floor_base = cap^level` via saturating `floor_base_of`; no hidden state.
**SD2 Cross-tier query** — spans all intersected tiers.
**SD3 Boundaries** — each slot in exactly one bucket per level (insert bumps every level); consistent half-open.
**SD4 Pagination** — offset-based by design (`page*size`); internal consistency per call; cross-call stability under concurrent mut is **not** required (documented). Reverse paging reverses slot groups only, preserving ascending keys within each slot. Consume planned quotas without scanning unused boundary tails; use one entry range, or two when a partial lowest boundary precedes other contributing slots.
**SD5 swap_order** — layout only; logical results byte-equal true↔false.
**SD6 Tier-less growth** — empty `levels` ⇒ `slot_rows` exact L0 count; may be stale once tiers exist (unused). Bulk adds pending; promote merges committed+staged. Hydrate/trunc re-seed mirror only re-entering tier-less. Serial/bulk/reopen same growth cadence.

**SD7 Range API** — `page(slots, size, idx, Order)` / `count(slots)` map `RangeBounds` to
inclusive `Option` bounds: `Excluded` via `checked_succ` / `checked_pred` (overflow ⇒
empty), `Unbounded` ⇒ `None` (never `MIN`/`MAX`: `swap_order` transforms bounds),
inverted ⇒ empty. Results must equal the inclusive internal API.

## Bugs

**Boundary off-by-one** · empty range / empty index panics · bulk never promotes (gate only committed).

## Checklist

- [ ] Pure `floor_align` / `floor_base_of`
- [ ] Ranges cover all tiers
- [ ] No gap/overlap at bounds
- [ ] Page internal consistency (offset design OK); bounded boundary scans and ascending within-slot keys
- [ ] Empty handled
- [ ] Insert = query floor formula
- [ ] swap_order parity
- [ ] slot_rows rules + bulk promote cadence
