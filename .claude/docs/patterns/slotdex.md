# SlotDex Review Patterns

**Files:** `slotdex/{mod,slot_type,test}.rs`.

**Arch:** Tier index for slot/time queries. One MapxRaw: entries `0x00|slot|key`,
counts `0x01|level|floor`, total `0x02`. One staged atomic batch per mutation
(no dirty flag). Tiers ≥1 memory-cached; L0 on disk. `slot_rows` O(1) mirror when
tier-less; bulk uses `pending_slot_rows`.

## Invariants

**SD1 Tier map pure** — `SlotType::tier` deterministic, no hidden state.
**SD2 Cross-tier query** — spans all intersected tiers.
**SD3 Boundaries** — each key one tier; consistent half-open.
**SD4 Pagination** — offset-based by design (`page*size`); internal consistency per call; cross-call stability under concurrent mut is **not** required (documented).
**SD5 swap_order** — layout only; logical results byte-equal true↔false.
**SD6 Tier-less growth** — empty `levels` ⇒ `slot_rows` exact L0 count; may be stale once tiers exist (unused). Bulk adds pending; promote merges committed+staged. Hydrate/trunc re-seed mirror only re-entering tier-less. Serial/bulk/reopen same growth cadence.

## Bugs

**Boundary off-by-one** · empty range / empty index panics · bulk never promotes (gate only committed).

## Checklist

- [ ] Pure deterministic tier()
- [ ] Ranges cover all tiers
- [ ] No gap/overlap at bounds
- [ ] Page internal consistency (offset design OK)
- [ ] Empty handled
- [ ] Insert=query tier formula
- [ ] swap_order parity
- [ ] slot_rows rules + bulk promote cadence
