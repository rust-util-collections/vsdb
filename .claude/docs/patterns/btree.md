# B+ Tree Review Patterns

**Files:** `strata/src/basic/persistent_btree/`.

**Arch:** B=16 (max 32 keys); COW new NodeId every mutate; structural sharing;
on MapxRaw. `pending` write buffer: public mutators flush before return;
`bulk_load` may flush intermediate at threshold — root never escapes while
referenced nodes buffered.

## Invariants

**BT1 COW** — every mutate path new NodeId up to root; no write to old id.
**BT2 Order** — in-node increasing; parent-child `child[i] < key[i] ≤ child[i+1]`.
**BT3 Occupancy** — non-root `B..=2B` keys; internal children = keys+1; root min exempt.
**BT4 Sharing** — other versions keep old nodes intact.
**BT5 GC** — collect only unreachable from **all** live commit roots.
**BT6 pending** — empty between ops; every return flushes finals; bulk intermediate flush does not publish unfinal root; `node()` prefers buffer; `discard_node` drops buffered.

## Bugs

**Split median** — internal sep parent-only; leaf sep stays as right first key.
**In-place mutate** — grep direct storage writes on mut paths.
**Underflow chain** — after mass delete.

## Checklist

- [ ] New NodeIds on all mut paths
- [ ] Split separator rules (internal vs leaf)
- [ ] Order after insert/delete/split/merge
- [ ] Occupancy + root exception
- [ ] No mutate shared nodes
- [ ] GC all roots
- [ ] Codec round-trip
- [ ] Empty/single-entry edges
- [ ] Buffer flush on every mut return; bulk root unobservable mid-way
