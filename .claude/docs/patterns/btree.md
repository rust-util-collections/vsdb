# B+ Tree Review Patterns

**Files:** `strata/src/basic/persistent_btree/` (`diff.rs` / `merge.rs`: version diff
and three-way merge, also used by VerMap).

**Arch:** B=16 (max 32 keys); COW new NodeId every mutate; structural sharing;
on MapxRaw. `pending` write buffer: public mutators flush before return;
`bulk_load` and merge replay may flush intermediate at threshold — root never
escapes while referenced nodes buffered. `*_buffered` ops are the non-flushing
crate-internal forms for multi-step operations.

## Invariants

**BT1 COW** — every mutate path new NodeId up to root; no write to old id.
**BT2 Order** — in-node increasing; parent-child `child[i] < key[i] ≤ child[i+1]`.
**BT3 Occupancy** — non-root `B..=2B` keys; internal children = keys+1; root min exempt.
**BT4 Sharing** — other versions keep old nodes intact.
**BT5 Reclaim** — `release_node` ref-0 cascade lazy-deletes; `gc()` = full rebuild; live set = **all** commit roots + branch `dirty_root`s.
**BT6 pending** — empty between ops; every return flushes finals; bulk intermediate flush does not publish unfinal root; `node()` prefers buffer; `discard_node` ref-0 only: buffered → dropped, flushed → lazy-deleted.

**BT7 Diff** — skip a front pair only when both are the **same NodeId** (identical key
set); otherwise expand the taller side (height hints affect speed only); entries compare
key-wise. Cost O(changes × depth × fanout); results equal a full-scan diff.
**BT8 Merge replay** — result = target + ∪ `diff(base_i, source)` source states (the whole
source-wins matrix); each step acquires the new root and `release_buffered`s the previous
**intermediate** (never the target); the final root is `disown_node`d → returned unowned;
dead intermediates drop from `pending` before reaching the engine.
**BT9 Deferred reclaim** — after `defer_reclaim()`, released keys queue instead of
lazy-deleting; the owner registers them only after its WAL sync. Default (standalone
tree) stays immediate. Unregistered keys are swept by the next `rebuild_ref_counts`.

## Bugs

**Split median** — internal sep parent-only; leaf sep stays as right first key. At split `left.max < sep == right.min`, no data key in both leaves (after deletes only `sep ≤ right.min`).
**In-place mutate** — grep direct storage writes on mut paths.
**Underflow chain** — after mass delete; order borrow-left → borrow-right → merge (prefer left).
**Reclaim leak** — replaced nodes never released to ref 0.

## Checklist

- [ ] New NodeIds on all mut paths
- [ ] Split separator rules (internal vs leaf)
- [ ] Order after insert/delete/split/merge
- [ ] Occupancy + root exception
- [ ] No mutate shared nodes
- [ ] GC live set = commit roots + dirty roots
- [ ] Codec round-trip
- [ ] Empty/single-entry edges
- [ ] Buffer flush on every mut return; bulk root unobservable mid-way
- [ ] Diff/merge property tests vs full-scan / decision-matrix references; ref counts == recount after merges
- [ ] Deferred owners never lazy-delete before a sync
