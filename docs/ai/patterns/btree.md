# B+ Tree Review Patterns

[B+ tree implementation](../../../strata/src/basic/persistent_btree)

**Arch:** `#![forbid(unsafe_code)]`; `B = 16` (`MAX_KEYS = 32`); slotted `NodeView` (`0x18` leaf, `0x19` internal); 2-bit `CLASS_MASK` (`0` internal, `LEAF`, `BLOB_LEAF`, `BLOB`). `[0x10 | id_be]` stores slotted pages and `[0x12 | id_be]` stores separated `> 1024 B` blobs; standalone ceiling at `[9]` (verified with one reverse seek for each of the four classes). `pending` write buffer: public mutators flush before return; `bulk_load` and large `VerMap` builds flush intermediate chunks (`[0x06]` `STAGE` in `VerMap`) — root never escapes while referenced nodes are buffered.

## Invariants

**BT1 COW** — every mutate path allocates a new `NodeId` up to root; no write to an existing ID.

**BT2 Order** — in-node strictly increasing; parent-child `child[i] < key[i] ≤ child[i+1]`.

**BT3 Occupancy** — non-root `B..=2B` keys; internal children = `keys + 1`; root min exempt (`len == 0` allowed only on transient pre-`shrink_root` pending nodes, rejected by `verify_pages`).

**BT4 Sharing & `BLOB_LEAF` edges** — versions share unchanged subtrees and `[0x12]` `BLOB` objects (`BLOB_LEAF` outgoing `BLOB` IDs are tracked in `RefState.edges` alongside `Internal` children; avoid re-allocating unchanged sibling blobs on leaf COW or materializing sibling blobs just to read `NodeView::len`).

**BT5 Reclaim & recount** — `release_node` ref-0 cascade lazy-deletes (standalone) or batches removal (`VerMap`); `finish_ref_counts` propagates `Err(VsdbError::Corrupt)` via `?` (never `.expect`) on corrupt pinned/ancillary pages; live set = **all** commit roots + branch `dirty_root`s + runtime snapshot leases (`refs.pins`).

**BT6 pending** — empty between ops; `object()` prefers `pending`; `discard_node` acts on ref-0 only and must drop outgoing pending `[0x12]` `BLOB` children even on cold (`!refs.ready`) restored standalone trees (`pending_bytes` kept exact).

**BT7 Diff** — skip a front pair only when both are the **same NodeId** (identical key
set); otherwise expand the taller side (height hints affect speed only); entries compare
key-wise. Shared NodeIds enable pruning; independently built equal-content
trees may require a full walk. Delta-sized cost depends on structural sharing;
results must equal a full-scan diff.

**BT8 Merge replay** — result = target + ∪ `diff(base_i, source)` source states (the whole
source-wins matrix); each step acquires the new root and `release_buffered`s the previous
**intermediate** (never the target); the final root is `disown_node`d → returned unowned;
dead intermediates drop from `pending` before reaching the engine.

**BT9 Retired roots** — a shared version store writes retirement markers before releasing its prior owners. Last-pin release queues object keys and marker removal for the next batch. Standalone pools retain their lazy-delete behavior.

**BT10 Reader ownership** — capture the published root and acquire its pin under the
shared ref lock; the snapshot and derived iterators share one owned lease. Last drop
cascades through the same deferred queue. Recount holds the lock through pin seeding,
sweep and count replacement; each independent lease adds one root reference. Deep
copies exclude source leases; VerMap then recounts its copied persistent roots.

**BT11 Internal page cache** — `TreeRuntime` shares a byte-bounded, eight-shard clock
cache across handles to one tree. Pending pages take precedence. Cache only
committed internal pages, keyed by their immutable node ID. Check eviction on
reclamation and isolation across trees with overlapping node IDs.

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
- [ ] GC live set = commit roots + dirty roots + live snapshot leases
- [ ] Codec round-trip
- [ ] Empty/single-entry edges
- [ ] Buffer flush on every mut return; bulk root unobservable mid-way
- [ ] Diff/merge property tests vs full-scan / decision-matrix references; ref counts == recount after merges
- [ ] Deferred owners never lazy-delete before a sync
- [ ] Snapshot capture/drop, alias recovery and clone preserve reader ownership
