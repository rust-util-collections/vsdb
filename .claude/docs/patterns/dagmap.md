# DagMap Review Patterns

**Files:** `dagmap/mod.rs`, `dagmap/{raw,rawkey}/{mod,test}.rs`.

**Arch:** Single-parent node tree: data `MapxRaw`, children `MapxOrdRawKey<DagMapRaw>`,
parent slot `Orphan<Option<DagMapRaw>>` held via **shadow** (not Clone). IDs:
parking_lot Mutex + crash-safe global `dag_id_ceiling` file (system dir; folds legacy
`id_num`). Children registry is index; parent slot is ownership truth.

## Invariants

**DG1 IDs** — monotonic, never reuse; mutex covers full alloc; ceiling before issue
(tmp→fsync→rename→dir fsync). Gaps ≤ batch OK.
**DG2 Acyclic** — parent exists; no node becomes its own ancestor.
**DG3 Orphans** — delete refuses children / cascades / documents dangling.
**DG4 Parent slot ownership** — fresh Orphan per `new()`; shadow parent. `destroy`: save
parent in `DESTROY_PARENT_KEY` + sync_wal → null own/owned-descendant slots + data → flush →
clear registries → unregister from parent last, so a retry still unlinks.
**DG5 Registry vs ownership** — walks gate on `child.parent` points back (or None residue);
foreign registry entries dropped only, never destroyed via foreign walk.
**DG6 Prune crash order** — destroy branches → atomic whole-mainline merge → flush → re-parent → flush →
mark `PRUNE_CLEARING_KEY` on consumed nodes → flush → clear → flush → unregister. Retry keys
off the marker (finishes the clear, never re-folds genesis) — not off field clear order.
In-place genesis enrich. Flushes scoped to DAG ns. Crash at phase boundary keeps value-exact survivors (`prune_crash_*`).
Merge staging must also preserve children already re-parented by a prior
interrupted prune; never publish intermediate ancestor values during retry.

## Bugs

**Ceiling late** · Mutex as bottleneck only if concurrent writers claimed · prune clear-before-merge · clear before marker is durable.

## Checklist

- [ ] Atomic monotonic ID + durable ceiling
- [ ] Valid parent; no cycle
- [ ] Delete child policy explicit
- [ ] Parent-slot ownership / shadow not Clone
- [ ] Ownership test before destroy-from-registry
- [ ] Prune phase order + marker + flushes; destroy retry unlinks
- [ ] Codec round-trip
