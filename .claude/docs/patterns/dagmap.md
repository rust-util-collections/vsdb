# DagMap Review Patterns

**Files:** `dagmap/{mod,raw,rawkey}/`.

**Arch:** Multi-parent DAG on MapxOrd. Each node owns parent slot
`Orphan<Option<DagMapRaw>>` via **shadow** (not Clone). `destroy` nulls own slot.
IDs: parking_lot Mutex + crash-safe ceiling batch file. Children registry is index;
parent slot is ownership truth.

## Invariants

**DG1 IDs** — monotonic, never reuse; mutex covers full alloc; ceiling before issue
(tmp→fsync→rename). Gaps ≤ batch OK.
**DG2 Acyclic** — parents exist and precede child (or documented order).
**DG3 Orphans** — delete refuses children / cascades / documents dangling.
**DG4 Parent slot ownership** — fresh Orphan per `new()`; shadow parent; destroy only own slot.
**DG5 Registry vs ownership** — walks gate on `child.parent` points back (or None residue);
foreign registry entries dropped only, never destroyed via foreign walk.
**DG6 Prune crash order** — destroy branches → atomic whole-mainline merge → flush → re-parent → flush →
clear → flush → unregister. Clear order parent→children→data so re-prune refuses half-head.
In-place genesis enrich. Flushes scoped to DAG ns. Crash at phase boundary keeps value-exact survivors (`prune_crash_*`).
Merge staging must also preserve children already re-parented by a prior
interrupted prune; never publish intermediate ancestor values during retry.

## Bugs

**Ceiling late** · Mutex as bottleneck only if concurrent writers claimed · prune clear-before-merge.

## Checklist

- [ ] Atomic monotonic ID + durable ceiling
- [ ] Valid parents
- [ ] Delete child policy explicit
- [ ] Parent-slot ownership / shadow not Clone
- [ ] Ownership test before destroy-from-registry
- [ ] Prune phase order + flushes
- [ ] Codec round-trip
