# VecDex Review Patterns

**Files:** `vecdex/{mod,hnsw,distance,dynamic,test}.rs`.

**Arch:** HNSW multi-layer; one MapxRaw tags `0x00`..`0x05`; staged overlay + one
atomic batch/mutation. Adj key `[TAG][layer u8][node u64 BE]`. LE u64 neighbor
packs. Alg-4 heuristic prune. Generic Scalar/Distance/K. `VecDexDyn` + manual
wire tags for metric.

## Invariants

**VD1 Entry = global max_layer** — insert promotes; remove of entry rescans true max.
**VD2 Bidirectional edges** — L edges mutual; remove both sides.
**VD3 Maps inverse** — key↔node↔info consistent through insert/remove/compact.
**VD4 Dim** — all vectors and queries == `meta.dim`.
**VD5 Filter traversal** — filter gates results only, not expansion.
**VD6 Frozen wire tags** — `WIRE_TAG_*` append-only manual serde; never derive enum order.
New metric = new tag; unknown tag hard fail. `MetricKind` pub Serialize → same discipline.

## Bugs

**Entry downgrade on remove** · wire-tag drift (derive/reorder) · missing prune after bi-edge · compact torn write outside one wiped staged commit.

## Checklist

- [ ] Entry at global max_layer
- [ ] Bidirectional edges + prune after insert
- [ ] Map triple consistency
- [ ] Dim checks
- [ ] Filter doesn’t block traversal
- [ ] Wire tags frozen/append-only + round-trip pins
- [ ] Compact single wiped staged commit
- [ ] Send+Sync; Scalar ops cover metrics
