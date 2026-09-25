# VecDex Review Patterns

**Files:** `vecdex/{mod,hnsw,distance,dynamic,test}.rs`.

**Arch:** HNSW multi-layer; one MapxRaw tags `0x00`..`0x05`; staged overlay + one
atomic batch per mutation/chunk. Adj key `[TAG][layer u8][node u64 BE]`. LE u64 neighbor
packs. Alg-4 heuristic prune. Generic Scalar/Distance/K. `VecDexDyn` + manual
wire tags for metric.

## Invariants

**VD1 Entry = global max_layer** — insert promotes; remove of entry rescans true max.
**VD2 Bidirectional edges** — L edges mutual; remove both sides.
**VD3 Maps inverse** — key↔node↔info consistent through insert/remove/compact.
**VD4 Dim** — all vectors and queries == `meta.dim`.
**VD5 Filter traversal** — filter gates results only, not expansion; termination and
pruning compare against the **passing** results (`ef = max(ef, k)`, no inflation);
`filter_visit_cap = max(64·ef, 4096)` evaluated nodes bounds near-empty predicates.
Recall at low selectivity is tested — a fixed visit budget is the known regression.
**VD6 Frozen wire tags** — `WIRE_TAG_*` append-only manual serde; never derive enum order.
New metric = new tag; unknown tag hard fail. `MetricKind` derives serde (variant index =
public wire for callers, not the meta format) → append only, never reorder.

## Bugs

**Entry downgrade on remove** · wire-tag drift (derive/reorder) · missing prune after bi-edge · compact torn write outside one wiped staged commit.

## Checklist

- [ ] Entry at global max_layer
- [ ] Bidirectional edges + prune after insert
- [ ] Map triple consistency
- [ ] Dim checks
- [ ] Filter doesn’t block traversal; low-selectivity recall test still passes
- [ ] Dimension / config errors are `DimensionMismatch` / `InvalidConfig`, not panics
- [ ] Wire tags frozen/append-only + round-trip pins
- [ ] Compact single wiped staged commit
- [ ] Send+Sync; Scalar ops cover metrics

## Restored-handle ownership

Serde / `from_meta` rebuild independent runtime caches over the same rows.
Retire the prior active handle before mutation and route all reads/writes
through one shared instance while mutable. Alternating writes through separate
restored handles can corrupt state even when calls are sequential. Independent
read handles are supported only while the underlying index stays immutable.
