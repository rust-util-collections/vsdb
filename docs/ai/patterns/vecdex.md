# VecDex Review Patterns

**Files:** `vecdex/{mod,hnsw,distance,dynamic,test}.rs`.

**Arch:** HNSW multi-layer; one MapxRaw tags `0x00`..`0x05`; staged overlay + one
atomic batch per mutation/chunk. Adj key `[TAG][layer u8][node u64 BE]`. LE u64 neighbor
packs. Alg-4 heuristic prune. Generic Scalar/Distance/K. `VecDexDyn` + manual
wire tags for metric.

## Invariants

**VD1 Entry = global max_layer** — insert promotes; remove of entry rescans true max.
**VD2 Bidirectional edges** — L edges mutual; remove both sides. Do not
  take reciprocal pruning or a base-layer connected graph for granted without
  measuring old-node eviction as well as insertion; keep recall distinct from
  reachability. See the current Open entry for its concrete disconnected
  components reproduction and repair scope.
**VD3 Maps inverse** — key↔node↔info consistent through insert/remove/compact.
**VD4 Input** — all vectors and queries == `meta.dim` and finite; mutations
reject nonfinite inputs before committing. `compact()` rejects legacy nonfinite
rows as `Corrupt` without rebuilding. Finite inputs can still overflow a
metric's arithmetic; the input check cannot guarantee finite distances.
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
- [ ] Dimension checks and finite inputs: reject `NaN`/infinities for vectors and queries, including dynamic-metric dispatch; preserve prior rows on rejection
- [ ] Legacy nonfinite stored row fails cold `compact()` as `Corrupt` without rewrite
- [ ] Wire tags frozen/append-only + round-trip pins
- [ ] Compact single wiped staged commit
- [ ] Send+Sync; Scalar ops cover metrics

## Restored-handle ownership

Serde / `from_meta` rebuild independent runtime caches over the same rows.
Retire the prior active handle before mutation and route all reads/writes
through one shared instance while mutable. Alternating writes through separate
restored handles can corrupt state even when calls are sequential. Independent
read handles are supported only while the underlying index stays immutable.
