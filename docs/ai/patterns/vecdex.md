# VecDex Review Patterns

[HNSW index and reader](../../../strata/src/vecdex)

**Arch:** HNSW multi-layer; one MapxRaw tags `0x00`..`0x06`; staged overlay + one
atomic batch per mutation/chunk. Adj key `[TAG][layer u8][node u64 BE]`. LE u64 neighbor
packs. Alg-4 heuristic prune. Sealed f32/f64 scalar encodings. `VecDexDyn` is a type alias with the metric stored in the schema row.

## Invariants

**VD1 Entry = global max_layer** — insert promotes; remove of entry rescans true max.

**VD2 Bidirectional edges** — L edges mutual; remove both sides. Do not
  assume pruning preserves base-layer connectivity: old-node evictions can split
  components. Public base searches lazily restart from unvisited live node-info
  rows when a component is exhausted before filling `ef`. An unfiltered beam
  at least as large as the index reaches all live vectors, including old graphs;
  smaller beams remain approximate. Mutation searches keep graph-only linking.

**VD3 Maps inverse** — key↔node↔info consistent through insert/remove/compact.

**VD4 Input** — all vectors and queries == `meta.dim` and finite; mutations
reject nonfinite inputs before committing. `compact()` rejects legacy nonfinite
rows as `Corrupt` without rebuilding. Finite inputs can still overflow a
metric's arithmetic; the input check cannot guarantee finite distances.

**VD5 Filter traversal** — filter gates results only, not expansion; termination and
pruning compare against the **passing** results (`ef = max(ef, k)`, no inflation);
`filter_visit_cap = max(64·ef, 4096)` evaluated nodes bounds near-empty predicates.
Restart candidates share that cap and visited set. Recall at low selectivity is
tested. Raw predicates receive the bytes produced by `K::encode_key`.
`search_with_raw_filter` decodes only final hits. Check typed and raw filters
on the same captured view, including after the writer changes the live index.

**VD6 Schema** — persisted configuration (`[0x06]` `META_KEY`, bound into `VSTYPE04` via 8-byte Keccak256) includes dimension, scalar width, nominal `type_tag`, `HnswConfig`, and metric parameters. `VecDexDyn` selects runtime metrics via `VecDexDyn::new_with_metric`; keep `MetricKind` discriminants (`0 = L2, 1 = Cosine, 2 = InnerProduct`, pinned in `dyn_wire_tags_are_frozen`) frozen/append-only and reject unknown tags or trailing schema bytes.

## Bugs

**Entry downgrade on remove** · wire-tag drift (derive/reorder) · missing prune after bi-edge · compact torn write outside one wiped staged commit.

## MMDB v5 query boundary

Resolve each query against one read view. Validate vector dimensions, reverse
key mappings, adjacency byte lengths and fallback-node key widths on the
fallible path. Reaching the visit cap on the last admitted neighbor or on a
component restart must stop further adjacency access. For a candidate read
optimization compare complete ordered result keys, distance bits, callbacks
and visit caps on exactly the same graph. Separate logical block acquisitions,
distinct retained data allocations, process RSS and verified storage requests.
[Query protocol and evidence](../../evaluations/v18-vecdex-v5/README.md)

During mutation, check every ID in a touched committed adjacency against the
exclusive `next_node_id` at chunk entry. An overlay adjacency may reference
IDs already allocated inside the chunk. Stale references to previously issued
IDs can have missing vector rows. Check both traversal and unpruned
reciprocal-edge append paths. An invalid ID or partial ID encoding must
return a typed error before the chunk publishes any row.

## Checklist

- [ ] Entry at global max_layer
- [ ] Bidirectional edges + prune after insert
- [ ] Exhaustive search reaches all components after insert, delete, replacement,
      batch, restore and compact; filtered restarts respect the visit cap
- [ ] Map triple consistency
- [ ] Dim checks
- [ ] Filter doesn’t block traversal; low-selectivity recall test still passes
- [ ] Dimension checks and finite inputs: reject `NaN`/infinities for vectors and queries, including dynamic-metric dispatch; preserve prior rows on rejection
- [ ] Legacy nonfinite stored row fails cold `compact()` as `Corrupt` without rewrite
- [ ] Wire tags frozen/append-only + round-trip pins
- [ ] Compact single wiped staged commit
- [ ] Send+Sync; Scalar ops cover metrics

## Restored-handle ownership

Serialize writers across aliases. Each mutation chunk reloads the committed graph state. `insert_batch` carries
its bounded vector cache across successful chunks within the call. Removal
must evict a staged vector from that cache.
Use one snapshot for multi-row queries and compaction. Preserve the allocation
ceiling across clear and compaction so ordinary point reads cannot observe a
different key's vector. Deep-cloning a mutable handle and cloning an immutable
reader have distinct storage identities.
