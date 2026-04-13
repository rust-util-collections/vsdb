# VecDex Subsystem Review Patterns

## Files
- `strata/src/vecdex/mod.rs` — VecDex<K, D, S> struct, public API, search_internal
- `strata/src/vecdex/hnsw.rs` — HNSW core: search_layer, neighbor selection, adjacency encoding
- `strata/src/vecdex/distance.rs` — Scalar trait, DistanceMetric trait, L2/Cosine/InnerProduct
- `strata/src/vecdex/test.rs` — unit tests

## Architecture
- HNSW (Hierarchical Navigable Small World) graph with multi-layer skip-list structure
- Persistent storage via VSDB primitives: MapxOrd (vectors, node_to_key), Mapx (key_to_node), MapxRaw (adjacency), Orphan (metadata)
- Adjacency compound key: `[layer: u8][node_id: u64 BE]` = 9 bytes
- Neighbor lists: packed little-endian u64 arrays
- Algorithm 4 connectivity-aware neighbor selection (heuristic)
- Generic over Scalar (f32/f64), DistanceMetric, and key type K

## Critical Invariants

### INV-VD1: Entry Point at Global Max Layer
The entry point node must be at the highest layer present in the graph. `meta.max_layer` must equal the maximum `node_info.max_layer` across all nodes.
**Check**: Verify insert promotes entry point when new node is at higher layer. Verify remove scans for the true global max_layer when the entry point is deleted.

### INV-VD2: Bidirectional Edges
For every edge (A, B) at layer L, both A's and B's neighbor lists at layer L must contain the other.
**Check**: Verify insert creates edges in both directions. Verify remove cleans up edges from both sides.

### INV-VD3: Key-Node Mapping Consistency
`key_to_node` and `node_to_key` must be exact inverses. `node_info` must have an entry for every live node.
**Check**: Verify insert populates all three maps. Verify remove clears all three maps. Verify compact preserves the invariant.

### INV-VD4: Dimension Consistency
All vectors in the index must have dimension == `meta.dim`. Query vectors must also match.
**Check**: Verify insert and search validate dimension before operating.

### INV-VD5: Filter Traversal Independence
Filtered search must allow non-matching nodes to participate in graph traversal (as candidates) while excluding them from the result set.
**Check**: Verify search_layer's filter only gates result insertion, not candidate expansion.

## Common Bug Patterns

### Entry Point Layer Downgrade (fixed in v13.2)
On remove of entry point, picking a replacement node without scanning for the global max_layer. Causes higher layers to become unreachable.
**Trigger**: Remove the entry point when other nodes exist at higher layers.

### Neighbor Count Overflow
After bidirectional edge insertion, a node may exceed m_max neighbors. prune_neighbors must be called.
**Check**: Verify prune is called after every bidirectional edge creation.

### Stale Metadata After Compact
compact() clears and re-inserts all vectors. If any intermediate error occurs, metadata may be inconsistent.
**Check**: Verify compact collects all pairs before clearing. Verify error propagation.

## Review Checklist
- [ ] Entry point always at global max_layer (INV-VD1)
- [ ] Bidirectional edges maintained (INV-VD2)
- [ ] key_to_node / node_to_key / node_info consistent (INV-VD3)
- [ ] Dimension validated on insert and search (INV-VD4)
- [ ] Filter does not block graph traversal (INV-VD5)
- [ ] prune_neighbors called after edge insertion
- [ ] compact preserves all data
- [ ] Send + Sync assertion present
- [ ] Scalar trait covers all arithmetic ops needed by distance metrics
