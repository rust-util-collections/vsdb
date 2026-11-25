# DagMap Review Patterns

[Single-parent overlay maps](../../../strata/src/dagmap)

`#![forbid(unsafe_code)]` on `dagmap::raw`. Each node owns one co-located `MapxRaw`. The root creates a `dag_id` token; children share the root's namespace and shard (`new_colocated`). Row `00` holds `VSDAGN01` `NodeMeta`, `01` indexes local child edges, and `02` prefixes user keys (`[0x01 | val]` live vs `[0x00]` tombstone, so empty `[]`/`()` values round-trip).

## Node ownership and recovery

- **Consumed-node guard:** Every local mutator (`insert`, `remove`, `get_mut`, `update`, and `new(Some(parent))`) must enforce `meta.state.live()`, rejecting `Clearing | Destroying | Destroyed` nodes even when unwiped `[02..03)` rows remain from an interrupted cleanup.
- Check node magic, `dag_id`, shard co-location and the child's recorded
  parent and edge before clearing storage reached through a registry entry.
  A stale index entry may be removed locally. Parentless roots and nodes
  owned by another parent keep their data.
- Allocate child edges monotonically within each parent. Publish the child's
  initial metadata before its index entry. Authenticate fallback reads and
  guard parent chains against cycles.
- Prune destroys side branches, folds the mainline atomically into the root
  and reparents live head children — while finishing `destroy_subtree()` on any owned `!state.live()` (`Destroying`) child of the head — before writing any clearing marker or wiping the head's `[01..02)` discovery edges. The marker
  retains the original owner and root identities. Retrying on the head
  completes marker publication before wiping local data. Keep discovery
  edges until the data pass completes. Fence the single shard before return.
- Destroy marks and wipes owned descendants with discovery edges intact.
  Finish descendant registries before unlinking the target. A retried
  destroy can use the owner saved in its destroying state.
- Use iterative traversal on deep graphs. A clone remaps every copied parent,
  child and recovery link into the copied graph and retains local edge tokens.

## Migration

The offline converter decodes the old node tuples and journals conversion. Check external references against the returned child-ID mapping. [DAG converter](../../../migration/src/dagmap.rs) · [Migration checks](migration.md)
