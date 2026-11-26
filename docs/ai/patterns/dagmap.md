# DagMap Review Patterns

[Single-parent overlay maps](../../../strata/src/dagmap)

`#![forbid(unsafe_code)]` on `dagmap::raw`. Each node owns one co-located `MapxRaw`. The root creates a `dag_id` token; children share the root's namespace and shard (`new_colocated`). Row `00` holds `VSDAGN01` `NodeMeta`, `01` indexes local child edges, and `02` prefixes user keys (`[0x01 | val]` live vs `[0x00]` tombstone, so empty `[]`/`()` values round-trip).

## Node ownership and recovery

- `new_in(ns, None)` places a root in `ns`. With a parent, raw and typed
  constructors inherit the parent's namespace and shard in both debug and
  release. One batch publishes the advanced parent edge ID, the edge and
  the child's owner record. Check the old snapshot, then fence, exit and
  reopen and create another child. Its edge ID must advance. A prefix reserved
  before an interrupted registration may remain unused.
  [Namespace fixture](../../../strata/tests/dagmap_namespace_test.rs) · [Graph recovery fixtures](../../../strata/src/dagmap/raw/test.rs)

- **Consumed-node guard:** Every local mutator (`insert`, `remove`, `get_mut`, `update`, and `new(Some(parent))`) must enforce `meta.state.live()`, rejecting `Clearing | Destroying | Destroyed` nodes even when unwiped `[02..03)` rows remain from an interrupted cleanup.
- Check node magic, `dag_id`, shard co-location and the child's recorded
  parent and edge before clearing storage reached through a registry entry.
  A stale index entry may be removed locally. Parentless roots and nodes
  owned by another parent keep their data.
- Authenticate fallback reads and guard parent chains against cycles.
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
