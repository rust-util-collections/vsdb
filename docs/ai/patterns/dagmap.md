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
  Check the magic and DAG id before decoding the rest of a node. A foreign
  identity, a missing record, a rejected prefix or another shard ends the
  lookup. An incomplete identity or a malformed body belonging to this DAG,
  including trailing bytes, is a read failure.
- Prune destroys side branches and finishes `destroy_subtree()` on any
  owned `!state.live()` (`Destroying`) child of the head. One co-located
  batch then publishes the fold, the reparented live head children and a
  clearing marker on every mainline node, so no folded node stays live.
  The marker retains the original owner and root identities. Retrying on
  the head completes marker publication before wiping local data. Keep
  discovery edges until the data pass completes. Fence the single shard
  before return. A prune entering at a live ancestor resumes an unmarked
  prune left by earlier releases when a live descendant's edge names a
  child of the root. A line that an earlier release marked only in part is
  finished from its deepest marked node and folded again in the same kind
  of batch, keeping edits and children added to its unmarked nodes.
- Destroy marks and wipes owned descendants with discovery edges intact.
  Finish descendant registries before unlinking the target. A retried
  destroy can use the owner saved in its destroying state.
- Use iterative traversal on deep graphs. A clone remaps every copied parent,
  child and recovery link into the copied graph and retains local edge tokens.
