# DagMap Review Patterns

Each node owns one raw map. The root creates a graph token. Children share
the root's namespace and shard. The row at `00` holds node metadata, `01`
indexes local child edges, and `02` prefixes user keys. Values carry a live
or tombstone tag, so zero-length application encodings are valid.

- Check node magic, graph identity, shard placement and the child's recorded
  parent and edge before clearing storage reached through a registry entry.
  A stale index entry may be removed locally. Parentless roots and nodes
  owned by another parent keep their data.
- Allocate child edges monotonically within each parent. Publish the child's
  initial metadata before its index entry. Authenticate fallback reads and
  guard parent chains against cycles.
- Prune destroys side branches, folds the mainline atomically into the root
  and reparents survivors before writing any clearing marker. The marker
  retains the original owner and root identities. Retrying on the head
  completes marker publication before wiping local data. Keep discovery
  edges until the data pass completes. Fence the single shard before return.
- Destroy marks and wipes owned descendants with discovery edges intact.
  Finish descendant registries before unlinking the target. A retried
  destroy can use the owner saved in its destroying state.
- Use iterative traversal on deep graphs. A clone remaps every copied parent,
  child and recovery link into the copied graph and retains local edge tokens.
- The offline migration crate owns old tuple decoders, the durable conversion
  journal and the relocation map for external child IDs.
