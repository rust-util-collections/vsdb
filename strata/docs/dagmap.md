# DagMap parent overlays

Each `DagMap<K, V>` node has at most one parent. Reads walk from the node
to its ancestors and return the first local override. Removing a key in a
child masks the inherited value. A parent keeps its own value until a prune
folds changes back into it. `DagMapRawKey<V>` uses byte keys, and `DagMapRaw`
uses bytes for both keys and values. Empty byte strings and unit values
round-trip.

## Override an inherited value

```rust
use vsdb::DagMap;

let mut prices = DagMap::<u32, u64>::new(None);
prices.insert(&1, &10);
let mut sale = DagMap::new(Some(&mut prices));
assert_eq!(sale.get(&1), Some(10));

sale.insert(&1, &8);
assert_eq!(prices.get(&1), Some(10));
let root = sale.prune().unwrap();
assert_eq!(root.get(&1), Some(8));
```

## Pruning and recovery

Serialize structural changes to each connected graph. Each node owns a raw
map on the root's shard. Creating a child publishes its owner record and the
parent's edge and next edge ID in one batch.

`prune()` consumes the selected node (the head) and returns the surviving
root. It removes ancestor side branches, folds the path from root to head,
reparents the head's surviving children and clears consumed nodes. One batch
publishes the fold, the reparented children and the clearing markers.
A successful prune fences the shard's WAL.

Save or serialize the head before pruning if the application needs to retry.
After an interruption, restore that head and call `prune()` again to finish
cleanup. To make an independent copy of the connected graph, use `clone()`,
or use `clone_in(&namespace)` for another namespace. The copy preserves each
parent's child tokens. [Co-located writes](../../docs/storage.md#batch-visibility-and-durability)
