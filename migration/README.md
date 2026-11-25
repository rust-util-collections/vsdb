# Migrating a v17 database

`vsdb_migrate` is a separate Rust library. Applications running v18 normally depend on `vsdb`. Link `vsdb_migrate` in the offline upgrade program.

## Upgrade order

1. Stop all processes using the v17 universe. Back up the base directory, explicitly placed namespace directories and application-owned handle files together. Explicit namespace paths in a staging copy must point to the staged data. For rollback, restore that complete backup and run the v17 binary on the restored path. Current v18 writers and the converter install a format marker that v17 checks. Earlier v18 roots may lack it until reopened writable.
2. Run `migrate_universe(base)` before configuring VSDB in the upgrade process. It checks shard locks and lifecycle records, binds each engine to its namespace and universe, and preserves the prefix allocator floor. A retry uses the durable upgrade identifier.
3. Call `vsdb_configure(VsdbOptions::new(base))` and convert each saved or embedded collection handle with its original types. Record converted tree roots and DAG child IDs, update application records, and verify they reopen before resuming traffic.

```rust,no_run
use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};
use vsdb_migrate::{migrate_basic, migrate_universe};

fn upgrade(base: &std::path::Path, saved: InstanceId) -> vsdb::Result<()> {
    migrate_universe(base)?;
    vsdb_configure(VsdbOptions::new(base))?;
    let map = migrate_basic::<Mapx<String, u64>>(saved)?;
    assert_eq!(map.instance_id(), saved);
    Ok(())
}
```

## Choose a converter

Use the saved-metadata entry point for an ID produced by `save_meta()`.
For a postcard-serialized handle (including a handle stored only in an
application record), use the byte-slice entry point and store its result.
Composite converters also update the backing collection. The conversions
preserve each collection's primary prefix.

| Data | Saved `__instance_meta__` conversion | Serialized byte-slice handle conversion |
| --- | --- | --- |
| Raw map | `migrate_raw(id)` | `raw_handle(&bytes)` |
| Mapx, MapxOrd, MapxOrdRawKey, Orphan | `migrate_basic::<T>(id)` | `basic_handle::<T>(&bytes)` |
| SlotDex (`u32`, `u64`, `u128` slots) | `slotdex::migrate_v17::<S, K>(id)` | `slotdex::migrate_handle_v17::<S, K>(&bytes)` |
| SlotDex (verified custom slot codec) | `slotdex::migrate_custom_v17::<S, K>(id, cap)` | `slotdex::migrate_custom_handle_v17::<S, K>(&bytes, cap)` |
| VecDex, including runtime metrics | `vecdex::migrate_v17::<K, M, S>(id)` | `vecdex::migrate_handle_v17::<K, M, S>(&bytes)` |
| VerMap | `versioned::migrate_v17::<K, V>(id)` | `versioned::migrate_handle_v17::<K, V>(&bytes)` |
| Standalone PersistentBTree | `versioned::migrate_tree_v17(id, &live_roots)` | `versioned::migrate_tree_handle_v17(&bytes, &live_roots)` |
| DagMapRaw root | `dagmap::migrate_v17(id)` | `dagmap::migrate_handle_v17(&bytes)` |
| DagMapRawKey root | `dagmap::migrate_rawkey_v17::<V>(id)` | `dagmap::migrate_rawkey_handle_v17::<V>(&bytes)` |
| Versioned trie checkpoints | Wrap the converted `VerMap` with `VerMapWithProof::from_map`. A root query rebuilds the trie from its rows. | — |

## Application-owned roots, keys and handles

- **Standalone B+ tree.** Supply every live v17 root, repeating a root when several owners hold it. The pool has no persisted root table. Replace each application's root ID with the corresponding returned ID. Keep the original list for retries.
- **DAG.** Convert the root first. Its prefix stays the same. Apply the returned old-to-new map to external descendant references. If a migrated typed child used a different value type containing a renamed library type, call `dagmap::migrate_child_v17::<V>(child_id)` with the relocated ID (or the original ID when absent from the mapping).
- **SlotDex.** For the custom converter, supply the original capacity and verify that the old codec always used the current fixed width and bucket alignment. The converter reconciles entries, counts and tier floors. Rebuild an ambiguous variable-width dataset from authoritative application records.
- **VecDex.** The converter preserves graph edges and node IDs. Use `MetricKind` as the metric type for a v17 runtime-selected metric.
- **Keys.** Ordered `usize` and `isize` keys require an explicit integer width chosen for the source architecture. Float and unordered hash-collection keys need an application-defined canonical replacement.
- **Nested handles.** Decode the surrounding application schema, convert the serialized handle bytes, then re-encode the value or sidecar. For ordinary raw-map values, `rewrite_values` applies a callback in bounded batches. Return `None` for values already converted so a retry can rescan the prefix. Use a schema-aware conversion for versioned values as well. Type tags include the fully qualified Rust type name. Preserve the original application names at conversion time or explicitly translate renamed types.

## Interrupted initialization and allocator recovery

The universe upgrade binds interrupted engine initializers to their original
namespace so v18 can complete shard creation. An orphan established record
or populated namespace directory requires recovery of the missing registration.
When the allocator file is missing, reconstruction includes the default shard's
historical ceiling, namespace data, instance metadata filenames and embedded
handle bytes.
